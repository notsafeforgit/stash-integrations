import copy
import json
import sys
import unittest
from unittest.mock import Mock, patch

import requests
from gallery_dl import config, exception, extractor
from gallery_dl.extractor.reddit import RedditAPI

from stash_ingest import metadata_fetch, pagination, reddit_api, request_budget
from stash_ingest.encoding import InvalidData
from stash_ingest.gallery import SourceSession
from stash_ingest.producer import Producer
from stash_ingest.runs import SourceFailure
import test_producer
from test_reddit_search import post

URL = 'https://www.reddit.com/search?q=author%3Aexample&sort=new&t=all'
SEARCH = '/search/.json'
PARAMS = {'q': 'author:example', 'sort': 'new', 't': 'all', 'limit': 5}


def response(data=None, *, status=200, body=None):
    result = requests.Response()
    result.status_code = status
    result._content = (body.encode() if body is not None else json.dumps(data).encode())
    result._content_consumed = True
    result.headers['Content-Type'] = 'text/html' if body is not None else 'application/json'
    return result


def blocked(status=403):
    return response(status=status, body="<div>You've been blocked by network security.</div>")


def listing(ids=('abc123',), after=None):
    return response({'data': {'children': [{'kind': 't3', 'data': post(i)} for i in ids], 'after': after}})


def detail():
    return response([{'data': {'children': [{'kind': 't3', 'data': post()}]}}, {'data': {'children': []}}])


class FallbackTests(unittest.TestCase):
    def setUp(self):
        config.clear()
        self.addCleanup(config.clear)
        list(extractor.extractors())
        self.settings = {'extractor': {'reddit': {'api': 'rest', 'client-id': 'fixture-client',
            'refresh-token': 'fixture-refresh', 'user-agent': 'fixture', 'native-api-fallback': 'oauth'}}}
        config._config.update(copy.deepcopy(self.settings))
        self.requests, self.receipts = [], []
        self.clock = 1000
        clock = patch('stash_ingest.reddit_api.time.monotonic', side_effect=lambda: self.clock)
        clock.start()
        self.addCleanup(clock.stop)

    def target(self, responses, url=URL, *, install=True, native=True):
        target = extractor.find(url)
        target.initialize()
        cache = {}
        def cached(func, *args, **kwargs):
            key = (func.__name__, *args)
            if key not in cache:
                cache[key] = func(*args)
            return cache[key]
        target.cache = cached
        pending = iter(responses)
        def send(method, url, **kwargs):
            self.requests.append((method, url, copy.deepcopy(kwargs)))
            value = next(pending)
            if isinstance(value, Exception):
                raise value
            value.url = url
            return value
        if native:
            producer = Mock(saved_pagination=None)
            producer.request_source.return_value = {'source_scope': 'service:reddit', 'request_uuid': 'permit'}
            producer.report_source.side_effect = lambda permit, value=None: self.receipts.append(request_budget.response_result(permit, value))
            producer.fail_source.side_effect = lambda code, scope: (_ for _ in ()).throw(SourceFailure(code, scope))
            session = SourceSession(producer, Mock(request=send))
            target.request = lambda url, **kwargs: session.request(kwargs.pop('method', 'GET'), url, **kwargs)
        else:
            target.request = lambda url, **kwargs: send(kwargs.pop('method', 'GET'), url, **kwargs)
        target.api = RedditAPI(target)
        if install:
            reddit_api.install_api(target)
        return target

    def test_successful_rest_never_authenticates_or_changes_configuration(self):
        target = self.target([listing()])
        self.assertEqual(target.api._call(SEARCH, dict(PARAMS))['data']['children'][0]['data']['id'], 'abc123')
        self.assertEqual([r[1] for r in self.requests], [reddit_api.REST + SEARCH])
        self.assertIsNone(self.requests[0][2]['headers'])
        self.assertEqual(target.config('api'), 'rest')
        self.assertEqual(target.api.root, reddit_api.REST)

    def test_fallback_is_scoped_and_retries_rest_after_cooldown(self):
        denied = blocked()
        denied.headers['Retry-After'] = '7'
        target = self.target([denied, response({'access_token': 'fixture-token'}), listing(after='t3_abc123'),
                              listing(('def456',)), detail(), response({'data': {'name': 'example'}}), listing(('aaa111',))])
        target.api._call(SEARCH, dict(PARAMS))
        target.api._call(SEARCH, {**PARAMS, 'after': 't3_abc123'})
        target.api.submission('abc123')
        target.api.user_about('example')
        self.clock += reddit_api.RECHECK_SECONDS + 1
        target.api._call(SEARCH, {**PARAMS, 'after': 't3_def456'})
        self.assertEqual([r[1] for r in self.requests], [reddit_api.REST + SEARCH,
            reddit_api.REST + '/api/v1/access_token', 'https://oauth.reddit.com' + SEARCH,
            'https://oauth.reddit.com' + SEARCH, reddit_api.REST + '/comments/abc123/.json',
            reddit_api.REST + '/user/example/about.json', reddit_api.REST + SEARCH])
        self.assertEqual(self.requests[0][2]['params'], self.requests[2][2]['params'])
        self.assertEqual(self.requests[3][2]['params']['after'], 't3_abc123')
        self.assertEqual(self.requests[-1][2]['params']['after'], 't3_def456')
        self.assertIsNone(self.requests[4][2]['headers'])
        self.assertEqual(self.requests[2][2]['headers']['Authorization'], 'Bearer fixture-token')
        self.assertEqual(self.receipts[0], {'request_uuid': 'permit', 'status': 403, 'retry_after_seconds': 7})
        self.assertEqual(len(self.receipts), len(self.requests))
        self.assertFalse(reddit_api.can_fallback(denied, reddit_api.REST + SEARCH))

    def test_failures_that_are_not_transport_blocks_never_switch(self):
        for value, error in [(response({'error': 403}, status=403), exception.AuthorizationError),
                             (response({'error': 404}, status=404), exception.NotFoundError),
                             (response({'error': 500}, status=500), exception.AbortExtraction),
                             (response(body='<html>unknown response</html>'), exception.AbortExtraction),
                             (blocked(429), SourceFailure), (blocked(200), SourceFailure),
                             (requests.exceptions.Timeout(), SourceFailure)]:
            with self.subTest(status=getattr(value, 'status_code', None)):
                self.requests.clear()
                target = self.target([value])
                with self.assertRaises(error):
                    target.api._call(SEARCH, dict(PARAMS))
                self.assertEqual(len(self.requests), 1)

    def test_oauth_rate_limit_is_not_a_reason_to_switch_back_or_complete(self):
        target = self.target([blocked(), response({'access_token': 'fixture-token'}), response({'error': 429}, status=429)])
        with self.assertRaises(SourceFailure) as raised:
            target.api._call(SEARCH, dict(PARAMS))
        self.assertEqual(raised.exception.code, 'rate_limited')
        self.assertEqual(len(self.requests), 3)
        self.assertEqual(self.receipts[-1]['status'], 429)

    def test_failed_oauth_authentication_is_not_an_empty_listing(self):
        target = self.target([blocked(), response({'error': 'invalid_grant'}, status=401)])
        with self.assertRaises(exception.AuthenticationError):
            target.api._call(SEARCH, dict(PARAMS))
        self.assertEqual(len(self.requests), 2)

    def test_fallback_permission_cannot_escape_its_exact_request(self):
        value = blocked()
        value.url = reddit_api.REST + SEARCH
        self.assertFalse(reddit_api.can_fallback(value))
        token = reddit_api._attempt.set(SEARCH)
        try:
            self.assertTrue(reddit_api.can_fallback(value))
            for url in ('https://oauth.reddit.com/search/.json', 'https://foreign.invalid/search/.json',
                        reddit_api.REST + '/user/example/about.json', 'http://www.reddit.com/search/.json'):
                self.assertFalse(reddit_api.can_fallback(value, url))
            value.url = 'https://foreign.invalid/search/.json'
            self.assertFalse(reddit_api.can_fallback(value, reddit_api.REST + SEARCH))
        finally:
            reddit_api._attempt.reset(token)

    def test_a_new_crawl_tries_rest_again(self):
        target = self.target([blocked(), response({'access_token': 'fixture-token'}), listing()])
        target.api._call(SEARCH, dict(PARAMS))
        fresh = self.target([listing()])
        fresh.api._call(SEARCH, dict(PARAMS))
        self.assertEqual(self.requests[-1][1], reddit_api.REST + SEARCH)

    def test_opt_in_and_public_route_validation(self):
        for endpoint in ('/user/example/saved/.json', '/api/morechildren', '/unrecognized/.json'):
            with self.subTest(endpoint=endpoint):
                self.requests.clear()
                target = self.target([blocked()])
                with self.assertRaises(SourceFailure):
                    target.api._call(endpoint, {})
                self.assertEqual(len(self.requests), 1)
        config.set(('extractor', 'reddit'), 'native-api-fallback', 'none')
        target = self.target([], install=False)
        original = target.api._call
        reddit_api.install_api(target)
        self.assertEqual(target.api._call, original)
        config.set(('extractor', 'reddit'), 'native-api-fallback', 'typo')
        with self.assertRaises(InvalidData):
            self.target([])
        config.set(('extractor', 'reddit'), 'native-api-fallback', 'oauth')
        config.set(('extractor', 'reddit'), 'native-search-api', 'browser')
        with self.assertRaises(InvalidData):
            self.target([])

    def test_real_pagination_checkpoints_precede_both_transports_and_do_not_skip_failure(self):
        test_producer.ProducerTests.setUp(self)
        anchor, _ = self.producer.cursor(self.producer.prepare(test_producer.reddit_data('anchor')))
        saved = {'extractor': 'reddit:home-search', 'url': URL, 'cursor': 't3_prior', 'item': anchor}
        self.lease.run['progress'] = {'items_seen': 10, 'files_completed': 10, 'cursor': anchor, 'pagination': saved}
        producer = Producer(self.box, self.lease, self.root, extractor_version='fixture')
        target = self.target([blocked(), response({'access_token': 'fixture-token'}),
                              response({'error': 500}, status=500)], install=False)
        original = target.request
        def request(url, **kwargs):
            self.assertEqual(producer.pagination, saved)
            self.assertEqual(producer.resume_cursor, anchor)
            return original(url, **kwargs)
        target.request = request
        pagination.install(target, producer)
        with self.assertRaises(exception.AbortExtraction):
            list(target.submissions())
        self.assertTrue(producer.pagination_started)
        self.assertEqual(producer.pagination, saved)
        self.assertEqual(self.requests[0][2]['params']['after'], 't3_prior')
        self.assertEqual(self.requests[-1][2]['params']['after'], 't3_prior')
        self.assertEqual(self.lease.checkpoints, [])

    def test_upstream_metadata_and_pagination_continue_across_oauth_pages(self):
        target = self.target([blocked(), response({'access_token': 'fixture-token'}),
                              listing(after='t3_abc123'), listing(('def456',))], install=False)
        producer = Mock(saved_pagination=None)
        pagination.install(target, producer)
        rows = list(target.submissions())
        self.assertEqual([p['id'] for p, _ in rows], ['abc123', 'def456'])
        self.assertEqual([c.args[0]['cursor'] for c in producer.begin_page.call_args_list], ['', 't3_abc123'])
        self.assertEqual(rows[0][0]['title'], post()['title'])
        self.assertEqual(rows[0][0]['selftext'], post()['selftext'])

    def test_metadata_sanitizer_keeps_fallback_and_uses_full_post_objects(self):
        url = 'https://www.reddit.com/comments/abc123/'
        target = self.target([blocked(), response({'access_token': 'fixture-token'}), detail()],
                             url=url, install=False, native=False)
        safe = metadata_fetch.safe_config(self.settings)
        self.assertEqual(safe['extractor']['reddit']['native-api-fallback'], 'oauth')
        result = metadata_fetch.collect(url, self.settings, factory=lambda _: target)
        self.assertNotIn('error', result)
        self.assertEqual([r[1] for r in self.requests], [reddit_api.REST + '/comments/abc123/.json',
                         reddit_api.REST + '/api/v1/access_token', 'https://oauth.reddit.com/comments/abc123/.json'])

    def test_discovery_fallback_preserves_saved_cursor_and_one_page_boundary(self):
        from stash_ingest.discovery_fetch import collect
        url = 'https://www.reddit.com/user/example/submitted/?sort=new'
        page_response = response({'data': {'children': [{'kind': 't3',
            'data': post(author_fullname='t2_fixture')}], 'after': 't3_abc123'}})
        values = [response({'data': {'name': 'example', 'id': 'fixture'}}), blocked(),
                  response({'access_token': 'fixture-token'}), page_response]
        page = collect(url, self.settings, {'after': 't3_prior'}, factory=lambda u: self.target(
            values, url=u, install=False, native=False))
        self.assertNotIn('error', page)
        self.assertFalse(page['complete'])
        self.assertEqual(page['cursor'], {'after': 't3_prior'})
        self.assertEqual(page['next_cursor'], {'after': 't3_abc123'})
        self.assertEqual([r[1] for r in self.requests], [reddit_api.REST + '/user/example/about.json',
            reddit_api.REST + '/user/example/submitted/.json', reddit_api.REST + '/api/v1/access_token',
            'https://oauth.reddit.com/user/example/submitted/.json'])
        self.assertEqual(self.requests[1][2]['params'], self.requests[3][2]['params'])
        self.assertEqual(self.requests[3][2]['params']['after'], 't3_prior')
        self.assertEqual([r['kind'] for r in page['records']], ['post', 'media'])

    def test_isolated_metadata_process_reports_each_attempt_and_keeps_oauth_429(self):
        url = 'https://www.reddit.com/comments/abc123/'
        class Budget:
            def __init__(self): self.requests, self.responses = [], []
            def __call__(self, url): return True
            def request(self, url):
                self.requests.append(url)
                return {'request_uuid': str(__import__('uuid').uuid4()), 'source_scope': 'service:reddit'}
            def report(self, result): self.responses.append(result)
        script = '''
import json, requests
from urllib.parse import urlsplit
from stash_ingest.metadata_fetch import main
def send(adapter, request, **kwargs):
    u = urlsplit(request.url)
    r = requests.Response()
    r.url, r.request, r._content_consumed = request.url, request, True
    r.status_code = 200
    r.headers['Content-Type'] = 'application/json'
    if u.path == '/api/v1/access_token':
        assert request.method == 'POST'
        r._content = b'{"access_token":"fixture-token"}'
    elif u.hostname == 'www.reddit.com':
        assert not request.headers.get('Authorization', '').startswith('Bearer ')
        r.status_code = 403
        r.headers['Content-Type'] = 'text/html'
        r._content = b"You've been blocked by network security."
    else:
        assert u.hostname == 'oauth.reddit.com'
        assert request.headers['Authorization'] == 'Bearer fixture-token'
        r.status_code = STATUS
        r._content = json.dumps(PAYLOAD).encode()
    return r
requests.adapters.HTTPAdapter.send = send
main()
'''
        for status in (200, 429):
            with self.subTest(status=status):
                budget = Budget()
                payload = json.loads(detail().content) if status == 200 else {'error': 429}
                program = script.replace('STATUS', str(status)).replace('PAYLOAD', repr(payload))
                settings = copy.deepcopy(self.settings)
                settings['extractor']['reddit']['sleep-request'] = 0
                result = metadata_fetch._exchange([sys.executable, '-B', '-c', program],
                    json.dumps({'url': url, 'settings': settings, 'resume': None,
                                'source_pacing': True, 'request_budget': True}).encode(),
                    10, reserve_source=budget)
                self.assertEqual([r['status'] for r in budget.responses], [403, 200, status])
                self.assertNotIn('block_reason', budget.responses[0])
                self.assertEqual(len(set(r['request_uuid'] for r in budget.responses)), 3)
                if status == 200:
                    self.assertNotIn('error', result)
                    self.assertTrue(result['records'])
                else:
                    self.assertEqual(result, {'error': 'rate_limited'})


if __name__ == '__main__':
    unittest.main()
