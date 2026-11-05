import copy
import html
import unittest
from unittest.mock import Mock
from urllib.parse import urlencode

from gallery_dl import config, exception, extractor
from gallery_dl.extractor.reddit import RedditAPI
from stash_ingest import pagination, reddit_search
from stash_ingest.encoding import InvalidData
from stash_ingest.gallery import SourceSession
from stash_ingest.producer import Producer
from stash_ingest.runs import SourceFailure
import test_producer


PARAMS = {'q': 'author:example nsfw:yes', 'sort': 'new', 't': 'all', 'raw_json': '1', 'restrict_sr': '1'}
URL = 'https://www.reddit.com/search?' + urlencode(PARAMS)


def page(ids=('abc123',), *, after=None, params=None, path='/search/', empty=False):
    params = PARAMS if params is None else params
    fields = ''.join('<input name="{}" value="{}">'.format(k, html.escape(params[k], quote=True))
                     for k in ('q', 'sort', 't'))
    rows = ''.join('<div class="search-result search-result-link" data-fullname="t3_{}"></div>'.format(i) for i in ids)
    continuation = ''
    if after is not None:
        url = reddit_search.ROOT + path + '?' + urlencode({**params, 'after': after, 'count': len(ids)})
        continuation = '<span class="nextprev"><a rel="nofollow next" href="{}">next</a></span>'.format(html.escape(url, quote=True))
    if empty:
        rows = '<p class="info">there doesn\'t seem to be anything here</p>'
    return ('<html><body class="combined-search-page search-page">' + fields
            + '<div class="listing search-result-listing"><div class="search-result-group">'
            + rows + '<footer>' + continuation + '</footer></div></div></body></html>')


def post(post_id='abc123', **kwargs):
    return {'id': post_id, 'name': 't3_' + post_id, 'created_utc': 1790812800, 'num_comments': 0,
            'title': 'Full original caption', 'selftext': 'Original details',
            'author': 'example', 'subreddit': 'example', 'is_self': False, 'is_video': False,
            'permalink': '/r/example/comments/' + post_id + '/title/',
            'url': 'https://i.redd.it/' + post_id + '.jpg', **kwargs}


def response(body='', *, status=200, data=None):
    return Mock(status_code=status, headers={'Content-Type': 'text/html' if data is None else 'application/json'},
                text=body, content=body.encode(), json=Mock(return_value=data))


def details(value):
    return response(data=[{'data': {'children': [{'kind': 't3', 'data': value}]}}, {'data': {'children': []}}])


class ParseTests(unittest.TestCase):
    def parse(self, body, params=None, path='/search/'):
        return reddit_search.parse_page(body, path, PARAMS if params is None else params)

    def test_order_and_continuation_and_verified_empty(self):
        self.assertEqual(self.parse(page(('def456', 'abc123'), after='t3_abc123')), (['t3_def456', 't3_abc123'], 't3_abc123'))
        self.assertEqual(self.parse(page()), (['t3_abc123'], None))
        self.assertEqual(self.parse(page((), empty=True)), ([], None))

    def test_unknown_empty_block_truncated_and_duplicate_results_do_not_complete(self):
        for body in (page((), empty=False), '<html><body>blocked</body></html>', page().replace('</body>', ''),
                     page(('abc123', 'abc123')), page(('bad!',)), page().replace('author:example', 'author:other'),
                     page().replace('name="sort" value="new"', 'name="sort" value="top"'),
                     page().replace('name="t" value="all"', 'name="t" value="year"'),
                     page().replace('search-result-listing', 'changed-layout'),
                     page().replace('<footer>', '').replace('</footer>', ''),
                     page(after='t3_abc123').replace('nofollow next', ''),
                     page() + '<body class="search-page"></body>', ' ' * (reddit_search.MAX_HTML + 1)):
            with self.subTest(body=body[:60]), self.assertRaises(InvalidData):
                self.parse(body)

    def test_continuation_cannot_change_scope_or_loop(self):
        body = page(after='t3_abc123')
        for changed in (body.replace('old.reddit.com', 'foreign.invalid'), body.replace('https:', 'http:'),
                        body.replace('old.reddit.com', 'user@old.reddit.com'),
                        body.replace('/search/?', '/r/other/search/?'),
                        body.replace('author%3Aexample', 'author%3Aother'),
                        body.replace('sort=new', 'sort=top'), body.replace('t=all', 't=year'),
                        body.replace('after=t3_abc123', 'after=t3_other'),
                        body.replace('after=t3_abc123', 'after=t3_abc123&amp;after=t3_abc123'),
                        body.replace('count=1', 'count=1&amp;restrict_sr=1')):
            with self.subTest(body=changed[-300:]), self.assertRaises(InvalidData):
                self.parse(changed)
        with self.assertRaises(InvalidData):
            self.parse(body, {**PARAMS, 'after': 't3_abc123'})

    def test_subreddit_scope_and_page_size_are_checked(self):
        params = {**PARAMS, 'restrict_sr': '1'}
        self.assertEqual(self.parse(page(params=params, path='/r/example/search/', after='t3_abc123'),
                                    params, '/r/example/search/')[1], 't3_abc123')
        with self.assertRaises(InvalidData):
            self.parse(page(tuple(str(i) for i in range(101))))


class AdapterTests(unittest.TestCase):
    def setUp(self):
        config.clear()
        self.addCleanup(config.clear)
        list(extractor.extractors())
        config.set(('extractor', 'reddit'), 'api', 'rest')
        config.set(('extractor', 'reddit'), 'native-search-api', 'browser')

    def target(self, url=URL, *, install=True):
        target = extractor.find(url)
        target.initialize()
        target.api = RedditAPI(target)
        if install:
            reddit_search.install(target)
        return target

    def test_lazy_full_metadata_and_real_upstream_filters_across_pages(self):
        config.set(('extractor', 'reddit'), 'id-max', 'abc123')
        target = self.target()
        value = post(media_metadata={'image': {'m': 'image/jpeg'}}, gallery_data={'items': [{'media_id': 'image'}]})
        target.request = Mock(side_effect=[response(page(('abc123', 'def456'), after='t3_def456')), details(value),
                                           details(post('def456')), response(page(('aaa111',))), details(post('aaa111'))])
        iterator = iter(target.submissions())
        self.assertEqual(next(iterator)[0], value)
        self.assertEqual(target.request.call_count, 2)
        self.assertEqual([p['id'] for p, _ in iterator], ['aaa111'])
        calls = target.request.call_args_list
        self.assertEqual([c.args[0] for c in calls], [reddit_search.ROOT + '/search/',
                         'https://www.reddit.com/comments/abc123/.json', 'https://www.reddit.com/comments/def456/.json',
                         reddit_search.ROOT + '/search/', 'https://www.reddit.com/comments/aaa111/.json'])
        self.assertEqual(calls[3].kwargs['params']['after'], 't3_def456')
        self.assertFalse(calls[0].kwargs['allow_redirects'])

    def test_comments_lookup_reuses_full_post_and_preserves_comment_iterator(self):
        config.set(('extractor', 'reddit'), 'comments', 10)
        target = self.target()
        value = post(num_comments=1)
        detail = details(value)
        detail.json.return_value[1]['data']['children'] = [{'kind': 't1', 'data': {'id': 'comment', 'replies': ''}}]
        target.request = Mock(side_effect=[response(page()), detail])
        rows = list(target.submissions())
        self.assertEqual(rows[0][0], value)
        self.assertEqual(list(rows[0][1]), [{'id': 'comment', 'replies': ''}])
        self.assertEqual(target.request.call_count, 2)

    def test_default_api_and_non_search_targets_are_unchanged(self):
        config.set(('extractor', 'reddit'), 'native-search-api', 'api')
        target = self.target(install=False)
        original = target.api._call
        reddit_search.install(target)
        self.assertEqual(target.api._call, original)
        config.set(('extractor', 'reddit'), 'native-search-api', 'browser')
        for url in ('https://www.reddit.com/user/example/submitted/?sort=top', 'https://www.reddit.com/comments/abc123/'):
            target = self.target(url, install=False)
            original = target.api._call
            reddit_search.install(target)
            self.assertEqual(target.api._call, original)

    def test_oauth_and_unrecognized_modes_fail_explicitly(self):
        config.set(('extractor', 'reddit'), 'api', 'oauth')
        config.set(('extractor', 'reddit'), 'client-id', 'fixture')
        config.set(('extractor', 'reddit'), 'refresh-token', 'fixture')
        with self.assertRaisesRegex(InvalidData, 'requires api=rest'):
            self.target()
        config.set(('extractor', 'reddit'), 'native-search-api', 'typo')
        with self.assertRaisesRegex(InvalidData, 'Unsupported'):
            self.target()

    def test_failed_or_mismatched_post_never_completes_page(self):
        for failure in (exception.AuthorizationError(), exception.HttpError('404', None),
                        details(post('other')), details({'id': 'abc123', 'name': 't3_abc123'})):
            target = self.target()
            target.request = Mock(side_effect=[response(page()), failure])
            with self.subTest(failure=type(failure).__name__), self.assertRaises(Exception):
                list(target.submissions())
            self.assertEqual(target.request.call_count, 2)

    def test_status_redirect_wrong_content_type_and_network_failure_do_not_complete(self):
        for failure in (response(page((), empty=True), status=403), response('', status=429),
                        response('', status=302), response(data={'data': {'children': [], 'after': None}}),
                        RuntimeError('request budget paused')):
            target = self.target()
            target.request = Mock(side_effect=[failure])
            with self.subTest(failure=type(failure).__name__), self.assertRaises((InvalidData, RuntimeError)):
                list(target.submissions())

    def test_actual_extractor_messages_keep_galleries_and_original_metadata(self):
        target = self.target(install=False)
        producer = Mock(saved_pagination=None)
        pagination.install(target, producer)
        value = post(url='https://www.reddit.com/gallery/abc123', gallery_data={'items': [{'media_id': 'imagea'}, {'media_id': 'imageb'}]},
                     media_metadata={i: {'status': 'valid', 'e': 'Image', 'm': 'image/jpeg', 's': {'u': 'https://i.redd.it/' + i + '.jpg'}}
                                     for i in ('imagea', 'imageb')})
        target.request = Mock(side_effect=[response(page()), details(value)])
        messages = list(target.items())
        self.assertEqual([row[1] for row in messages[1:]], ['https://i.redd.it/imagea.jpg', 'https://i.redd.it/imageb.jpg'])
        self.assertEqual(messages[0][2]['title'], value['title'])
        self.assertEqual(messages[0][2]['selftext'], value['selftext'])
        self.assertEqual(messages[0][2]['gallery_data'], value['gallery_data'])
        producer.begin_page.assert_called_once()

    def test_real_producer_saved_page_is_durable_before_html_and_post_failure(self):
        test_producer.ProducerTests.setUp(self)
        data = test_producer.reddit_data('anchor')
        anchor, _ = self.producer.cursor(self.producer.prepare(data))
        saved = {'extractor': 'reddit:home-search', 'url': URL, 'cursor': 't3_prior', 'item': anchor}
        self.lease.run['progress'] = {'items_seen': 10, 'files_completed': 10, 'cursor': anchor, 'pagination': saved}
        producer = Producer(self.box, self.lease, self.root, extractor_version='fixture')
        target = self.target(install=False)
        calls = []
        def request(url, **kwargs):
            self.assertEqual(producer.pagination, saved)
            self.assertEqual(producer.resume_cursor, anchor)
            calls.append((url, copy.deepcopy(kwargs)))
            if len(calls) == 1:
                self.assertEqual(kwargs['params']['after'], 't3_prior')
                return response(page())
            raise RuntimeError('metadata unavailable')
        target.request = request
        pagination.install(target, producer)
        with self.assertRaisesRegex(RuntimeError, 'metadata unavailable'):
            list(target.submissions())
        self.assertTrue(producer.pagination_started)
        self.assertEqual(producer.pagination, saved)
        self.assertEqual(self.lease.checkpoints, [])
        self.assertEqual(len(calls), 2)

    def test_native_budget_and_block_detection_apply_to_html_and_post_requests(self):
        for first, second in ((response('', status=429), None),
                              (response("You've been blocked by network security"), None),
                              (response(page()), response('', status=429))):
            producer = Mock(saved_pagination=None)
            producer.request_source.return_value = {'source_scope': 'service:reddit'}
            producer.fail_source.side_effect = lambda code, scope: (_ for _ in ()).throw(SourceFailure(code, scope))
            session = SourceSession(producer, Mock(request=Mock(side_effect=[first, second])))
            target = self.target(install=False)
            target.request = lambda url, **kwargs: session.request('GET', url, **kwargs)
            pagination.install(target, producer)
            with self.assertRaises(SourceFailure) as raised:
                list(target.submissions())
            self.assertEqual(raised.exception.code, 'rate_limited')
            urls = [call.args[0] for call in producer.request_source.call_args_list]
            self.assertEqual(urls[0], reddit_search.ROOT + '/search/')
            self.assertEqual(len(urls), 2 if second is not None else 1)
            self.assertEqual(producer.report_source.call_count, len(urls))
            producer.begin_page.assert_called_once()


if __name__ == '__main__':
    unittest.main()
