import copy
import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from stash_ingest import redgifs_history
from stash_ingest.client import Unavailable
from stash_ingest.encoding import InvalidData
from stash_ingest.runs import SourceFailure


URL = 'https://www.redgifs.com/users/example'


class Producer:
    replay_archive = False
    source_failure = None

    def __init__(self):
        self.pages = []

    def begin_page(self, page):
        self.pages.append(dict(page))

    def fail_source(self, code, scope, *, page_limit=None):
        self.source_failure = SourceFailure(code, scope, page_limit=page_limit)
        raise self.source_failure


class RedgifsHistoryTests(unittest.TestCase):
    def setUp(self):
        self.producer = Producer()
        self.calls = []
        self.counts = {'': 9, 'i': 2, 'g': 5}
        self.target = SimpleNamespace(url=URL, key='Example', per_page=100,
                                      api=SimpleNamespace(_call=self.call))

    def call(self, endpoint, params):
        self.assertEqual(endpoint, '/v2/users/example/search')
        self.calls.append(dict(params))
        page, kind = params['page'], params.get('type', '')
        if page > 2:
            failure = SourceFailure('source_history_limit', 'service:redgifs', page_limit=2)
            self.producer.source_failure = failure
            raise failure
        return {'pages': self.counts[kind], 'gifs': [dict(params)]}

    def install(self, resume=None):
        redgifs_history.install(self.target, self.producer, resume)
        return self.target.gifs()

    def test_limit_completes_available_history_after_all_useful_views_are_exhausted(self):
        values = list(self.install())
        self.assertEqual([(r.get('type'), r['order'], r['page']) for r in self.calls],
                         [(None, 'new', 1), (None, 'new', 2), (None, 'new', 3),
                          ('i', 'new', 1), ('i', 'new', 2),
                          ('g', 'new', 1), ('g', 'new', 2), ('g', 'old', 1), ('g', 'old', 2),
                          ('g', 'best', 1), ('g', 'best', 2), ('g', 'best', 3)])
        self.assertEqual(len(values), 10)
        self.assertIsNone(self.producer.source_failure)
        self.assertEqual(self.producer.redgifs_history_coverage, {
            'policy': 'available', 'page_limit': 2, 'image_pages': 2, 'video_pages': 5,
            'limited_types': ['videos']})

    def test_reverse_only_reads_missing_pages_and_complete_filters_have_no_gap(self):
        self.counts['g'] = 3
        list(self.install())
        self.assertEqual([(r['order'], r['page']) for r in self.calls if r.get('type') == 'g'],
                         [('new', 1), ('new', 2), ('old', 1)])
        self.assertEqual(self.producer.redgifs_history_coverage['limited_types'], [])

    def test_normal_profiles_do_not_need_extra_requests(self):
        self.counts[''] = 2
        list(self.install())
        self.assertEqual(len(self.calls), 2)
        self.assertFalse(hasattr(self.producer, 'redgifs_history_coverage'))

    def test_numeric_legacy_checkpoint_is_used_exactly_before_upgrade(self):
        resume = {'extractor': 'redgifs:user', 'url': URL, 'cursor': '3', 'item': ''}
        list(self.install(resume))
        self.assertEqual(self.producer.pages[0], resume)
        self.assertEqual(self.calls[0]['page'], 3)
        self.assertEqual(redgifs_history.decode(self.producer.pages[1]['cursor'])['phase'], 1)

    def test_restart_replays_current_filtered_page_and_preserves_gap_note(self):
        iterator = self.install()
        for _ in range(7):
            next(iterator)
        resume = self.producer.pages[-1]
        self.assertEqual(redgifs_history.decode(resume['cursor'])['phase'], 4)
        iterator.close()
        self.calls.clear()
        self.producer = Producer()
        list(self.install(resume))
        self.assertEqual(self.producer.pages[0], resume)
        self.assertEqual([(r['type'], r['order'], r['page']) for r in self.calls],
                         [('g', 'old', 1), ('g', 'old', 2),
                          ('g', 'best', 1), ('g', 'best', 2), ('g', 'best', 3)])
        self.assertEqual(self.producer.redgifs_history_coverage['limited_types'], ['videos'])

    def test_completed_checkpoint_survives_lost_finish_without_more_requests(self):
        list(self.install())
        resume = self.producer.pages[-1]
        self.assertEqual(redgifs_history.decode(resume['cursor'])['phase'], 7)
        self.calls.clear()
        self.producer = Producer()
        list(self.install(resume))
        self.assertEqual(self.calls, [])
        self.assertEqual(self.producer.pages, [resume])
        self.assertEqual(self.producer.redgifs_history_coverage['limited_types'], ['videos'])

    def test_legacy_terminal_checkpoint_runs_popularity_before_completing(self):
        resume = {'extractor': 'redgifs:user', 'url': URL, 'cursor': redgifs_history.LEGACY_PREFIX + json.dumps(
            dict(phase=5, page=1, limit=2, images=2, videos=5)), 'item': ''}
        list(self.install(resume))
        self.assertEqual([(r['type'], r['order'], r['page']) for r in self.calls],
                         [('g', 'best', 1), ('g', 'best', 2), ('g', 'best', 3)])
        self.assertEqual(self.producer.pages[0], resume)
        self.assertEqual(redgifs_history.decode(self.producer.pages[-1]['cursor'])['phase'], 7)
        self.assertEqual(self.producer.redgifs_history_coverage['limited_types'], ['videos'])

    def test_rate_limit_at_each_filtered_pass_preserves_page_and_requires_retry(self):
        for phase in range(1, 7):
            with self.subTest(phase=phase):
                self.setUp()
                self.counts = {'': 5, 'i': 5, 'g': 5}
                state = dict(phase=phase, page=1, limit=2, images=5, videos=5)
                resume = {'extractor':'redgifs:user','url':URL,'cursor':redgifs_history.encode(state),'item':''}
                original = self.call
                failure = SourceFailure('rate_limited', 'service:redgifs')
                def limited(*_):
                    self.producer.source_failure = failure
                    raise failure
                self.target.api._call = limited
                with self.assertRaises(SourceFailure) as raised:
                    list(self.install(resume))
                self.assertIs(raised.exception, failure)
                self.assertEqual(self.producer.pages, [resume])
                self.assertFalse(hasattr(self.producer, 'redgifs_history_coverage'))
                self.producer = Producer()
                self.target.api._call = original
                list(self.install(resume))
                self.assertEqual(self.calls[0]['page'], 1)
                self.assertEqual(self.calls[0]['type'], 'i' if phase in (1, 2, 5) else 'g')
                self.assertEqual(self.calls[0]['order'], 'best' if phase >= 5 else 'old' if phase in (2, 4) else 'new')
                self.assertEqual(self.producer.redgifs_history_coverage['limited_types'], ['images', 'videos'])

    def test_rate_limit_after_last_popularity_page_is_not_a_confirmed_ceiling(self):
        original = self.call
        def call(endpoint, params):
            if params['order'] == 'best' and params['page'] == 3:
                self.producer.fail_source('rate_limited', 'service:redgifs')
            return original(endpoint, params)
        self.target.api._call = call
        with self.assertRaises(SourceFailure) as raised:
            list(self.install())
        self.assertEqual(raised.exception.code, 'rate_limited')
        self.assertFalse(hasattr(self.producer, 'redgifs_history_coverage'))
        resume = self.producer.pages[-1]
        state = redgifs_history.decode(resume['cursor'])
        self.assertEqual((state['phase'], state['page']), (6, 3))
        self.calls.clear()
        self.producer = Producer()
        self.target.api._call = original
        list(self.install(resume))
        self.assertEqual([(r['order'], r['page']) for r in self.calls], [('best', 3)])
        self.assertIsNone(self.producer.source_failure)

    def test_popularity_can_retrieve_a_post_missing_from_newest_and_oldest(self):
        self.target.per_page = 1
        def call(_, params):
            if params.get('type') == 'i':
                return {'pages': 0, 'total': 0, 'gifs': []}
            if params['page'] > 2:
                self.producer.fail_source('source_history_limit', 'service:redgifs', page_limit=2)
            ids = {'new': ['newest', 'recent'], 'old': ['oldest', 'old'], 'best': ['middle', 'newest']}
            return {'pages': 5, 'gifs': [{'id': ids[params['order']][params['page'] - 1]}]}
        self.target.api._call = call
        values = list(self.install())
        self.assertEqual({v['id'] for v in values}, {'newest', 'recent', 'middle', 'old', 'oldest'})
        self.assertEqual(redgifs_history.decode(self.producer.pages[-1]['cursor'])['phase'], 7)

    def test_empty_or_inconsistent_claimed_pages_do_not_establish_coverage(self):
        for value in ({'pages':1,'gifs':[]}, {'pages':0,'gifs':[{}]},
                      {'pages':0,'gifs':[],'total':10}, {'pages':0,'gifs':[],'total':False},
                      {'pages':1,'gifs':[{}],'page':2}, {'pages':1,'gifs':[{}],'page':True}):
            with self.subTest(value=value):
                self.target.api._call = lambda *_: value
                with self.assertRaises(InvalidData):
                    list(self.install())
                self.assertFalse(hasattr(self.producer, 'redgifs_history_coverage'))

    def test_explicitly_empty_account_can_finish(self):
        for value in ({'pages':0,'gifs':[]}, {'pages':1,'gifs':[],'total':0}):
            with self.subTest(value=value):
                self.target.api._call = lambda *_: value
                self.assertEqual(list(self.install()), [])

    def test_sparse_reported_items_require_all_media_types_and_orderings(self):
        def call(_, params):
            self.calls.append(dict(params))
            return {'page': params['page'], 'pages': 1, 'total': 2, 'gifs': []}
        self.target.api._call = call
        self.assertEqual(list(self.install()), [])
        self.assertEqual([(p.get('type'), p['order']) for p in self.calls],
                         [(None, 'new'), ('i', 'new'), ('i', 'old'), ('g', 'new'),
                          ('g', 'old'), ('i', 'best'), ('g', 'best')])
        self.assertTrue(self.producer.redgifs_history_coverage['sparse_listings'])
        self.assertEqual(self.producer.redgifs_history_coverage['page_limit'], 0)

    def test_sparse_middle_page_does_not_hide_later_posts(self):
        self.target.per_page = 1
        def call(_, params):
            self.calls.append(dict(params))
            page = params['page']
            return {'page': page, 'pages': 3, 'total': 3,
                    'gifs': [] if page == 2 else [{'id': (params.get('type', ''), params['order'], page)}]}
        self.target.api._call = call
        values = list(self.install())
        self.assertEqual(len(self.calls), 21)
        self.assertEqual(len(values), 14)
        self.assertTrue(any(v['id'] == ('g', 'best', 3) for v in values))

    def test_sparse_filter_revisits_previously_skipped_views_once(self):
        original = self.call
        def call(endpoint, params):
            value = original(endpoint, params)
            if params.get('type') == 'g' and params['order'] == 'new' and params['page'] == 1:
                value.update(total=401, gifs=[])
            return value
        self.target.api._call = call
        list(self.install())
        self.assertTrue(any(p.get('type') == 'i' and p['order'] == 'old' for p in self.calls))
        self.assertTrue(any(p.get('type') == 'i' and p['order'] == 'best' for p in self.calls))
        self.assertTrue(self.producer.redgifs_history_coverage['sparse_listings'])

    def test_sparse_checkpoint_survives_rate_limit_without_claiming_completion(self):
        def call(_, params):
            if params.get('type') == 'g' and params['order'] == 'best':
                self.producer.fail_source('rate_limited', 'service:redgifs')
            return {'pages': 1, 'total': 1, 'gifs': []}
        self.target.api._call = call
        with self.assertRaises(SourceFailure):
            list(self.install())
        resume = self.producer.pages[-1]
        self.assertEqual(redgifs_history.decode(resume['cursor'])['sparse'], 1)
        self.assertFalse(hasattr(self.producer, 'redgifs_history_coverage'))
        self.producer = Producer()
        self.target.api._call = lambda *_: {'pages': 1, 'total': 1, 'gifs': []}
        self.assertEqual(list(self.install(resume)), [])
        self.assertTrue(self.producer.redgifs_history_coverage['sparse_listings'])

    def test_previous_v2_checkpoint_preserves_its_current_page(self):
        cursor = redgifs_history.PREVIOUS_PREFIX + json.dumps(dict(phase=3, page=2, limit=2, images=2, videos=5))
        resume = {'extractor': 'redgifs:user', 'url': URL, 'cursor': cursor, 'item': ''}
        list(self.install(resume))
        self.assertEqual(self.producer.pages[0], resume)
        self.assertEqual((self.calls[0]['type'], self.calls[0]['page']), ('g', 2))

    def test_transient_and_account_failures_remain_failures(self):
        for code in ('rate_limited', 'timeout', 'account_deleted', 'account_not_found'):
            with self.subTest(code=code):
                failure = SourceFailure(code, 'service:redgifs')
                def call(*_):
                    self.producer.source_failure = failure
                    raise failure
                self.target.api._call = call
                with self.assertRaises(SourceFailure) as raised:
                    list(self.install())
                self.assertIs(raised.exception, failure)
                self.assertIs(self.producer.source_failure, failure)

    def test_invalid_first_page_and_unproven_limit_cannot_finish(self):
        for limit in (None, 2):
            with self.subTest(limit=limit):
                failure = SourceFailure('source_history_limit', 'service:redgifs', page_limit=limit)
                def call(*_):
                    self.producer.source_failure = failure
                    raise failure
                self.target.api._call = call
                with self.assertRaises(SourceFailure):
                    list(self.install())

    def test_malformed_responses_checkpoints_and_filtered_urls_fail_closed(self):
        for value in ({}, {'pages': True, 'gifs': []}, {'pages': 1, 'gifs': None}):
            self.target.api._call = lambda *_: value
            with self.assertRaises(InvalidData):
                list(self.install())
        for cursor in ('0', '-1', '{}', redgifs_history.PREFIX + '{"phase":4}', '1000001'):
            with self.assertRaises(InvalidData):
                redgifs_history.decode(cursor)
        self.target.url = URL + '?order=best'
        with self.assertRaises(InvalidData):
            self.install()


class InitialHistoryTests(unittest.TestCase):
    def setUp(self):
        self.run = {'collection_uuid': 'collection', 'collection_revision': 2,
                    'root_uuid': 'root', 'root_revision': 3, 'target_url': URL}
        self.previous = {**self.run, 'sequence': 1, 'state': 'succeeded', 'operation': 'download',
                         'completed': [{'since': None, 'until': '2026-11-03T00:00:00Z'}]}
        self.client = Mock()

    def mode(self, rows):
        self.client._request.return_value = rows
        return redgifs_history.scan_mode(self.client, self.run)

    def test_new_or_unfinished_accounts_require_initial_history(self):
        self.assertEqual(self.mode([]), 'initial')
        for change in ({'state': 'running'}, {'state': 'deferred'}, {'state': 'cancelled'},
                       {'completed': []}, {'operation': 'enrich'}, {'retrieval_url': URL + '?type=i'},
                       {'completed': [{'since': '2026-11-01T00:00:00Z', 'until': '2026-11-03T00:00:00Z'}]},
                       {'completed': [{'since': None, 'until': '2026-11-03T00:00:00Z', 'basis': 'traversal'}]},
                       {'collection_revision': 1}, {'root_revision': 2}, {'target_url': URL + 'other'}):
            with self.subTest(change=change):
                self.assertEqual(self.mode([{**self.previous, **change}]), 'initial')
        self.assertEqual(self.mode([self.previous]), 'routine')

    def test_history_lookup_paginates_and_rejects_ambiguous_results(self):
        page = [{**self.previous, 'sequence': i + 1, 'state': 'cancelled'} for i in range(50)]
        self.client._request.side_effect = [page, [{**self.previous, 'sequence': 51}]]
        self.assertEqual(redgifs_history.scan_mode(self.client, self.run), 'routine')
        self.assertIn(b'"after":50', self.client._request.call_args.args[2])
        self.client._request.side_effect = None
        for invalid in (None, {}, [self.previous, self.previous], [{**self.previous, 'completed': None}]):
            with self.subTest(invalid=invalid), self.assertRaises(Unavailable):
                self.mode(invalid)
