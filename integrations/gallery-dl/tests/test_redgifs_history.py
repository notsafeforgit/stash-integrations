import copy
import unittest
from types import SimpleNamespace

from stash_ingest import redgifs_history
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

    def test_limit_expands_supported_filters_and_reports_only_remaining_gap(self):
        values = list(self.install())
        self.assertEqual([(r.get('type'), r['order'], r['page']) for r in self.calls],
                         [(None, 'new', 1), (None, 'new', 2), (None, 'new', 3),
                          ('i', 'new', 1), ('i', 'new', 2),
                          ('g', 'new', 1), ('g', 'new', 2), ('g', 'old', 1), ('g', 'old', 2)])
        self.assertEqual(len(values), 8)
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
                         [('g', 'old', 1), ('g', 'old', 2)])
        self.assertEqual(self.producer.redgifs_history_coverage['limited_types'], ['videos'])

    def test_completed_checkpoint_survives_lost_finish_without_more_requests(self):
        list(self.install())
        resume = self.producer.pages[-1]
        self.assertEqual(redgifs_history.decode(resume['cursor'])['phase'], 5)
        self.calls.clear()
        self.producer = Producer()
        list(self.install(resume))
        self.assertEqual(self.calls, [])
        self.assertEqual(self.producer.pages, [resume])
        self.assertEqual(self.producer.redgifs_history_coverage['limited_types'], ['videos'])

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
