from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
import json
import sqlite3
from unittest.mock import Mock, patch
import unittest
import uuid

from stash_ingest.client import Client, Unavailable
from stash_ingest.launchers import reddit_main
from stash_ingest.outbox import Capacity, Conflict, LeaseLost, Outbox
from stash_ingest.scheduled_calls import ScheduledCalls, advance_schedule
from stash_ingest.source_calls import SourceCalls
from helpers import PRODUCER, ROOT
import test_launchers


def spec():
    return {'service': 'reddit', 'mode': 'new', 'source': {'root_uuid': ROOT, 'operation': 'download',
        'policy_sha256': 'a' * 64, 'cooldown_seconds': 0,
        'window': {'since': '2026-10-17T07:00:00.000Z', 'until': '2026-10-24T07:00:00.000Z'}}}


class SchedulingTests(unittest.TestCase):
    def setUp(self):
        test_launchers.LauncherTests.setUp(self)
        self.now = 1000
        self.box = self.open()
        self.addCleanup(lambda: self.box.close())
        self.calls = ScheduledCalls(self.box)
        self.client = Mock(endpoint=self.box.endpoint, producer=PRODUCER)
        self.client.capabilities.return_value = {'source_subscriptions': True}
        self.client._request.return_value = {'items': [{'enabled': True, 'revision': 7, 'collection': {
            'uuid': '10000000-0000-4000-8000-000000000001', 'root_uuid': ROOT, 'namespace': 'native:reddit',
            'target_url': 'https://www.reddit.com/user/example/', 'state': 'active'}}], 'total': 1}

    invoke = test_launchers.LauncherTests.invoke

    def open(self):
        return Outbox(self.outbox, 'http://fixture.invalid', PRODUCER, clock=lambda: self.now)

    def record(self, call=None):
        return self.calls.record(call or self.call, 'b' * 64, spec)

    def test_launcher_saves_before_network_and_retry_freezes_original_policy_and_relative_window(self):
        self.value['source_category'] = 'reddit'
        self.profile.write_text(json.dumps(self.value))
        args = [*self.base, '--subscriptions', '--profile', str(self.profile), '--date-min-relative', '1 week ago']
        self.client.capabilities.side_effect = Unavailable('http_error', 503, 120)
        with patch('stash_ingest.launchers.Client', return_value=self.client), patch('stash_ingest.launchers.Outbox', side_effect=lambda *a, **k: self.open()):
            status, result, error = self.invoke(reddit_main, args)
            self.assertEqual((status, error, result['state']), (0, '', 'pending'))
            self.assertEqual(result['scheduling']['http_status'], 503)
            original = self.calls.definition(self.calls.find(self.call))
            self.profile.unlink()
            self.now += 60
            self.assertEqual(self.invoke(reddit_main, args)[1], result)
            self.assertEqual(self.client.capabilities.call_count, 1)
            self.assertEqual(self.invoke(reddit_main, [*args, '--strict-errors'])[0], 2)
        self.box.close()
        self.box = self.open()
        self.calls = ScheduledCalls(self.box)
        self.now += 60
        self.client.capabilities.side_effect = None
        self.assertEqual(advance_schedule(self.calls, self.client)['state'], 'recorded')
        recorded = SourceCalls(self.box).summary(self.call)
        self.assertEqual(recorded['definition']['window'], original['source']['window'])
        self.assertEqual(recorded['definition']['policy_sha256'], original['source']['policy_sha256'])
        self.assertEqual(recorded['target_count'], 2)
        with patch('stash_ingest.launchers.Client', return_value=self.client):
            self.assertEqual(self.invoke(reddit_main, args)[1]['state'], 'recorded')
        self.assertEqual(self.client.capabilities.call_count, 2)

    def test_outage_retries_indefinitely_with_capped_delay_and_honors_retry_after(self):
        self.record()
        for attempt in range(1, 14):
            self.client.capabilities.side_effect = Unavailable('network_unavailable')
            result = advance_schedule(self.calls, self.client)
            self.assertEqual(result['state'], 'pending')
            self.assertEqual(result['available_at'] - self.now, min(300, 15 * 2 ** min(attempt-1, 5)))
            self.assertEqual(advance_schedule(self.calls, self.client)['state'], 'idle')
            self.now = result['available_at']
        self.client.capabilities.side_effect = Unavailable('queue_full', 429, 3600)
        self.assertEqual(advance_schedule(self.calls, self.client)['available_at'], self.now + 3600)

    def test_authentication_and_malformed_responses_require_explicit_retry_without_blocking_other_calls(self):
        self.record()
        self.client.capabilities.side_effect = Unavailable('stash_token_rejected', 401)
        self.assertEqual(advance_schedule(self.calls, self.client)['state'], 'review')
        self.assertEqual(advance_schedule(self.calls, self.client)['state'], 'idle')
        second = str(uuid.uuid4())
        self.record(second)
        self.client.capabilities.side_effect = None
        self.assertEqual(advance_schedule(self.calls, self.client)['uuid'], second)
        self.calls.retry(self.call)
        self.client._request.return_value = {'items': 'not a list'}
        self.assertEqual(advance_schedule(self.calls, self.client)['state'], 'review')

    def test_interruption_and_stale_lease_cannot_duplicate_handoff(self):
        self.record()
        old = self.calls.claim()
        self.box.close()
        self.box = self.open()
        self.calls = ScheduledCalls(self.box)
        self.now += 901
        self.assertEqual(advance_schedule(self.calls, self.client)['state'], 'recorded')
        with self.assertRaises(LeaseLost):
            self.calls.finish(old, [], {})
        self.assertEqual(advance_schedule(self.calls, self.client)['state'], 'idle')
        self.assertEqual(SourceCalls(self.box).summary()['calls'], 1)

    def test_handoff_rolls_back_both_sides_and_can_resume(self):
        self.record()
        self.box.db.execute("""CREATE TRIGGER stop_handoff BEFORE UPDATE ON scheduled_calls WHEN NEW.state='recorded'
            BEGIN SELECT RAISE(ABORT,'interrupted'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            advance_schedule(self.calls, self.client)
        self.assertEqual(SourceCalls(self.box).summary()['calls'], 0)
        self.box.db.execute('DROP TRIGGER stop_handoff')
        self.now += 901
        self.assertEqual(advance_schedule(self.calls, self.client)['state'], 'recorded')

    def test_empty_subscription_list_is_final_and_capacity_retains_intents(self):
        self.record()
        with self.assertRaises(Capacity):
            ScheduledCalls(self.box, max_calls=1).record(str(uuid.uuid4()), 'b'*64, spec)
        self.client._request.return_value = {'items': [], 'total': 0}
        self.assertEqual(advance_schedule(self.calls, self.client)['state'], 'empty')
        self.assertEqual(advance_schedule(self.calls, self.client)['state'], 'idle')

    def test_concurrent_recorders_keep_one_intent_and_changed_options_conflict(self):
        def record(_):
            with closing(self.open()) as box:
                return ScheduledCalls(box).record(self.call, 'b'*64, spec)
        with ThreadPoolExecutor(max_workers=4) as workers:
            results = list(workers.map(record, range(8)))
        self.assertTrue(all(value == results[0] for value in results))
        with self.assertRaises(Conflict):
            self.calls.record(self.call, 'c'*64, spec)
        self.calls.record(self.call, 'b'*64, Mock(side_effect=AssertionError('recomputed')))

    def test_upgrade_from_nineteen_preserves_existing_caller_and_can_reopen(self):
        value = spec()['source']
        SourceCalls(self.box).record(self.call, 'b'*64, lambda: {**value, 'targets': ['https://example.invalid/profile']})
        before = SourceCalls(self.box).summary(self.call)
        self.box.db.execute('DROP TABLE scheduled_calls')
        self.box.db.execute("DROP INDEX request_run_tickets")
        self.box.db.execute('PRAGMA user_version=19')
        self.box.close()
        self.box = self.open()
        self.assertEqual(self.box.db.execute('PRAGMA user_version').fetchone()[0], 21)
        self.assertEqual(SourceCalls(self.box).summary(self.call), before)

    def test_real_http_503_response_is_retained_with_status_but_without_response_secrets(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        import os
        import threading
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_GET(self):
                self.send_response(503)
                self.send_header('Retry-After', '240')
                self.end_headers()
                self.wfile.write(b'private upstream diagnostic')
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            endpoint = f'http://127.0.0.1:{server.server_port}'
            with closing(Outbox(self.directory/'http.sqlite', endpoint, PRODUCER, clock=lambda: self.now)) as box:
                calls = ScheduledCalls(box)
                calls.record(self.call, 'b'*64, spec)
                with patch.dict(os.environ, {'STASH_INGEST_TOKEN': 'fixture-secret'}):
                    result = advance_schedule(calls, Client(endpoint, PRODUCER))
                self.assertEqual((result['state'], result['http_status'], result['available_at']), ('pending', 503, self.now+240))
                self.assertNotIn('private', json.dumps(result))
                self.assertNotIn('fixture-secret', json.dumps(result))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(5)

    def test_dispatch_retries_scheduling_while_other_work_can_progress(self):
        from types import SimpleNamespace
        from stash_ingest.worker_dispatch import dispatch_all
        self.record()
        self.client.capabilities.side_effect = Unavailable('http_error', 503)
        profiles = SimpleNamespace(uuid=str(uuid.uuid4()), entries=[])
        with patch('stash_ingest.worker_dispatch.WorkerDispatcher.once', return_value={'state': 'source_succeeded'}) as work:
            result = dispatch_all(self.box, self.client, profiles)
        self.assertEqual(result['scheduling']['state'], 'pending')
        self.assertEqual(result['state'], 'source_succeeded')
        work.assert_called_once()

    def test_later_subscription_page_failure_never_commits_a_partial_target_list(self):
        self.record()
        original = self.calls.definition(self.calls.find(self.call))
        template = self.client._request.return_value['items'][0]
        items = [{**template, 'collection': {**template['collection'],
            'uuid': f'10000000-0000-4000-8000-{i:012d}',
            'target_url': f'https://www.reddit.com/user/example{i}/'}} for i in range(1,101)]
        self.client._request.side_effect = [{'items': items, 'total': 100}, Unavailable('http_error', 503)]
        self.assertEqual(advance_schedule(self.calls, self.client)['state'], 'pending')
        self.assertEqual(SourceCalls(self.box).summary()['calls'], 0)
        self.now += 15
        self.client._request.side_effect = [{'items': items, 'total': 100}, {'items': [], 'total': 100}]
        self.assertEqual(advance_schedule(self.calls, self.client)['state'], 'recorded')
        recorded = SourceCalls(self.box).summary(self.call)
        self.assertEqual(recorded['target_count'], 200)
        self.assertEqual(recorded['definition']['window'], original['source']['window'])

    def test_http_recovery_classification_retains_terminal_errors_for_review(self):
        for status in (408, 425, 429, 500, 502, 503, 504, 400, 401, 403, 404, 409, 422):
            call = str(uuid.uuid4())
            self.record(call)
            self.client.capabilities.side_effect = Unavailable('http_error', status)
            result = advance_schedule(self.calls, self.client, call=call)
            self.assertEqual(result['state'], 'pending' if status in (408,425,429,500,502,503,504) else 'review')
            self.assertEqual(result['http_status'], status)
