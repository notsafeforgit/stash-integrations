from contextlib import redirect_stdout
import io
import json
import sqlite3
import unittest
from unittest.mock import patch
import uuid

from stash_ingest.cli import main
from stash_ingest.encoding import digest, encode
from stash_ingest.outbox import Conflict
from stash_ingest.profile_retirement import retire_profile
import test_outbox
from helpers import capture, PRODUCER


class ProfileRetirementTests(unittest.TestCase):
    setUp = test_outbox.OutboxTests.setUp
    open = test_outbox.OutboxTests.open
    claim = test_outbox.OutboxTests.claim
    reason = 'Collection revision 1 was a manual batch; reviewed revision 2 is the account collection.'

    def profile(self):
        base = capture()
        event = {k: base[k] for k in ('protocol', 'producer_uuid', 'event_uuid', 'collection_uuid',
                 'collection_revision', 'root_uuid', 'run_uuid', 'observed_at', 'retention_policy', 'extractor_version')}
        event.update(kind='account.profile', owner_uuid=str(uuid.uuid4()), fence=1,
                     source={'category': 'reddit', 'user': {'id': '123', 'name': 'Example'}})
        body = encode(event)
        self.box.enqueue(body)
        return event, body

    def retire(self, event, body, **kwargs):
        return retire_profile(self.box, event['event_uuid'], digest(body), 2, self.reason, **kwargs)

    def test_retirement_preview_reopen_replay_and_exact_evidence(self):
        event, body = self.profile()
        delivery, = self.claim()
        self.box.fail(delivery, 'event_rejected_409', review=True)
        before = tuple(self.box.db.execute('SELECT * FROM events').fetchone())
        self.assertEqual(self.retire(event, body)['state'], 'preview')
        self.assertEqual(self.box.status()['counts']['review'], 1)
        first = self.retire(event, body, apply=True)
        self.assertFalse(first['server_acknowledged'])
        self.box.close()
        self.box = self.open()
        self.assertEqual(self.retire(event, body, apply=True), first)
        self.assertEqual(tuple(self.box.db.execute('SELECT * FROM events').fetchone()), before)
        self.assertIsNone(self.box.receipt(event['event_uuid']))
        self.assertEqual(self.claim(), [])
        self.box.enqueue(body)
        status = self.box.status()
        self.assertEqual((status['counts']['review'], status['counts']['retired'], status['queued_bytes']), (0, 1, 0))
        self.assertEqual(status['retired_bytes'], len(body))
        self.assertIsNone(status['oldest_queued_seconds'])
        with self.assertRaises(Conflict):
            self.box.retry(event['event_uuid'])
        with self.assertRaises(Conflict):
            self.box.enqueue(encode(dict(event, extractor_version='changed')))
        with self.assertRaises(Conflict):
            retire_profile(self.box, event['event_uuid'], digest(body), 3, self.reason, apply=True)
        for sql in ('DELETE FROM events', 'UPDATE events SET body=NULL',
                    'DELETE FROM profile_event_retirements', "UPDATE profile_event_retirements SET reason='changed'"):
            with self.assertRaises(sqlite3.IntegrityError):
                self.box.db.execute(sql)

    def test_reject_pending_sending_changed_digest_and_media_reports(self):
        event, body = self.profile()
        with self.assertRaises(Conflict):
            self.retire(event, body, apply=True)
        delivery, = self.claim()
        with self.assertRaises(Conflict):
            self.retire(event, body, apply=True)
        self.box.fail(delivery, 'event_rejected_409', review=True)
        with self.assertRaises(Conflict):
            retire_profile(self.box, event['event_uuid'], 'a' * 64, 2, self.reason, apply=True)
        other = capture()
        other_body = encode(other)
        self.box.enqueue(other_body)
        delivery, = self.claim()
        self.box.fail(delivery, 'event_rejected_409', review=True)
        with self.assertRaises(Conflict):
            self.retire(other, other_body, apply=True)
        self.assertEqual(self.box.status()['counts']['review'], 2)

    def test_cli_is_local_preview_by_default(self):
        event, body = self.profile()
        delivery, = self.claim()
        self.box.fail(delivery, 'event_rejected_409', review=True)
        args = ['--outbox', str(self.path), '--endpoint', self.box.endpoint, '--producer', PRODUCER,
                'retire-profile', event['event_uuid'], '--sha256', digest(body), '--superseding-revision', '2',
                '--reason', self.reason]
        for apply in (False, True):
            output = io.StringIO()
            with patch('stash_ingest.cli.Client', side_effect=AssertionError('No server request permitted')), redirect_stdout(output):
                self.assertEqual(main(args + (['--apply'] if apply else [])), 0)
            self.assertEqual(json.loads(output.getvalue())['state'], 'retired' if apply else 'preview')

    def test_interrupted_retirement_rolls_back_without_touching_report(self):
        event, body = self.profile()
        delivery, = self.claim()
        self.box.fail(delivery, 'event_rejected_409', review=True)
        with self.assertRaisesRegex(RuntimeError, 'interrupted'):
            with self.box.transaction():
                self.box.db.execute('INSERT INTO profile_event_retirements VALUES(?,?,?,?,?)',
                    (event['event_uuid'], digest(body), 2, self.reason, self.now[0]))
                raise RuntimeError('interrupted')
        self.box.close()
        self.box = self.open()
        self.assertEqual(self.box.status()['counts']['review'], 1)
        self.assertEqual(self.box.db.execute('SELECT body FROM events').fetchone()[0], body)
        self.retire(event, body, apply=True)

    def test_schema_22_upgrade_adds_only_ledger(self):
        event, body = self.profile()
        delivery, = self.claim()
        self.box.fail(delivery, 'event_rejected_409', review=True)
        before = tuple(self.box.db.execute('SELECT * FROM events').fetchone())
        from test_outbox_migration import schema_twenty_two
        schema_twenty_two(self.box.db)
        self.box.close()
        self.box = self.open()
        self.assertEqual(tuple(self.box.db.execute('SELECT * FROM events').fetchone()), before)
        from stash_ingest.outbox import SCHEMA
        self.assertEqual(self.box.db.execute('PRAGMA user_version').fetchone()[0], SCHEMA)
        self.retire(event, body, apply=True)
