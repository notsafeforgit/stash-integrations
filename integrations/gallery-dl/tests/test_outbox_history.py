from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import uuid

from helpers import PRODUCER, RUN, capture, file_event, receipt
from stash_ingest.client import Unavailable
from stash_ingest.encoding import InvalidData, encode
from stash_ingest.outbox import Conflict, Outbox
from stash_ingest.outbox_history import apply, finish_retirement, inspect


class HistoryFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'outbox.sqlite'
        self.now = [100000.0]
        self.box = Outbox(self.path, 'http://localhost:8009', PRODUCER, clock=lambda: self.now[0])
        self.addCleanup(lambda: self.box.close())
        self.native = sqlite3.connect(Path(self.temp.name) / 'native.sqlite')
        self.native.row_factory = sqlite3.Row
        self.addCleanup(self.native.close)
        self.native.executescript('''
            CREATE TABLE native_schema(singleton INTEGER PRIMARY KEY,lineage TEXT);
            INSERT INTO native_schema VALUES(1,'org.notsafeforgit.stash.native-archive');
            CREATE TABLE ingest_producers(uuid TEXT PRIMARY KEY);
            CREATE TABLE source_runs(uuid TEXT PRIMARY KEY,state TEXT,revision INTEGER,updated_at_ms INTEGER);
            CREATE TABLE ingest_receipts(producer_uuid TEXT,event_uuid TEXT,digest TEXT,credential_uuid TEXT,
                collection_uuid TEXT,collection_revision INTEGER,root_uuid TEXT,run_uuid TEXT,kind TEXT,
                post_uuid TEXT,capture_uuid TEXT,job_uuid TEXT,result TEXT,committed_at TEXT,
                PRIMARY KEY(producer_uuid,event_uuid));
        ''')
        self.native.execute('INSERT INTO ingest_producers VALUES(?)', (PRODUCER,))
        self.native.execute("INSERT INTO source_runs VALUES(?,'succeeded',3,100000000)", (RUN,))
        self.native.commit()
        self.receipts = {}

    def accept(self, event, *, acknowledge=True):
        self.box.enqueue(encode(event))
        delivery, = self.box.claim(str(uuid.uuid4()))
        saved = receipt(event)
        columns = ('producer_uuid','event_uuid','sha256','credential_uuid','collection_uuid','collection_revision',
                   'root_uuid','run_uuid','kind','post_uuid','capture_uuid','job_uuid')
        self.native.execute('INSERT INTO ingest_receipts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                            tuple(saved.get(k) for k in columns) + (json.dumps(saved['result']), saved['committed_at']))
        self.native.commit()
        self.receipts[event['event_uuid']] = saved
        if acknowledge:
            self.box.acknowledge(delivery, saved)
        return saved

    def plan(self):
        self.now[0] = 300000
        return inspect(self.box.db, self.native, self.box.endpoint, now=self.now[0])

    def lookup(self, event_uuid):
        self.assertFalse(self.box.db.in_transaction)
        if event_uuid not in self.receipts:
            raise Unavailable('http_error', 404)
        return self.receipts[event_uuid]


class HistoryTests(HistoryFixture):
    def test_verified_history_is_removed_and_sequence_survives_empty_queue_and_reopen(self):
        source = capture()
        self.accept(source)
        self.accept(file_event(source))
        maximum = self.box.db.execute('SELECT max(seq) FROM events').fetchone()[0]
        plan = self.plan()
        self.assertEqual(len(plan['runs']), 1)
        self.assertEqual(apply(self.box, plan, batch_size=1), 2)
        self.assertEqual(self.box.db.execute('SELECT count(*) FROM events').fetchone()[0], 0)
        self.assertEqual(self.box.db.execute('PRAGMA foreign_key_check').fetchall(), [])
        self.box.close()
        self.box = Outbox(self.path, 'http://localhost:8009', PRODUCER)
        self.box.enqueue(encode(capture(run_uuid=str(uuid.uuid4()))))
        self.assertGreater(self.box.db.execute('SELECT seq FROM events').fetchone()[0], maximum)
        self.assertEqual(self.native.execute('SELECT count(*) FROM ingest_receipts').fetchone()[0], 2)

    def test_lost_acknowledgement_and_pending_parent_dependency_are_preserved(self):
        source = capture()
        self.accept(source)
        child = file_event(source)
        self.accept(child, acknowledge=False)
        before = [tuple(r) for r in self.box.db.execute('SELECT * FROM events ORDER BY seq')]
        plan = self.plan()
        self.assertEqual(plan['runs'], [])
        self.assertEqual(apply(self.box, plan), 0)
        self.assertEqual(before, [tuple(r) for r in self.box.db.execute('SELECT * FROM events ORDER BY seq')])
        delivery, = self.box.claim(str(uuid.uuid4()))
        self.assertEqual(delivery.body, encode(child))
        self.box.acknowledge(delivery, self.receipts[child['event_uuid']])

    def test_open_unknown_and_recent_runs_remain(self):
        self.accept(capture())
        for state in ('queued', 'running', 'deferred'):
            self.native.execute('UPDATE source_runs SET state=?', (state,))
            self.assertEqual(self.plan()['runs'], [])
        self.native.execute("UPDATE source_runs SET state='succeeded',updated_at_ms=300000000")
        self.assertEqual(self.plan()['runs'], [])
        self.native.execute('DELETE FROM source_runs')
        self.assertEqual(self.plan()['runs'], [])

    def test_missing_changed_or_extra_native_receipts_block_retirement(self):
        event = capture()
        self.accept(event)
        self.native.execute("UPDATE ingest_receipts SET digest=?", ('a' * 64,))
        plan = self.plan()
        self.assertEqual(plan['runs'], [])
        self.assertEqual(plan['mismatched_runs'], [RUN])
        self.native.execute("UPDATE ingest_receipts SET digest=?", (self.receipts[event['event_uuid']]['sha256'],))
        columns = [row[1] for row in self.native.execute('PRAGMA table_info(ingest_receipts)')]
        duplicate = dict(self.native.execute('SELECT * FROM ingest_receipts').fetchone())
        duplicate['event_uuid'] = str(uuid.uuid4())
        self.native.execute('INSERT INTO ingest_receipts VALUES(' + ','.join('?' for _ in columns) + ')',
                            tuple(duplicate[k] for k in columns))
        self.assertEqual(self.plan()['mismatched_runs'], [RUN])
        self.native.execute('DELETE FROM ingest_receipts')
        self.assertEqual(self.plan()['runs'], [])
        self.assertEqual(self.box.db.execute('SELECT count(*) FROM events').fetchone()[0], 1)

    def test_new_local_work_after_inspection_prevents_deletion(self):
        self.accept(capture())
        plan = self.plan()
        self.box.enqueue(encode(capture()))
        with self.assertRaisesRegex(InvalidData, 'changed after verification'):
            apply(self.box, plan)
        self.assertEqual(self.box.db.execute('SELECT count(*) FROM events').fetchone()[0], 2)
        self.assertEqual(self.box.db.execute('SELECT count(*) FROM retired_event_runs').fetchone()[0], 0)

    def test_retired_replay_uses_stash_without_recreating_history(self):
        event = capture()
        saved = self.accept(event)
        apply(self.box, self.plan())
        self.box.receipt_lookup = self.lookup
        self.assertEqual(self.box.enqueue(encode(event)), event['event_uuid'])
        self.assertEqual(self.box.receipt(event['event_uuid']), saved)
        with self.assertRaises(Conflict):
            self.box.enqueue(encode(dict(event, metadata={'title': 'changed'})))
        with self.assertRaises(Conflict):
            self.box.enqueue(encode(capture()))
        self.assertEqual(self.box.db.execute('SELECT count(*) FROM events').fetchone()[0], 0)
        self.box.receipt_lookup = lambda _: (_ for _ in ()).throw(Unavailable('network_unavailable'))
        with self.assertRaises(Unavailable):
            self.box.enqueue(encode(event))

    def test_interrupted_retirement_resumes_and_blocks_stale_writers(self):
        source = capture()
        self.accept(source)
        self.accept(file_event(source))
        plan = self.plan()
        def interrupted(box, run_uuid, **_):
            with box.transaction():
                box.db.execute('DELETE FROM events WHERE seq=(SELECT max(seq) FROM events)')
            raise OSError('simulated interruption')
        with patch('stash_ingest.outbox_history.finish_retirement', side_effect=interrupted):
            with self.assertRaises(OSError):
                apply(self.box, plan)
        self.box.receipt_lookup = self.lookup
        self.assertEqual(self.box.enqueue(encode(source)), source['event_uuid'])
        with self.assertRaises(sqlite3.IntegrityError):
            self.box.db.execute("UPDATE events SET attempts=attempts+1")
        self.assertEqual(finish_retirement(self.box, RUN, batch_size=1), 1)
        self.assertEqual(apply(self.box, plan), 0)
        self.assertEqual(self.box.db.execute('PRAGMA integrity_check').fetchone()[0], 'ok')

    def test_compaction_preserves_retirement_proofs_and_pending_bytes(self):
        self.accept(capture())
        apply(self.box, self.plan())
        event = capture(run_uuid=str(uuid.uuid4()))
        self.box.enqueue(encode(event))
        before = [tuple(r) for r in self.box.db.execute('SELECT * FROM events')]
        proofs = [tuple(r) for r in self.box.db.execute('SELECT * FROM retired_event_runs')]
        self.box.db.execute('VACUUM')
        self.assertEqual(before, [tuple(r) for r in self.box.db.execute('SELECT * FROM events')])
        self.assertEqual(proofs, [tuple(r) for r in self.box.db.execute('SELECT * FROM retired_event_runs')])


if __name__ == '__main__':
    unittest.main()
