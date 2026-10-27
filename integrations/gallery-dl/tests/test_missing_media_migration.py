from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import sqlite3
import unittest
from unittest.mock import patch

from stash_ingest import missing_media
from stash_ingest.encoding import InvalidData
from stash_ingest.outbox import Outbox
from stash_ingest.run_queue import RunQueue
from test_run_queue import request, admission
import uuid
from helpers import PRODUCER
import test_outbox_migration


class MissingMediaMigrationTests(unittest.TestCase):
    open = test_outbox_migration.OutboxMigrationTests.open

    def setUp(self):
        test_outbox_migration.OutboxMigrationTests.setUp(self)
        with closing(self.open()) as box:
            queue = RunQueue(box)
            queue.enqueue(request(), ticket_uuid=str(uuid.uuid4()))
            delivery = queue.claim(str(uuid.uuid4()))
            queue.admit(delivery, admission(delivery.body))
            queue.enqueue(request(30, 40), ticket_uuid=str(uuid.uuid4()))
            queue.claim(str(uuid.uuid4()))
            box.db.execute('DROP TABLE scheduled_calls')
            box.db.execute('DROP TABLE missing_media')
            box.db.execute("DROP INDEX request_run_tickets")
            box.db.execute('PRAGMA user_version=18')
        self.tables = [row[0] for row in self.db.execute("SELECT name FROM sqlite_schema WHERE type='table'")]
        self.frozen = {table: self.db.execute('SELECT * FROM '+table).fetchall() for table in self.tables}
        self.legacy = self.path.with_name(self.path.name + '.missing-media.sqlite')
        self.rows = [('imgur', 'Missing', 3, '2026-10-19T12:00:00+00:00', '2026-10-19T12:00:02+00:00', 404),
                     ('redgifs', 'partial', 2, '2026-10-19T11:00:00+00:00', '2026-10-19T11:00:01+00:00', 410)]

    def cache(self):
        with closing(sqlite3.connect(self.legacy)) as db:
            missing_media.initialize(db)
            db.execute('CREATE TABLE binding(endpoint TEXT NOT NULL,producer TEXT NOT NULL)')
            db.execute('INSERT INTO binding VALUES(?,?)', ('http://fixture.invalid', PRODUCER))
            db.executemany('INSERT INTO missing_media VALUES(?,?,?,?,?,?)', self.rows)
            db.execute('PRAGMA application_id=0x534D4953')  # SMIS
            db.execute('PRAGMA user_version=1')
            db.commit()

    def unchanged(self):
        self.assertEqual(self.db.execute('SELECT * FROM events ORDER BY seq').fetchall(), self.before)
        self.assertEqual(self.db.execute('PRAGMA foreign_key_check').fetchall(), [])
        self.assertEqual({table: self.db.execute('SELECT * FROM '+table).fetchall() for table in self.tables}, self.frozen)

    def test_concurrent_upgrade_imports_once_preserves_queue_and_restart_ignores_old_file(self):
        self.cache()
        original = self.legacy.read_bytes()
        def upgrade(_):
            with closing(self.open()) as box:
                return box.db.execute('PRAGMA user_version').fetchone()[0]
        with ThreadPoolExecutor(max_workers=3) as pool:
            self.assertEqual(list(pool.map(upgrade, range(3))), [22, 22, 22])
        self.assertEqual(self.db.execute('SELECT * FROM missing_media ORDER BY service,item').fetchall(), self.rows)
        self.unchanged()
        with closing(self.open()) as box:
            missing_media.failed(box, {'service': 'redgifs', 'item': 'partial'}, 404)
        self.assertEqual(self.legacy.read_bytes(), original)
        self.legacy.write_bytes(b'obsolete file must never be read again')
        with closing(self.open()) as box:
            self.assertEqual(missing_media.cached(box, {'service':'redgifs','item':'partial'})['attempts'], 3)

    def test_upgrade_without_old_file_and_fresh_outbox_use_only_the_producer_database(self):
        with closing(self.open()) as box:
            self.assertEqual(box.db.execute('PRAGMA user_version').fetchone()[0], 22)
            self.assertEqual(box.db.execute('SELECT count(*) FROM missing_media').fetchone()[0], 0)
        self.assertFalse(self.legacy.exists())
        self.unchanged()
        fresh = self.path.with_name('fresh.sqlite')
        with closing(Outbox(fresh, 'http://fixture.invalid', PRODUCER)) as box:
            missing_media.failed(box, {'service':'imgur','item':'Missing'}, 404)
            self.assertEqual(box.db.execute('SELECT attempts FROM missing_media').fetchone()[0], 1)
        self.assertFalse(fresh.with_name(fresh.name+'.missing-media.sqlite').exists())

    def test_interrupted_import_rolls_back_table_version_and_rows_then_retries(self):
        self.cache()
        original = self.legacy.read_bytes()
        connect = sqlite3.connect
        class Interrupted(sqlite3.Connection):
            def execute(db, sql, *args, **kwargs):
                if sql == 'INSERT INTO missing_media VALUES(?,?,?,?,?,?)' and args[0][0] == 'redgifs':
                    raise OSError('simulated interruption after first imported item')
                return super().execute(sql, *args, **kwargs)
        def connection(*args, **kwargs):
            kwargs['factory'] = Interrupted
            return connect(*args, **kwargs)
        with patch('stash_ingest.outbox.sqlite3.connect', side_effect=connection), self.assertRaises(OSError):
            self.open()
        self.assertEqual(self.db.execute('PRAGMA user_version').fetchone()[0], 18)
        self.assertIsNone(self.db.execute("SELECT name FROM sqlite_schema WHERE name='missing_media'").fetchone())
        self.assertEqual(self.legacy.read_bytes(), original)
        self.unchanged()
        with closing(self.open()) as box:
            self.assertEqual(box.db.execute('SELECT count(*) FROM missing_media').fetchone()[0], 2)

    def test_foreign_newer_misbound_or_malformed_legacy_state_is_preserved_and_refused(self):
        self.cache()
        pristine = self.legacy.read_bytes()
        changes = ["PRAGMA application_id=123", "PRAGMA user_version=2", "UPDATE binding SET producer='foreign'",
                   "UPDATE missing_media SET first_seen='invalid'", "UPDATE missing_media SET item=''", 'DROP TABLE binding']
        for sql in changes:
            with self.subTest(sql=sql):
                self.legacy.write_bytes(pristine)
                with closing(sqlite3.connect(self.legacy)) as db:
                    db.execute(sql); db.commit()
                invalid = self.legacy.read_bytes()
                with self.assertRaises((InvalidData, sqlite3.DatabaseError)):
                    self.open()
                self.assertEqual(self.db.execute('PRAGMA user_version').fetchone()[0], 18)
                self.assertEqual(self.legacy.read_bytes(), invalid)
                self.unchanged()

    def test_schema_collision_and_wrong_outbox_binding_roll_back(self):
        self.cache()
        with self.assertRaises(InvalidData):
            Outbox(self.path, 'http://elsewhere.invalid', PRODUCER)
        self.assertEqual(self.db.execute('PRAGMA user_version').fetchone()[0], 18)
        self.db.execute('CREATE TABLE missing_media(unrecognized TEXT)')
        with self.assertRaises(sqlite3.DatabaseError):
            self.open()
        self.assertEqual(self.db.execute('PRAGMA user_version').fetchone()[0], 18)
        self.unchanged()
