from contextlib import closing
import sqlite3
import unittest

from stash_archive.bundle import FORMAT, database_metadata
from stash_archive.storage import InvalidArchive


class CapturePolicyTests(unittest.TestCase):
    def test_snapshot_identity_is_checked_without_claiming_deep_integrity(self):
        with closing(sqlite3.connect(':memory:')) as db:
            db.executescript(f"""
                CREATE TABLE native_schema(singleton INTEGER PRIMARY KEY, lineage TEXT);
                INSERT INTO native_schema VALUES(1,'{FORMAT}');
                CREATE TABLE schema_migrations(version INTEGER,dirty INTEGER);
                INSERT INTO schema_migrations VALUES(1000077,0);
                CREATE TABLE parent(id INTEGER PRIMARY KEY);
                CREATE TABLE child(parent INTEGER REFERENCES parent(id));
                INSERT INTO child VALUES(999);
            """)
            trace = []
            db.set_trace_callback(trace.append)
            self.assertEqual(database_metadata(db, 'library', verification='capture')['schema'], 1000077)
            self.assertFalse(any('integrity_check' in sql or 'foreign_key_check' in sql for sql in trace))
            with self.assertRaisesRegex(InvalidArchive, 'foreign key'):
                database_metadata(db, 'library')
            db.execute('UPDATE schema_migrations SET dirty=1')
            with self.assertRaisesRegex(InvalidArchive, 'clean native'):
                database_metadata(db, 'library', verification='capture')

    def test_unknown_verification_mode_fails_closed(self):
        with closing(sqlite3.connect(':memory:')) as db:
            with self.assertRaises(InvalidArchive):
                database_metadata(db, 'library', verification='off')


from test_receipts import ReceiptFixture, ORIGIN, PRODUCER
from stash_archive.receipts import verify_snapshot_producers


class ProducerBindingTests(ReceiptFixture):
    def test_identity_report_does_not_claim_historical_receipts_are_valid(self):
        self.accepted(self.event())
        self.db.execute("DELETE FROM ingest_receipts")
        self.db.commit()
        report = verify_snapshot_producers(self.library, [self.outbox], ORIGIN)
        self.assertFalse(report["receipts_audited"])
        self.assertTrue(report["registered_producers_complete"])
        self.assertEqual(report["producers"], [{"producer_uuid": PRODUCER}])
        with self.assertRaisesRegex(InvalidArchive, "missing from the native snapshot"):
            self.verify()

    def test_identity_report_requires_all_producers_exactly_once(self):
        for paths in ([], [self.outbox, self.outbox]):
            with self.subTest(paths=paths), self.assertRaises(InvalidArchive):
                verify_snapshot_producers(self.library, paths, ORIGIN)
        with self.assertRaisesRegex(InvalidArchive, "another origin"):
            verify_snapshot_producers(self.library, [self.outbox], "https://foreign.example")

    def test_unknown_outbox_schema_is_not_accepted_as_a_binding(self):
        with closing(sqlite3.connect(self.outbox)) as db:
            db.execute("PRAGMA user_version=999")
        with self.assertRaisesRegex(InvalidArchive, "Unsupported producer"):
            verify_snapshot_producers(self.library, [self.outbox], ORIGIN)

    def test_uncoordinated_capture_still_reconciles_receipts(self):
        from stash_archive.bundle import export_archive
        self.accepted(self.event())
        self.db.execute("DELETE FROM ingest_receipts")
        self.db.commit()
        with self.assertRaisesRegex(InvalidArchive, "missing from the native snapshot"):
            export_archive(self.library, self.root / "uncoordinated", reserve=0,
                           verification="capture", producer_origin=ORIGIN,
                           components=[{"role": "producer_outbox", "name": "queue.sqlite", "path": self.outbox}])
