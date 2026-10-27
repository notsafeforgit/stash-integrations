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
