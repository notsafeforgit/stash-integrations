from contextlib import closing
import sqlite3
import tempfile
from pathlib import Path
import unittest

from stash_ingest.encoding import InvalidData, encode
from stash_ingest.outbox import Outbox, SCHEMA
from helpers import PRODUCER, capture


def schema_twenty_five(path):
    """Recreate the discovery journal and dispatch tables that schema 25 retained."""
    with closing(Outbox(path, "http://fixture.invalid", PRODUCER, clock=lambda: 1000)) as box:
        box.enqueue(encode(capture()))
        db = box.db
        for table in ("discovery_executions", "discovery_detail_executions"):
            db.execute(f"""CREATE TABLE {table}(job_uuid TEXT PRIMARY KEY, phase TEXT NOT NULL,
                body BLOB, body_sha256 TEXT, reserved_bytes INTEGER NOT NULL DEFAULT 0)""")
        db.execute("""CREATE TABLE discovery_dispatch(collection_uuid TEXT NOT NULL, policy_sha256 TEXT NOT NULL,
            PRIMARY KEY(collection_uuid,policy_sha256))""")
        db.execute("CREATE TABLE discovery_collection_dispatch(policy_sha256 TEXT PRIMARY KEY NOT NULL)")
        db.execute("""CREATE TABLE discovery_detail_dispatch(collection_uuid TEXT NOT NULL, policy_sha256 TEXT NOT NULL,
            PRIMARY KEY(collection_uuid,policy_sha256))""")
        db.execute("CREATE TABLE discovery_detail_collection_dispatch(policy_sha256 TEXT PRIMARY KEY NOT NULL)")
        db.execute("ALTER TABLE worker_dispatch ADD COLUMN discovery_delivery_after TEXT NOT NULL DEFAULT ''")
        db.execute("ALTER TABLE worker_dispatch ADD COLUMN discovery_detail_delivery_after TEXT NOT NULL DEFAULT ''")
        db.execute("INSERT INTO worker_dispatch(uuid,enrichment_delivery_after,discovery_delivery_after) VALUES(?,?,?)",
                   ("00000000-0000-4000-8000-000000000001", "kept-cursor", "retired-cursor"))
        db.execute("PRAGMA user_version=25")


class DiscoveryRetirementTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "outbox.sqlite"
        schema_twenty_five(self.path)
        self.db = sqlite3.connect(self.path)
        self.addCleanup(self.db.close)

    def open(self):
        return Outbox(self.path, "http://fixture.invalid", PRODUCER, clock=lambda: 1000)

    def tables(self):
        return {row[0] for row in self.db.execute("SELECT name FROM sqlite_schema WHERE type='table'")}

    def test_finished_discovery_rows_without_bytes_are_dropped_and_queue_survives(self):
        self.db.execute("INSERT INTO discovery_executions(job_uuid,phase) VALUES('a','completed'),('b','failed')")
        self.db.execute("INSERT INTO discovery_detail_executions(job_uuid,phase) VALUES('c','completed')")
        self.db.commit()
        with closing(self.open()) as box:
            self.assertEqual(box.db.execute("PRAGMA user_version").fetchone()[0], SCHEMA)
            self.assertEqual(box.status()["counts"]["pending"], 1)
            self.assertEqual(box.metadata_reserved_bytes(), 0)
            columns = {row["name"] for row in box.db.execute("PRAGMA table_info(worker_dispatch)")}
            self.assertEqual(columns, {"uuid", "after_entry", "enrichment_delivery_after", "revision"})
            self.assertEqual(box.db.execute("SELECT enrichment_delivery_after FROM worker_dispatch").fetchone()[0], "kept-cursor")
        for table in ("discovery_executions", "discovery_detail_executions", "discovery_dispatch",
                      "discovery_collection_dispatch", "discovery_detail_dispatch", "discovery_detail_collection_dispatch"):
            self.assertNotIn(table, self.tables())

    def test_unacknowledged_discovery_work_refuses_upgrade_and_leaves_outbox_unchanged(self):
        cases = {
            "active lease": "INSERT INTO discovery_executions(job_uuid,phase) VALUES('a','active')",
            "review": "INSERT INTO discovery_executions(job_uuid,phase) VALUES('a','review')",
            "held body": "INSERT INTO discovery_executions(job_uuid,phase,body) VALUES('a','completed',X'7B7D')",
            "reserved bytes": "INSERT INTO discovery_detail_executions(job_uuid,phase,reserved_bytes) VALUES('a','failed',9)",
        }
        for name, statement in cases.items():
            with self.subTest(name=name):
                self.db.execute(statement)
                self.db.commit()
                before = self.db.execute("PRAGMA user_version").fetchone()[0]
                with self.assertRaisesRegex(InvalidData, "Discovery retirement refused"):
                    self.open()
                self.assertEqual(self.db.execute("PRAGMA user_version").fetchone()[0], before)
                self.assertIn("discovery_executions", self.tables())
                self.assertIn("discovery_detail_executions", self.tables())
                self.assertIn("discovery_dispatch", self.tables())
                self.assertEqual(self.db.execute("SELECT count(*) FROM worker_dispatch WHERE discovery_delivery_after='retired-cursor'").fetchone()[0], 1)
                self.db.execute("DELETE FROM discovery_executions")
                self.db.execute("DELETE FROM discovery_detail_executions")
                self.db.commit()


if __name__ == "__main__":
    unittest.main()
