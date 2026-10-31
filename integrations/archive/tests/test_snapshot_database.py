"""Exercise live SQLite writes between actual incremental backup steps."""

from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from stash_archive.bundle import snapshot_database
from stash_archive.storage import InvalidArchive


class SnapshotDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "live queue.sqlite"
        self.target = self.root / "snapshot.sqlite"
        self.writer = sqlite3.connect(self.source)
        self.addCleanup(self.writer.close)
        self.writer.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA wal_autocheckpoint=0;
            CREATE TABLE records(id INTEGER PRIMARY KEY, revision INTEGER, body BLOB);
        """)
        self.writer.executemany("INSERT INTO records VALUES(?, 0, zeroblob(4096))",
                                [(i,) for i in range(1024)])
        self.writer.commit()
        self.pages = self.writer.execute("PRAGMA page_count").fetchone()[0]
        self.steps = []

    def instrument(self, callback):
        connect = sqlite3.connect
        steps = self.steps

        class ObservedConnection(sqlite3.Connection):
            def backup(self, target, *, progress, **kwargs):
                def observe(status, remaining, total):
                    steps.append((status, remaining, total))
                    if len(steps) > 2 + (total + 255) // 256:
                        raise AssertionError("Snapshot repeatedly copied the same database pages")
                    callback(self, status, remaining, total)
                    progress(status, remaining, total)
                return super().backup(target, progress=observe, **kwargs)

        def observed_connect(*args, **kwargs):
            if kwargs.get("uri"):
                kwargs["factory"] = ObservedConnection
            return connect(*args, **kwargs)

        return patch("stash_archive.bundle.sqlite3.connect", side_effect=observed_connect)

    def write_during_copy(self, incoming, status, remaining, total):
        self.writer.execute("UPDATE records SET revision=revision+1 WHERE id=0")
        self.writer.commit()

    def test_wal_writers_commit_while_snapshot_copies_each_page_once(self):
        with self.instrument(self.write_during_copy):
            snapshot_database(self.source, self.target, "operating_database", 0)
        self.assertGreater(len(self.steps), 1)
        self.assertEqual(len(self.steps), (self.pages + 255) // 256)
        self.assertEqual([s[2] for s in self.steps], [self.pages] * len(self.steps))
        self.assertEqual([s[1] for s in self.steps],
                         [max(0, self.pages - 256 * i) for i in range(1, len(self.steps) + 1)])
        self.assertEqual(self.writer.execute("SELECT revision FROM records WHERE id=0").fetchone(),
                         (len(self.steps),))
        with closing(sqlite3.connect(self.target)) as copied:
            self.assertEqual(copied.execute("PRAGMA integrity_check").fetchone(), ("ok",))
            self.assertEqual(copied.execute("SELECT revision FROM records WHERE id=0").fetchone(), (0,))
            self.assertEqual(copied.execute("SELECT count(*), sum(length(body)) FROM records").fetchone(),
                             (1024, 1024 * 4096))
        self.assertEqual(self.writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone(), (0, 0, 0))

    def test_interrupted_copy_releases_source_snapshot(self):
        def interrupt(incoming, status, remaining, total):
            self.write_during_copy(incoming, status, remaining, total)
            raise InvalidArchive("injected capture interruption")

        with self.instrument(interrupt), self.assertRaisesRegex(InvalidArchive, "injected"):
            snapshot_database(self.source, self.target, "operating_database", 0)
        self.assertEqual(self.writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone(), (0, 0, 0))

    def test_lost_read_snapshot_aborts_on_first_repeated_backup_step(self):
        def lose_snapshot(incoming, status, remaining, total):
            incoming.rollback()
            self.write_during_copy(incoming, status, remaining, total)

        with self.instrument(lose_snapshot), self.assertRaisesRegex(InvalidArchive, "restarted"):
            snapshot_database(self.source, self.target, "operating_database", 0)
        self.assertEqual(len(self.steps), 2)
        self.assertEqual(self.writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone(), (0, 0, 0))


if __name__ == "__main__":
    unittest.main()
