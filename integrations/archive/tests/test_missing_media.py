from contextlib import closing
import sqlite3
import unittest

from stash_ingest import missing_media
from stash_ingest.outbox import Outbox
from stash_archive.bundle import export_archive, import_archive
from stash_archive.storage import InvalidArchive
from test_receipts import ReceiptFixture, ORIGIN, PRODUCER


class MissingMediaArchiveTests(unittest.TestCase):
    setUp = ReceiptFixture.setUp
    verify = ReceiptFixture.verify

    def remember(self):
        for item, attempts in [('Missing', 3), ('Partial', 2)]:
            for _ in range(attempts):
                missing_media.failed(self.box, {'service':'imgur','item':item}, 404)

    def test_export_restore_keeps_skip_and_partial_attempts_without_adjacent_file(self):
        self.remember()
        expected = self.verify()['producers'][0]['missing_media']
        self.assertEqual(expected['counts'], {'retrying':1, 'skipped':1})
        archive, restored = self.root/'archive', self.root/'restored'
        export_archive(self.library, archive, components=[{'role':'producer_outbox','name':'worker.sqlite','path':self.outbox}], reserve=0)
        import_archive(archive, restored, reserve=0)
        path = restored/'components/producer_outbox/worker.sqlite'
        actual = self.verify(library=restored/'library.sqlite', outbox=path)['producers'][0]['missing_media']
        self.assertEqual(actual, expected)
        with closing(Outbox(path, ORIGIN, PRODUCER)) as box:
            self.assertEqual(missing_media.cached(box, {'service':'imgur','item':'Missing'})['attempts'], 3)
            self.assertEqual(missing_media.failed(box, {'service':'imgur','item':'Partial'}, 410)['attempts'], 3)
        self.assertFalse(path.with_name(path.name+'.missing-media.sqlite').exists())

    def test_schema_eighteen_keeps_historical_report_and_future_twenty_one_is_rejected(self):
        current = self.verify()
        del current['producers'][0]['missing_media']
        del current['producers'][0]['scheduled_calls']
        self.box.db.execute('DROP TABLE scheduled_calls')
        self.box.db.execute('DROP TABLE missing_media')
        self.box.db.execute('PRAGMA user_version=18')
        self.assertEqual(self.verify(), current)
        self.box.db.execute('PRAGMA user_version=22')
        with self.assertRaisesRegex(InvalidArchive, 'Unsupported producer'):
            self.verify()

    def test_downgrade_or_missing_table_cannot_hide_decisions(self):
        self.remember()
        self.box.db.execute('PRAGMA user_version=18')
        with self.assertRaisesRegex(InvalidArchive, 'missing-media decisions'):
            self.verify()
        self.box.db.execute('PRAGMA user_version=19')
        self.box.db.execute('DROP TABLE missing_media')
        with self.assertRaisesRegex(InvalidArchive, 'missing-media decisions'):
            self.verify()

    def test_corrupt_decision_is_rejected_without_modifying_snapshot(self):
        self.remember()
        self.box.db.execute("UPDATE missing_media SET last_seen='invalid'")
        before = self.box.db.execute('SELECT * FROM missing_media').fetchall()
        with self.assertRaisesRegex(InvalidArchive, 'missing-media observation'):
            self.verify()
        self.assertEqual(self.box.db.execute('SELECT * FROM missing_media').fetchall(), before)
