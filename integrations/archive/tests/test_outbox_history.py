import sqlite3
import uuid

from test_receipts import ReceiptFixture, RUN, ORIGIN
from stash_archive.storage import InvalidArchive
from stash_ingest.outbox_history import apply, inspect


class RetiredHistoryTests(ReceiptFixture):
    def setUp(self):
        super().setUp()
        self.db.execute('CREATE TABLE source_runs(uuid TEXT PRIMARY KEY,state TEXT,revision INTEGER,updated_at_ms INTEGER)')
        self.db.execute("INSERT INTO source_runs VALUES(?,'succeeded',2,100000000)", (RUN,))
        self.db.commit()
        self.box.clock = lambda: 100000

    def retire(self):
        self.db.row_factory = sqlite3.Row
        try:
            plan = inspect(self.box.db, self.db, ORIGIN, now=300000)
            self.assertEqual(len(plan['runs']), 1)
            apply(self.box, plan)
        finally:
            self.db.row_factory = None

    def test_native_history_verifies_after_local_rows_are_removed(self):
        self.accepted(self.event())
        self.retire()
        report = self.verify()
        self.assertEqual(report['producers'][0]['retired_source_runs'],
                         {'runs': 1, 'events': 1, 'receipts_verified': True})

    def test_missing_or_changed_native_receipt_fails_retirement_proof(self):
        self.accepted(self.event())
        self.retire()
        self.db.execute("UPDATE ingest_receipts SET result='{}'")
        self.db.commit()
        with self.assertRaisesRegex(InvalidArchive, 'Retired producer receipts'):
            self.verify()

    def test_retired_run_reopened_in_library_is_rejected(self):
        self.accepted(self.event())
        self.retire()
        self.db.execute("UPDATE source_runs SET state='running'")
        self.db.commit()
        with self.assertRaisesRegex(InvalidArchive, 'terminal source run'):
            self.verify()

    def test_sequence_counter_cannot_move_back_after_pruning(self):
        self.accepted(self.event())
        self.retire()
        self.box.db.execute('UPDATE outbox_history_state SET last_sequence=0')
        with self.assertRaisesRegex(InvalidArchive, 'high-water mark'):
            self.verify()
