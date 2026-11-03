from stash_archive.bundle import snapshot_database
from stash_archive.storage import InvalidArchive
from test_receipts import ReceiptFixture


class CompactReceiptTests(ReceiptFixture):
    def test_compact_snapshot_restores_with_native_proof_and_exact_pending_payload(self):
        self.accepted(self.event())
        self.accepted(self.event(), acknowledge=False)
        before = self.verify()['producers'][0]['counts']
        self.assertEqual(self.compact_acknowledgements()['compacted_events'], 1)
        native, queue = self.root / 'native-copy.sqlite', self.root / 'outbox-copy.sqlite'
        snapshot_database(self.library, native, "library", reserve=0)
        snapshot_database(self.outbox, queue, "producer_outbox", reserve=0)
        self.assertEqual(self.verify(library=native, outbox=queue)['producers'][0]['counts'], before)

    def test_compact_acknowledgement_rejects_changed_native_result(self):
        self.accepted(self.event())
        self.compact_acknowledgements()
        self.verify()
        self.db.execute("UPDATE ingest_receipts SET result='{}'")
        self.db.commit()
        with self.assertRaisesRegex(InvalidArchive, 'Compact acknowledgement differs'):
            self.verify()

    def test_compact_acknowledgement_cannot_be_downgraded_or_corrupted(self):
        self.accepted(self.event())
        self.compact_acknowledgements()
        self.box.db.execute('PRAGMA user_version=24')
        with self.assertRaisesRegex(InvalidArchive, 'Compact acknowledgement differs'):
            self.verify()
        self.box.db.execute('PRAGMA user_version=25')
        value = self.box.db.execute('SELECT receipt FROM events').fetchone()[0]
        self.box.db.execute('UPDATE events SET receipt=?', (value[:-1] + bytes([value[-1] ^ 1]),))
        with self.assertRaisesRegex(InvalidArchive, 'Compact acknowledgement differs'):
            self.verify()
