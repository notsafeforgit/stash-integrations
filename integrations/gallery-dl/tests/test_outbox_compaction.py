import json
from unittest.mock import Mock, patch
import uuid

from helpers import PRODUCER, RUN, capture, file_event
from test_outbox_history import HistoryFixture
from test_download_outbox import download, download_receipt
from stash_ingest.client import Unavailable, drain_once
from stash_ingest.encoding import InvalidData, encode
from stash_ingest.outbox import Conflict, Outbox
from stash_ingest.outbox_compaction import compact
from stash_ingest.outbox_history import apply
from stash_ingest.receipt_proofs import SIZE, is_compact


class AcknowledgementCompactionTests(HistoryFixture):
    def compact(self, **kwargs):
        self.native.execute('PRAGMA query_only=ON')
        self.native.execute('BEGIN')
        try:
            return compact(self.box, self.native, now=300000, apply=True, **kwargs)
        finally:
            self.native.rollback()
            self.native.execute('PRAGMA query_only=OFF')

    def test_unfinished_crawl_keeps_duplicates_dependencies_and_pending_payloads(self):
        self.native.execute("UPDATE source_runs SET state='queued'")
        self.native.commit()
        source = capture()
        self.accept(source)
        child = file_event(source)
        self.box.enqueue(encode(child))
        before = [dict(row) for row in self.box.db.execute('SELECT * FROM events ORDER BY seq')]
        result = self.compact(batch_size=1)
        self.assertEqual(result['compacted_events'], 1)
        after = [dict(row) for row in self.box.db.execute('SELECT * FROM events ORDER BY seq')]
        self.assertEqual(len(after[0]['receipt']), SIZE)
        self.assertTrue(is_compact(after[0]['receipt']))
        before[0]['receipt'] = after[0]['receipt']
        self.assertEqual(before, after)
        self.assertEqual(self.plan()['runs'], [])
        self.box.receipt_lookup = Mock(side_effect=AssertionError('enqueue must stay local'))
        self.box.enqueue(encode(source))
        self.box.receipt_lookup.assert_not_called()
        delivery, = self.box.claim(str(uuid.uuid4()))
        self.assertEqual(delivery.body, encode(child))
        self.assertEqual(self.box.db.execute('PRAGMA foreign_key_check').fetchall(), [])

    def test_old_receipt_lookup_validates_native_and_caches_only_bounded_memory(self):
        source = capture()
        saved = self.accept(source)
        self.compact()
        lookup = Mock(side_effect=self.lookup)
        self.box.receipt_lookup = lookup
        first = self.box.receipt(source['event_uuid'])
        self.assertEqual(first['capture_uuid'], saved['capture_uuid'])
        first['result']['status'] = 'mutated by caller'
        self.assertEqual(self.box.receipt(source['event_uuid'])['result'], saved['result'])
        self.assertEqual(lookup.call_count, 1)
        self.assertEqual(self.box.db.execute('SELECT length(receipt) FROM events').fetchone()[0], SIZE)
        with self.box.transaction(), self.assertRaisesRegex(Conflict, 'transaction'):
            self.box.receipt(source['event_uuid'])

    def test_new_report_acknowledges_compact_parent_outside_transaction_using_drainer_client(self):
        source = capture()
        saved = self.accept(source)
        self.compact()
        event = download(source)
        self.box.enqueue(encode(event))
        client = Mock(endpoint=self.box.endpoint, producer=self.box.producer)
        client.capabilities.return_value = {'file_ingestion': False, 'attachment_download_protocol': 1}
        client.batch.return_value = [{'status': 200, 'receipt': download_receipt(event, saved)}]
        client.receipt.side_effect = self.lookup
        self.box.receipt_lookup = Mock(side_effect=AssertionError('must use the actual drainer client'))
        self.assertEqual(drain_once(self.box, client)['acknowledged'], 1)
        client.receipt.assert_called_once_with(source['event_uuid'])
        self.assertEqual(self.box.status()['counts']['review'], 0)

    def test_compact_parent_lookup_outage_retries_original_report_without_review(self):
        source = capture()
        saved = self.accept(source)
        self.compact()
        event = download(source)
        self.box.enqueue(encode(event))
        client = Mock(endpoint=self.box.endpoint, producer=self.box.producer)
        client.capabilities.return_value = {'file_ingestion': False, 'attachment_download_protocol': 1}
        client.batch.return_value = [{'status': 200, 'receipt': download_receipt(event, saved)}]
        client.receipt.side_effect = Unavailable('network_unreachable', 503)
        result = drain_once(self.box, client)
        self.assertEqual((result['retried'], result['review']), (1, 0))
        self.assertEqual(self.box.db.execute('SELECT body FROM events WHERE event_uuid=?',
                                            (event['event_uuid'],)).fetchone()[0], encode(event))
        self.now[0] += 1000
        client.receipt.side_effect = self.lookup
        self.assertEqual(drain_once(self.box, client)['acknowledged'], 1)

    def test_expired_delivery_during_parent_lookup_keeps_payload_and_fence(self):
        source = capture()
        saved = self.accept(source)
        self.compact()
        event = download(source)
        self.box.enqueue(encode(event))
        client = Mock(endpoint=self.box.endpoint, producer=self.box.producer)
        client.capabilities.return_value = {'file_ingestion': False, 'attachment_download_protocol': 1}
        client.batch.return_value = [{'status': 200, 'receipt': download_receipt(event, saved)}]
        def expired(event_uuid):
            self.now[0] += 121
            return self.lookup(event_uuid)
        client.receipt.side_effect = expired
        result = drain_once(self.box, client)
        self.assertEqual((result['lease_lost'], result['acknowledged'], result['review']), (1, 0, 0))
        delivery, = self.box.claim(str(uuid.uuid4()))
        self.assertEqual(delivery.body, encode(event))

    def test_recent_acknowledgements_remain_full_and_repeated_pass_is_noop(self):
        old = capture()
        self.accept(old)
        self.now[0] = 290000
        recent = capture()
        self.accept(recent)
        self.assertEqual(self.compact()['compacted_events'], 1)
        self.assertEqual(self.compact()['eligible_events'], 0)
        self.assertGreater(self.box.db.execute('SELECT length(receipt) FROM events WHERE event_uuid=?',
                                             (recent['event_uuid'],)).fetchone()[0], SIZE)

    def test_completed_run_retirement_has_same_proof_after_receipt_compaction(self):
        source = capture()
        self.accept(source)
        self.accept(file_event(source))
        full_plan = self.plan()
        self.assertEqual(self.compact()['compacted_events'], 2)
        compact_plan = self.plan()
        self.assertEqual(full_plan['runs'][0]['receipts_sha256'], compact_plan['runs'][0]['receipts_sha256'])
        self.assertEqual(apply(self.box, compact_plan, batch_size=1), 2)
        self.assertEqual(self.box.db.execute('SELECT count(*) FROM events').fetchone()[0], 0)

    def test_missing_native_receipt_aborts_batch_and_partial_pass_can_resume(self):
        first, second = capture(), capture()
        self.accept(first)
        self.accept(second)
        removed = tuple(self.native.execute('SELECT * FROM ingest_receipts WHERE event_uuid=?',
                                           (second['event_uuid'],)).fetchone())
        self.native.execute('DELETE FROM ingest_receipts WHERE event_uuid=?', (second['event_uuid'],))
        self.native.commit()
        with self.assertRaisesRegex(InvalidData, 'missing its native receipt'):
            self.compact()
        self.assertTrue(all(len(row[0]) > SIZE for row in self.box.db.execute('SELECT receipt FROM events')))
        with self.assertRaisesRegex(InvalidData, 'missing its native receipt'):
            self.compact(batch_size=1)
        self.assertEqual(self.box.db.execute('SELECT count(*) FROM events WHERE length(receipt)=?', (SIZE,)).fetchone()[0], 1)
        self.native.execute('INSERT INTO ingest_receipts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)', removed)
        self.native.commit()
        self.assertEqual(self.compact()['compacted_events'], 1)

    def test_changed_native_receipt_prevents_compaction_and_later_remote_lookup(self):
        source = capture()
        self.accept(source)
        original = self.native.execute('SELECT result FROM ingest_receipts').fetchone()[0]
        self.native.execute("UPDATE ingest_receipts SET result='{}'")
        self.native.commit()
        with self.assertRaisesRegex(InvalidData, 'differs from its native receipt'):
            self.compact()
        self.native.execute('UPDATE ingest_receipts SET result=?', (original,))
        self.native.commit()
        self.compact()
        saved = json.loads(json.dumps(self.receipts[source['event_uuid']]))
        saved['result'] = {}
        self.box.receipt_lookup = lambda _: saved
        with self.assertRaisesRegex(InvalidData, 'Compact acknowledgement differs'):
            self.box.receipt(source['event_uuid'])

    def test_migration_keeps_full_receipts_and_older_binary_refuses_compact_schema(self):
        self.accept(capture())
        before = self.box.db.execute('SELECT receipt FROM events').fetchone()[0]
        self.box.db.execute('PRAGMA user_version=24')
        self.box.close()
        self.box = Outbox(self.path, 'http://localhost:8009', PRODUCER)
        self.assertEqual(self.box.db.execute('SELECT receipt FROM events').fetchone()[0], before)
        self.assertEqual(self.box.db.execute('PRAGMA user_version').fetchone()[0], 25)
        with patch('stash_ingest.outbox.SCHEMA', 24), self.assertRaisesRegex(InvalidData, 'Unsupported'):
            Outbox(self.path, 'http://localhost:8009', PRODUCER, create=False)
