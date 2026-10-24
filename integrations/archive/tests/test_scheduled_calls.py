from contextlib import closing
import uuid

from stash_ingest.encoding import encode
from stash_ingest.outbox import Outbox
from stash_ingest.scheduled_calls import ScheduledCalls
from stash_archive.bundle import export_archive, import_archive
from stash_archive.storage import InvalidArchive
from test_receipts import ReceiptFixture, ORIGIN, PRODUCER, MEDIA_ROOT


class ScheduledArchiveTests(ReceiptFixture):
    def record(self):
        self.call = str(uuid.uuid4())
        self.calls = ScheduledCalls(self.box)
        return self.calls.record(self.call, 'b'*64, lambda: {'service': 'reddit', 'mode': 'new', 'source': {
            'root_uuid': MEDIA_ROOT, 'operation': 'download', 'policy_sha256': 'a'*64, 'cooldown_seconds': 0,
            'window': {'since': None, 'until': '2026-10-24T07:00:00.000Z'}}})

    def test_pending_and_leased_schedules_export_restore_and_resume(self):
        self.record()
        delivery = self.calls.claim()
        expected = self.verify()['producers'][0]['scheduled_calls']
        archive, restored = self.root/'archive', self.root/'restored'
        export_archive(self.library, archive, components=[{'role':'producer_outbox','name':'worker.sqlite','path':self.outbox}], reserve=0)
        import_archive(archive, restored, reserve=0)
        path = restored/'components/producer_outbox/worker.sqlite'
        self.assertEqual(self.verify(library=restored/'library.sqlite', outbox=path)['producers'][0]['scheduled_calls'], expected)
        with closing(Outbox(path, ORIGIN, PRODUCER, clock=lambda: self.box.clock()+901)) as box:
            resumed = ScheduledCalls(box).claim()
            self.assertEqual(resumed['uuid'], delivery['uuid'])
            self.assertEqual(resumed['definition'], delivery['definition'])
            self.assertGreater(resumed['fence'], delivery['fence'])

    def test_recorded_intent_must_keep_its_original_atomic_handoff(self):
        self.record()
        target = 'https://www.reddit.com/user/example/submitted/?sort=new'
        self.calls.finish(self.calls.claim(), [target], {target: {'collection_uuid': str(uuid.uuid4()), 'revision': 1}})
        self.assertEqual(self.verify()['producers'][0]['scheduled_calls']['counts']['recorded'], 1)
        self.box.db.execute('DELETE FROM source_call_targets')
        with self.assertRaisesRegex(InvalidArchive, 'frozen targets'):
            self.verify()
        self.box.db.execute('DELETE FROM source_calls')
        with self.assertRaisesRegex(InvalidArchive, 'handoff'):
            self.verify()

    def test_schema_nineteen_remains_valid_and_cannot_omit_new_pending_requests(self):
        self.record()
        self.box.db.execute('PRAGMA user_version=19')
        with self.assertRaisesRegex(InvalidArchive, 'scheduling intents'):
            self.verify()
        self.box.db.execute('DROP TABLE scheduled_calls')
        self.assertNotIn('scheduled_calls', self.verify()['producers'][0])
        self.box.db.execute('PRAGMA user_version=20')
        with self.assertRaisesRegex(InvalidArchive, 'scheduling intents'):
            self.verify()

    def test_definition_damage_is_rejected(self):
        self.record()
        self.box.db.execute('DROP TRIGGER scheduled_call_immutable')
        self.box.db.execute("UPDATE scheduled_calls SET definition=?", (encode({'changed': True}),))
        with self.assertRaises(InvalidArchive):
            self.verify()
