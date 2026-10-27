import hashlib
import json
import uuid

from stash_ingest.encoding import encode
from stash_ingest.retention import POLICY
from stash_archive.storage import InvalidArchive
from test_receipts import ReceiptFixture, PRODUCER, CREDENTIAL


class ProfileReceiptTests(ReceiptFixture):
    receipt_migration = '1000134_account_profiles.up.sql'

    def setUp(self):
        super().setUp()
        self.db.execute('''CREATE TABLE source_account_profiles(uuid TEXT, producer_uuid TEXT, event_uuid TEXT,
            collection_uuid TEXT, collection_revision INTEGER, account_uuid TEXT, observed_at TEXT,
            extractor_version TEXT, source TEXT, review TEXT)''')
        self.db.commit()

    def profile(self, *, accepted=True, acknowledged=True):
        event = {k: v for k, v in self.event().items() if k not in ('relative_path', 'size', 'sha256', 'media_kind')}
        event.update(kind='account.profile', owner_uuid=str(uuid.uuid4()), fence=1,
                     extractor_version='fixture', retention_policy=POLICY,
                     source={'category': 'reddit', 'user': {'id': '123', 'name': 'Example',
                             'social_links': [{'url': 'https://onlyfans.com/example'}]}})
        self.box.enqueue(encode(event))
        if not accepted:
            return event
        delivery, = self.box.claim(str(uuid.uuid4()))
        receipt = {k: event[k] for k in ('producer_uuid', 'event_uuid', 'collection_uuid', 'collection_revision', 'root_uuid', 'run_uuid', 'kind')}
        receipt.update(sha256=hashlib.sha256(encode(event)).hexdigest(), credential_uuid=CREDENTIAL,
                       result={'status': 'recorded', 'profile_uuid': str(uuid.uuid4()), 'account_uuid': str(uuid.uuid4()), 'media_ingested': False},
                       committed_at='2026-10-04T12:00:00+00:00')
        self.db.execute('''INSERT INTO ingest_receipts(producer_uuid,event_uuid,digest,credential_uuid,
            collection_uuid,collection_revision,root_uuid,run_uuid,kind,result,committed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)''',
            tuple(receipt[k] for k in ('producer_uuid', 'event_uuid', 'sha256', 'credential_uuid', 'collection_uuid',
                                      'collection_revision', 'root_uuid', 'run_uuid', 'kind')) +
            (json.dumps(receipt['result']), '2026-10-04 12:00:00'))
        self.db.execute('INSERT INTO source_account_profiles VALUES(?,?,?,?,?,?,?,?,?,?)',
                        (receipt['result']['profile_uuid'], PRODUCER, event['event_uuid'], event['collection_uuid'],
                         event['collection_revision'], receipt['result']['account_uuid'], event['observed_at'],
                         event['extractor_version'], json.dumps(event['source']), ''))
        self.db.commit()
        if acknowledged:
            self.box.acknowledge(delivery, receipt)
        else:
            self.box.fail(delivery, 'network_unavailable')
        return event

    def test_pending_and_acknowledged_profiles_preserve_original_evidence(self):
        first = self.profile()
        self.profile(acknowledged=False)
        self.profile(accepted=False)
        counts = self.verify()['producers'][0]['counts']
        self.assertEqual((counts['acknowledged'], counts['pending'], counts['accepted_unacknowledged']), (1, 2, 1))
        self.db.execute('DELETE FROM source_account_profiles WHERE event_uuid=?', (first['event_uuid'],))
        self.db.commit()
        with self.assertRaisesRegex(InvalidArchive, 'missing its observation'):
            self.verify()

    def test_lost_reply_cannot_discard_changed_native_profile(self):
        self.profile(acknowledged=False)
        self.db.execute("UPDATE source_account_profiles SET source='{}'")
        self.db.commit()
        with self.assertRaisesRegex(InvalidArchive, 'differs from native history'):
            self.verify()
