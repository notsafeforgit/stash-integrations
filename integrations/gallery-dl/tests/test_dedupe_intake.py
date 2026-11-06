from copy import deepcopy
from pathlib import Path
import sqlite3
import tempfile
import unittest
import uuid

from stash_ingest.client import Unavailable
from stash_ingest.dedupe import Journal, apply_saved, private_directory
from stash_ingest.dedupe_client import MEDIA_POLICY
from stash_ingest.dedupe_intake import Registration, Review, equal_files
from stash_ingest.dedupe_candidates import directory_identity
from test_dedupe import MemoryClient
from test_intake_folder import Server


class RegistrationTests(unittest.TestCase):
    def fixture(self, directory):
        root = Path(directory) / 'media'
        root.mkdir()
        for name in ('a.jpg', 'b.jpg'):
            (root / name).write_bytes(b'equal test bytes')
        state, _ = private_directory(Path(directory) / 'state')
        config = {'endpoint': 'http://127.0.0.1:8009', 'root': directory_identity(root),
                  'root_uuid': str(uuid.uuid4()), 'media_policy': MEDIA_POLICY,
                  'intake_collection_uuid': str(uuid.uuid4())}
        journal = Journal.create(state, config, [{'keep_path': 'a.jpg', 'remove_path': 'b.jpg'}])
        server = Server(config)
        for name in ('a.jpg', 'b.jpg'):
            server.add(name)
        original_intake_preview = server.preview
        def intake_preview(*args):
            return {**original_intake_preview(*args), 'saved_post_count': 1}
        server.preview = intake_preview
        registration = Registration(journal, server, root)
        client = MemoryClient(root)
        client.journal = journal
        original = client.preview
        def preview(pair):
            value = original(pair)
            if any(pair[key] not in server.registered for key in ('keep_path', 'remove_path')):
                value.update(eligible=False, blocked_reason='file_requires_intake')
            return value
        client.preview = preview
        return root, journal, server, registration, client

    def test_admission_lost_response_recovers_same_uuid_and_waits_before_removal(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, journal, server, registration, client = self.fixture(tmp)
            server.lose_response = True
            with self.assertRaises(Unavailable):
                apply_saved(client, journal, intake=registration)
            self.assertEqual(len(server.jobs), 1)
            self.assertTrue((root / 'b.jpg').exists())
            request_id = next(iter(server.jobs))
            resumed = Journal(journal.directory, journal.config)
            client.journal = resumed
            registration = Registration(resumed, server, root)
            result = apply_saved(client, resumed, intake=registration)
            self.assertFalse(result['finished'])
            self.assertTrue(result['waiting_for_intake'])
            self.assertEqual(len(server.applies), 1)
            server.finish(request_id)
            result = apply_saved(client, resumed, intake=Registration(resumed, server, root))
            self.assertFalse(result['finished'])
            self.assertEqual(len(server.jobs), 2)
            for request_id in server.jobs:
                if server.jobs[request_id]['state'] == 'queued':
                    server.finish(request_id)
            result = apply_saved(client, resumed, intake=Registration(resumed, server, root))
            self.assertTrue(result['finished'])
            self.assertEqual(result['committed'], 1)
            self.assertEqual(len(server.applies), 2)
            self.assertTrue(all(r['deduplication_policy'] == MEDIA_POLICY for r in server.applies))
            self.assertFalse((root / 'b.jpg').exists())

    def test_false_candidate_never_admits_or_removes_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, journal, server, registration, client = self.fixture(tmp)
            (root / 'b.jpg').write_bytes(b'other test bytes')
            result = apply_saved(client, journal, intake=registration)
            self.assertEqual(result['review_reasons'], {'different_or_empty_files': 1})
            self.assertFalse(server.applies)
            self.assertTrue((root / 'b.jpg').exists())

    def test_unregistered_copy_without_saved_metadata_stays_in_review(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, journal, server, registration, client = self.fixture(tmp)
            original = server.preview
            server.preview = lambda *args: {**original(*args), 'saved_post_count': 0}
            result = apply_saved(client, journal, intake=registration)
            self.assertEqual(result['review_reasons'], {'saved_metadata_required': 1})
            self.assertFalse(server.applies)
            self.assertTrue((root / 'b.jpg').exists())

    def test_native_removal_fence_remains_review(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, journal, server, registration, client = self.fixture(tmp)
            server.reject = True
            result = apply_saved(client, journal, intake=registration)
            self.assertEqual(result['review_reasons'], {'intake_preview_changed': 1})
            self.assertEqual(result['committed'], 0)
            self.assertTrue((root / 'b.jpg').exists())

    def test_failed_native_intake_is_not_retried_or_reported_removed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, journal, server, registration, client = self.fixture(tmp)
            apply_saved(client, journal, intake=registration)
            server.finish(next(iter(server.jobs)), state='failed')
            result = apply_saved(client, journal, intake=Registration(journal, server, root))
            self.assertEqual(result['review_reasons'], {'intake_failed': 1})
            self.assertEqual(len(server.applies), 1)
            self.assertTrue((root / 'b.jpg').exists())

    def test_version_one_journal_migrates_without_losing_pending_request(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, journal, server, registration, client = self.fixture(tmp)
            key, pair = next(iter(journal.records.items()))
            # A v1 file-only request remains unchanged; enabling a new policy
            # in the host configuration cannot reinterpret its saved scope.
            with sqlite3.connect(journal.path) as db:
                db.execute('DROP TABLE registrations')
                db.execute('PRAGMA user_version=1')
            resumed = Journal(journal.directory, journal.config)
            self.assertEqual(resumed.run_uuid, journal.run_uuid)
            self.assertEqual(resumed.records[key], pair)
            with sqlite3.connect(journal.path) as db:
                self.assertEqual(db.execute('PRAGMA user_version').fetchone()[0], 2)


if __name__ == '__main__':
    unittest.main()
