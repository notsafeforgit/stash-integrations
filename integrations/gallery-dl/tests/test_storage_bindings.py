import copy
from contextlib import closing, redirect_stdout, redirect_stderr
import io
import json
import os
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from stash_ingest.configuration import Configuration, STORAGE_SCHEMA, STORAGE_LAYERED_SCHEMA
from stash_ingest.configuration_store import Configurations
from stash_ingest.config_migration import main as convert
from stash_ingest.encoding import InvalidData
from stash_ingest.outbox import Outbox
from stash_ingest.profile_dispatch import ProfileDispatcher
from stash_ingest.storage import StorageBindings, main, review, review_directory
from stash_ingest.storage_marker import NAME, body
from helpers import PRODUCER, ROOT
from test_configuration import profile_fixture
from test_layered_configuration import layered_fixture


class StorageBindingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.filename, self.inline = profile_fixture(self.directory)
        self.registry = self.directory / 'storage.json'
        self.value = review({ROOT: self.directory/'media'}, self.directory/'locks')
        self.write_storage()

    def write_storage(self):
        self.registry.write_text(json.dumps(self.value))

    def reference(self):
        return {'uuid': self.value['uuid'], 'path': str(self.registry)}

    def activate(self, *, layered=False):
        value = json.loads(self.filename.read_text())
        value['schema'] = STORAGE_LAYERED_SCHEMA if layered else STORAGE_SCHEMA
        value['root'] = {'uuid': ROOT}
        del value['locks']
        value['storage'] = self.reference()
        self.filename.write_text(json.dumps(value))
        return Configuration(self.filename)

    def open(self):
        return Outbox(self.directory/'outbox.sqlite', 'https://fixture.invalid', PRODUCER)

    def test_new_profiles_keep_storage_proofs_out_of_saved_policy_and_hash(self):
        previous = Configuration(self.filename)
        current = self.activate()
        self.assertEqual(current.policy_sha256, previous.policy_sha256)
        with closing(self.open()) as box:
            saved = Configurations(box)
            saved.remember(current)
            row = saved.rows(self.filename)[0]
            document = json.loads(row['document'])
            self.assertEqual(document['root'], {'uuid': ROOT})
            self.assertEqual(document['storage'], self.reference())
            self.assertNotIn('locks', document)
            self.assertNotIn('fs-v1:', row['document'].decode())
            self.assertNotIn('identity', document['root'])
            self.assertEqual(saved.load(row).policy_sha256, previous.policy_sha256)
        changed = copy.deepcopy(current.document)
        changed['gallery']['extractor']['filename'] = 'changed.{extension}'
        self.assertNotEqual(Configuration.from_document(changed, self.directory).policy_sha256, previous.policy_sha256)

    def test_old_jobs_use_current_binding_after_relocation_without_rewriting_receipts(self):
        previous = Configuration(self.filename)
        with closing(self.open()) as box:
            saved = Configurations(box)
            saved.remember(previous)
            row = saved.rows(self.filename)[0]
            before = tuple(row[key] for key in ('document', 'document_sha256', 'policy_sha256', 'runtime_sha256'))
        current = self.activate()
        moved = self.directory/'mounted-media'
        (self.directory/'media').rename(moved)
        self.value['roots'][ROOT] = review_directory(moved)
        self.write_storage()
        with closing(self.open()) as box:
            saved = Configurations(box)
            current = Configuration(self.filename)
            saved.remember(current)
            row = saved.rows(self.filename)[0]
            restored = saved.load(row, storage=current.storage)
            self.assertEqual(restored.root.path, moved)
            self.assertEqual(restored.policy_sha256, previous.policy_sha256)
            self.assertEqual(restored.document, previous.document)
            self.assertEqual(len(saved.rows(self.filename)), 1)
            self.assertEqual(tuple(row[key] for key in ('document', 'document_sha256', 'policy_sha256', 'runtime_sha256')), before)
            with patch('stash_ingest.profile_dispatch.Dispatcher') as dispatcher:
                dispatcher.return_value.once.return_value = {'state': 'yielded'}
                self.assertEqual(ProfileDispatcher(box, object(), self.filename).once(), {'state': 'yielded'})
                self.assertEqual(dispatcher.call_args.args[2].root.path, moved)

    def test_missing_or_wrong_registry_never_falls_back_to_retained_directory(self):
        with closing(self.open()) as box:
            Configurations(box).remember(Configuration(self.filename))
            self.activate()
            for damage in ('missing', 'wrong_uuid', 'wrong_mount'):
                with self.subTest(damage=damage):
                    bad = copy.deepcopy(self.value)
                    if damage == 'wrong_uuid':
                        bad['uuid'] = str(uuid.uuid4())
                    elif damage == 'wrong_mount':
                        other = self.directory/'other'
                        other.mkdir(exist_ok=True)
                        bad['roots'][ROOT]['token'] = review_directory(other)['token']
                    self.registry.write_text(json.dumps(bad))
                    if damage == 'missing':
                        self.registry.unlink()
                    with patch('stash_ingest.profile_dispatch.Dispatcher') as dispatcher:
                        result = ProfileDispatcher(box, object(), self.filename).once()
                        self.assertEqual(result, {'state': 'unavailable', 'error_code': 'worker_profile_unavailable'})
                        dispatcher.assert_not_called()
                    self.write_storage()

    def test_broken_new_layer_still_runs_retained_instructions_using_current_storage(self):
        self.filename, _, overlay, _ = layered_fixture(self.directory)
        with closing(self.open()) as box:
            old = Configuration(self.filename)
            Configurations(box).remember(old)
            new = self.activate(layered=True)
            self.assertEqual(new.policy_sha256, old.policy_sha256)
            overlay.unlink()
            with patch('stash_ingest.profile_dispatch.Dispatcher') as dispatcher:
                dispatcher.return_value.once.return_value = {'state': 'yielded'}
                self.assertEqual(ProfileDispatcher(box, object(), self.filename).once()['state'], 'yielded')
                used = dispatcher.call_args.args[2]
                self.assertEqual(used.storage.uuid, self.value['uuid'])
                self.assertEqual(used.document, old.document)
                self.assertEqual(used.policy_sha256, old.policy_sha256)

    def test_frozen_registry_reference_cannot_be_redirected_to_another_namespace(self):
        current = self.activate()
        other = copy.deepcopy(self.value)
        other['uuid'] = str(uuid.uuid4())
        with closing(self.open()) as box:
            saved = Configurations(box)
            saved.remember(current)
            with self.assertRaises(InvalidData):
                saved.load(saved.rows(self.filename)[0], storage=StorageBindings(other, self.registry))
        absent = copy.deepcopy(current.document)
        absent['root']['uuid'] = str(uuid.uuid4())
        with self.assertRaises(InvalidData):
            Configuration.from_document(absent, self.directory)

    def test_storage_is_independent_of_device_number_but_rejects_directory_replacement(self):
        original = os.fstat
        def renumber(fd):
            info = original(fd)
            if stat.S_ISDIR(info.st_mode):
                return SimpleNamespace(st_mode=info.st_mode, st_ino=info.st_ino, st_dev=info.st_dev+999)
            return info
        with patch('stash_ingest.filesystem.os.fstat', side_effect=renumber), \
                patch('os.fstatvfs', side_effect=AssertionError('Mount IDs are not durable storage identity')):
            self.activate().check()
        (self.directory/'media').rename(self.directory/'old')
        (self.directory/'media').mkdir()
        with self.assertRaises(InvalidData):
            Configuration(self.filename)

    def test_missing_changed_or_unsafe_marker_pauses_work_without_repairing_it(self):
        self.activate()
        marker = self.directory / 'media' / NAME
        original = marker.read_bytes()
        for damage in ('missing', 'different', 'truncated', 'symlink', 'fifo'):
            with self.subTest(damage=damage):
                marker.unlink()
                if damage == 'different':
                    marker.write_bytes(body(str(uuid.uuid4())))
                elif damage == 'truncated':
                    marker.write_bytes(original[:10])
                elif damage == 'symlink':
                    target = self.directory / 'marker-target'
                    target.write_bytes(original)
                    marker.symlink_to(target)
                elif damage == 'fifo':
                    os.mkfifo(marker)
                with self.assertRaises(InvalidData):
                    Configuration(self.filename)
                if damage == 'missing':
                    self.assertFalse(marker.exists())
                else:
                    # Explicit setup must not silently replace corrupt evidence either.
                    if damage == 'different':
                        before = marker.read_bytes()
                        review_directory(marker.parent)
                        self.assertEqual(marker.read_bytes(), before)
                    else:
                        with self.assertRaises((InvalidData, OSError)):
                            review_directory(marker.parent)
                    marker.unlink()
                marker.write_bytes(original)

    def test_interrupted_provision_and_registry_publication_are_retryable(self):
        fresh = self.directory / 'fresh'
        fresh.mkdir()
        with patch('stash_ingest.storage_marker.os.link', side_effect=OSError('interrupted publication')):
            with self.assertRaises(OSError):
                review_directory(fresh)
        self.assertEqual(list(fresh.iterdir()), [])
        binding = review_directory(fresh)
        self.assertEqual(review_directory(fresh), binding)
        output = self.directory / 'retried.json'
        args = ['--root', ROOT, str(fresh), '--locks', str(self.directory/'locks'), '--output', str(output)]
        with patch('stash_ingest.config_migration.publish_profile', side_effect=OSError('interrupted registry')), \
                redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            self.assertEqual(main(args), 1)
        self.assertFalse(output.exists())
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            self.assertEqual(main(args), 0)
        self.assertEqual(StorageBindings(json.loads(output.read_bytes()), output).document['roots'][ROOT], binding)

    def test_concurrent_explicit_setups_share_one_marker(self):
        from concurrent.futures import ThreadPoolExecutor
        directory = self.directory / 'concurrent'
        directory.mkdir()
        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(review_directory, [directory] * 16))
        self.assertTrue(all(result == results[0] for result in results))
        self.assertEqual([path.name for path in directory.iterdir()], [NAME])

    def test_old_registry_tokens_keep_strict_historical_semantics(self):
        from stash_ingest.filesystem import Root
        value = copy.deepcopy(self.value)
        for binding in (*value['roots'].values(), value['locks']):
            old = Root.review(binding['path'])
            binding['token'] = f"fs-v1:{old['filesystem_id']:x}:{old['identity'][1]:x}"
        storage = StorageBindings(value, self.registry)
        storage.bind(ROOT)
        self.assertEqual(storage.verification_files(ROOT), [])
        with patch('os.fstatvfs', return_value=SimpleNamespace(f_fsid=1)):
            with self.assertRaises(InvalidData):
                storage.bind(ROOT)

    def test_directory_change_during_hashing_is_rejected_even_with_copied_marker(self):
        current = self.activate()
        media = self.directory / 'media'
        output = media / 'finished.jpg'
        output.write_bytes(b'finished content')
        original_read = os.read
        moved = False
        def move_during_read(fd, amount):
            nonlocal moved
            result = original_read(fd, amount)
            if amount == 1 << 20 and not moved:
                moved = True
                marker = (media / NAME).read_bytes()
                media.rename(self.directory / 'replaced')
                media.mkdir()
                (media / NAME).write_bytes(marker)
                output.write_bytes(b'finished content')
            return result
        with patch('os.read', side_effect=move_during_read):
            with self.assertRaisesRegex(InvalidData, 'root moved'):
                current.root.completed(output)

    def test_registry_paths_translate_without_changing_logical_references(self):
        logical = copy.deepcopy(self.value)
        for binding in (*logical['roots'].values(), logical['locks']):
            binding['path'] = '/container/' + Path(binding['path']).name
        def translate(value, base):
            return self.directory/Path(value).name
        storage = StorageBindings(logical, self.registry, path_resolver=translate)
        media, locks = storage.bind(ROOT)
        self.assertEqual(media.path, self.directory/'media')
        self.assertEqual(locks.path, self.directory/'locks')
        self.assertEqual(storage.uuid, self.value['uuid'])

    def test_review_command_publishes_private_registry_without_overwriting(self):
        output = self.directory/'reviewed.json'
        args = ['--root', ROOT, str(self.directory/'media'), '--locks', str(self.directory/'locks'), '--output', str(output)]
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            self.assertEqual(main(args), 0)
            before = output.read_bytes()
            self.assertEqual(main(args), 1)
        self.assertEqual(output.read_bytes(), before)
        self.assertEqual(output.stat().st_mode & 0o077, 0)
        result = StorageBindings(json.loads(before), output)
        result.bind(ROOT)

    def test_conversion_uses_shared_registry_without_changing_scrape_rules(self):
        self.filename, base, overlay, _ = layered_fixture(self.directory)
        previous = Configuration(self.filename)
        for layered in (False, True):
            with self.subTest(layered=layered):
                output = self.directory / ('layered.json' if layered else 'flat.json')
                args = ['--config', str(base), '--config', str(overlay), '--root', ROOT,
                        '--storage', str(self.registry), '--category', 'twitter', '--full-history',
                        '--working-directory', str(self.directory), '--output', str(output)]
                if layered:
                    args.append('--layered')
                with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                    self.assertEqual(convert(args), 0)
                configured = Configuration(output)
                self.assertEqual(configured.policy_sha256, previous.policy_sha256)
                self.assertEqual(configured._gallery, previous._gallery)
                self.assertEqual(configured.document['root'], {'uuid': ROOT})
                self.assertNotIn('locks', configured.document)
