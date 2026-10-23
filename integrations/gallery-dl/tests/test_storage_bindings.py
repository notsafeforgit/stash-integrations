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
        with patch('stash_ingest.filesystem.os.fstat', side_effect=renumber):
            self.activate().check()
        (self.directory/'media').rename(self.directory/'old')
        (self.directory/'media').mkdir()
        with self.assertRaises(InvalidData):
            Configuration(self.filename)

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
