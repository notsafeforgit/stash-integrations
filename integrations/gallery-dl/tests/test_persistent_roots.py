import copy
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from stash_ingest.configuration import Configuration
from stash_ingest.configuration_store import Configurations
from stash_ingest.encoding import InvalidData
from stash_ingest.filesystem import Root, inherit_filesystem_identity
from stash_ingest.outbox import Outbox
from stash_ingest.profile_dispatch import ProfileDispatcher
from helpers import PRODUCER
from test_configuration import profile_fixture


class PersistentRootTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)

    def test_review_survives_device_renumbering_but_not_replacement(self):
        binding = Root.review(self.path)
        old = [binding['identity'][0] + 1, binding['identity'][1]]
        with self.assertRaises(InvalidData):
            Root(self.path, old)
        root = Root(self.path, old, filesystem_id=binding['filesystem_id'])
        media = self.path / 'file.jpg'
        media.write_bytes(b'unchanged media')
        self.assertEqual(root.completed(media), ('file.jpg', 15, hashlib.sha256(media.read_bytes()).hexdigest()))
        with patch('stash_ingest.filesystem.os.fstatvfs', return_value=SimpleNamespace(f_fsid=binding['filesystem_id'] ^ 1)):
            with self.assertRaises(InvalidData):
                root.verify()
        self.path.rename(self.path.with_name(self.path.name + '-old'))
        self.addCleanup(shutil.rmtree, self.path.with_name(self.path.name + '-old'))
        self.path.mkdir()
        with self.assertRaises(InvalidData):
            root.verify()

    def test_unknown_filesystems_and_bad_proofs_are_rejected(self):
        for value in (0, True, -1, 2**64, '123'):
            with self.subTest(value=value), self.assertRaises(InvalidData):
                Root(self.path, Root.probe(self.path), filesystem_id=value)
        with patch('stash_ingest.filesystem.os.fstatvfs', return_value=SimpleNamespace(f_fsid=0)):
            with self.assertRaises(InvalidData):
                Root.review(self.path)

    def test_proof_only_augments_the_exact_original_directory(self):
        original = {'uuid': PRODUCER, 'path': '/media', 'identity': [44, 1920]}
        reviewed = {**original, 'filesystem_id': 1234}
        self.assertEqual(inherit_filesystem_identity(original, reviewed), reviewed)
        for key, value in (('uuid', 'different'), ('path', '/other'), ('identity', [45, 1920])):
            other = {**reviewed, key:value}
            self.assertEqual(inherit_filesystem_identity(original, other), original)
        already = {**original, 'filesystem_id':5678}
        self.assertEqual(inherit_filesystem_identity(already, reviewed), already)
        self.assertNotIn('filesystem_id', original)

    def test_retained_policies_use_reviewed_mount_proof_without_rewriting_history(self):
        filename, document = profile_fixture(self.path)
        original = Configuration(filename)
        reviewed = copy.deepcopy(document)
        for name in ('root', 'locks'):
            reviewed[name]['filesystem_id'] = Root.review(document[name]['path'])['filesystem_id']
        with closing(Outbox(self.path/'outbox.sqlite', 'https://fixture.invalid', PRODUCER)) as box:
            saved = Configurations(box)
            saved.remember(original)
            row = saved.rows(filename)[0]
            before = bytes(row['document']), row['document_sha256'], row['policy_sha256']
            actual_fstat = os.fstat
            def renumber(fd):
                info = actual_fstat(fd)
                if stat.S_ISDIR(info.st_mode):
                    return SimpleNamespace(st_dev=info.st_dev+1, st_ino=info.st_ino, st_mode=info.st_mode)
                return info
            with patch('stash_ingest.filesystem.os.fstat', side_effect=renumber):
                with self.assertRaises(InvalidData):
                    saved.load(row)
                filename.write_text(json.dumps(reviewed))
                current = Configuration(filename)
                restored = saved.load(row, directory_bindings=current.document)
                self.assertEqual(restored.policy_sha256, original.policy_sha256)
                self.assertEqual(restored.document, original.document)
                restored.check()
                with patch('stash_ingest.profile_dispatch.Dispatcher') as dispatcher:
                    dispatcher.return_value.once.return_value = {'state':'idle'}
                    result = ProfileDispatcher(box, object(), filename).once()
                    self.assertEqual(result, {'state':'idle'})
                    self.assertEqual(dispatcher.call_args.args[2].root.filesystem_id, reviewed['root']['filesystem_id'])
            after = saved.rows(filename)[0]
            self.assertEqual(before, (bytes(after['document']), after['document_sha256'], after['policy_sha256']))
