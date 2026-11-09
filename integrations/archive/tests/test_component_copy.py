import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from stash_archive.component_stage import copy_file, digest_file
from stash_archive.storage import InvalidArchive


class ComponentCopyDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.source = self.root / "private\nconfig.json"
        self.target = self.root / "component-00001"
        self.secret = b"fixture-private-token"
        self.source.write_bytes(self.secret)

    def test_stable_copy_preserves_bytes(self):
        copy_file(self.source, self.target, 0)
        self.assertEqual(self.target.read_bytes(), self.secret)

    def test_growth_reports_source_and_byte_counts_without_contents(self):
        def grow(*args):
            with self.source.open("ab") as output:
                output.write(b"extra")

        with patch("stash_archive.component_stage.require_space", side_effect=grow):
            with self.assertRaises(InvalidArchive) as raised:
                copy_file(self.source, self.target, 0)
        message = str(raised.exception)
        self.assertIn("External configuration grew during capture", message)
        self.assertIn(f"source={str(self.source)!r}", message)
        self.assertIn(f"expected_bytes={len(self.secret)}", message)
        self.assertIn(f"bytes_read={len(self.secret) + 5}", message)
        self.assertNotIn(self.secret.decode(), message)
        self.assertNotIn("\n", message)

    def test_rewrite_reports_changed_timestamps_without_contents(self):
        before = self.source.stat()
        fsync = os.fsync

        def rewrite(fd):
            fsync(fd)
            self.source.write_bytes(b"x" * len(self.secret))
            os.utime(self.source, ns=(before.st_atime_ns, before.st_mtime_ns + 1000000000))

        with patch("stash_archive.component_stage.os.fsync", side_effect=rewrite):
            with self.assertRaises(InvalidArchive) as raised:
                copy_file(self.source, self.target, 0)
        message = str(raised.exception)
        self.assertIn(f"source={str(self.source)!r}", message)
        self.assertIn(f"copied_bytes={len(self.secret)}", message)
        self.assertIn(f"mtime_ns={before.st_mtime_ns}", message)
        self.assertIn(f"mtime_ns={before.st_mtime_ns + 1000000000}", message)
        self.assertIn("ctime_ns=", message)
        self.assertNotIn(self.secret.decode(), message)

    def test_atomic_replacement_reports_both_file_identities(self):
        before = self.source.stat()
        replacement = self.root / "replacement.json"
        replacement.write_bytes(self.secret)
        os.utime(replacement, ns=(before.st_atime_ns, before.st_mtime_ns))
        after = replacement.stat()
        fsync = os.fsync

        def replace(fd):
            fsync(fd)
            os.replace(replacement, self.source)

        with patch("stash_archive.component_stage.os.fsync", side_effect=replace):
            with self.assertRaises(InvalidArchive) as raised:
                copy_file(self.source, self.target, 0)
        message = str(raised.exception)
        self.assertIn(f"source={str(self.source)!r}", message)
        self.assertIn(f"inode={before.st_ino}", message)
        self.assertIn(f"inode={after.st_ino}", message)
        self.assertIn("path_after=[", message)
        self.assertNotIn(self.secret.decode(), message)

    def test_digest_change_identifies_the_staged_path(self):
        import hashlib

        file_digest = hashlib.file_digest

        def change_after_read(stream, algorithm):
            digest = file_digest(stream, algorithm)
            self.source.write_bytes(b"changed")
            return digest

        with patch("stash_archive.component_stage.hashlib.file_digest", side_effect=change_after_read):
            with self.assertRaises(InvalidArchive) as raised:
                digest_file(self.source)
        message = str(raised.exception)
        self.assertIn(f"path={str(self.source)!r}", message)
        self.assertIn(f"size={len(self.secret)}", message)
        self.assertIn("size=7", message)
        self.assertNotIn(self.secret.decode(), message)
