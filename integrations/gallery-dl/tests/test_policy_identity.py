"""Compatibility survives source-only changes; provenance still identifies them."""
from contextlib import closing
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from stash_ingest.configuration import Configuration, policy_identity, runtime_identity
from stash_ingest.configuration_store import Configurations
from stash_ingest.encoding import InvalidData
from stash_ingest.outbox import Outbox
from stash_ingest.runtime import implementation_identity
from helpers import PRODUCER
from test_configuration import profile_fixture


class PolicyIdentityTests(unittest.TestCase):
    def test_comments_and_entrypoint_changes_do_not_strand_saved_work(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            profile, _ = profile_fixture(root)
            package = root / "stash_ingest"
            package.mkdir()
            code = package / "worker.py"
            code.write_text("def run():\n    return 1\n")
            for name in ("stash_gallery.py", "stash_ingest_bootstrap.py"):
                (root / name).write_text("# entrypoint\n")
            with (patch("stash_ingest.configuration.implementation_identity", side_effect=lambda: implementation_identity(package)),
                  closing(Outbox(root / "outbox.sqlite", "http://fixture.invalid", PRODUCER)) as box):
                before, provenance = Configuration(profile), runtime_identity()
                saved = Configurations(box)
                saved.remember(before)
                for changed in (code, root / "stash_gallery.py"):
                    changed.write_text(changed.read_text() + "# a harmless explanatory comment\n")
                    after = Configuration(profile)
                    self.assertEqual(after.policy_sha256, before.policy_sha256)
                    self.assertEqual(after.compatibility, before.compatibility)
                    current = runtime_identity()
                    self.assertNotEqual(current["adapter_sha256"], provenance["adapter_sha256"])
                    self.assertEqual(current["downloaders"], provenance["downloaders"])
                    row, = saved.rows(profile)
                    self.assertEqual(saved.load(row).policy_sha256, before.policy_sha256)
                    saved.remember(after)
                    self.assertEqual(box.db.execute("SELECT count(*) FROM worker_configurations").fetchone()[0], 1)
                    provenance = current
                self.assertEqual(box.db.execute("PRAGMA user_version").fetchone()[0], 19)

    def test_changed_contract_and_legacy_fingerprints_require_explicit_upgrade(self):
        with tempfile.TemporaryDirectory() as temporary:
            profile, _ = profile_fixture(temporary)
            current = policy_identity()
            with closing(Outbox(Path(temporary) / "outbox.sqlite", "http://fixture.invalid", PRODUCER)) as box:
                saved = Configurations(box)
                before = Configuration(profile)
                saved.remember(before)
                row, = saved.rows(profile)
                with patch("stash_ingest.configuration.EXECUTION_CONTRACT", "stash-worker-execution-v2"):
                    self.assertNotEqual(Configuration(profile).policy_sha256, before.policy_sha256)
                    self.assertEqual(saved.rows(profile), [])
                    with self.assertRaisesRegex(InvalidData, "compatibility upgrade"):
                        saved.load(row)
                # Existing source-hash policies must not silently become aliases
                # for the new contract. Their original admissions stay intact.
                legacy = {"capture_contract": current["capture_contract"], "adapter_sha256": "f" * 64}
                with patch("stash_ingest.configuration.policy_identity", return_value=legacy):
                    old = Configuration(profile)
                    saved.remember(old)
                raw = box.db.execute("SELECT * FROM worker_configurations WHERE policy_sha256=?", (old.policy_sha256,)).fetchone()
                with self.assertRaisesRegex(InvalidData, "compatibility upgrade"):
                    saved.load(raw)
                row, = saved.rows(profile)
                self.assertEqual(row["policy_sha256"], before.policy_sha256)
                self.assertEqual(box.db.execute("SELECT policy_sha256 FROM worker_configurations WHERE policy_sha256=?", (old.policy_sha256,)).fetchone()[0], old.policy_sha256)
