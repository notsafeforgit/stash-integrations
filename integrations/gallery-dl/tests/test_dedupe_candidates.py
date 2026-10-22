import copy
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from stash_ingest.dedupe import Journal, private_directory, journal_database
from stash_ingest.dedupe_candidates import candidates_from_report, directory_identity, validate_review
from stash_ingest.dedupe_client import relative_path
from stash_ingest.encoding import InvalidData, decode, digest, encode
from stash_ingest.fclones_path import decode_path


class FclonesPathsTests(unittest.TestCase):
    def test_stfu8_is_decoded_once_without_reinterpreting_literal_names(self):
        cases = {
            r"/media/literal\\n.mp4": "/media/literal\\n.mp4",
            r"/media/line\nfeed.mp4": "/media/line\nfeed.mp4",
            r"/media/tab\treturn\r.mp4": "/media/tab\treturn\r.mp4",
            r"/media/byte\xFF.mp4": "/media/byte\udcff.mp4",
            r"/media/utf8\xC3\xA9.mp4": "/media/utf8é.mp4",
            r"/media/code\u00200B.mp4": "/media/code\u200b.mp4",
            "/media/café 中文 😺 ' \" $.mp4": "/media/café 中文 😺 ' \" $.mp4",
        }
        for encoded, literal in cases.items():
            with self.subTest(encoded=encoded):
                self.assertEqual(literal, decode_path(encoded))
        for bad in (None, 1, "\\", r"\z", r"\x0", r"\xGG", r"\u0020", r"\u110000", r"\u00D800", "\ud800"):
            with self.subTest(bad=repr(bad)), self.assertRaises(InvalidData):
                decode_path(bad)

    def test_api_path_rules_reject_controls_and_invalid_utf8(self):
        for name in ("a\\b.mp4", "a\n.mp4", "a\x7f.mp4", "a\u0085.mp4", "a\udcff.mp4"):
            with self.subTest(name=repr(name)), self.assertRaises(InvalidData):
                relative_path(name)
        self.assertEqual("café\u200b.mp4", relative_path("café\u200b.mp4"))

    def test_actual_fclones_output_preserves_paths_and_reviews_unsupported_groups(self):
        executable = shutil.which("fclones")
        if executable is None:
            self.skipTest("fclones not installed")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            names = ["café 中文 '\" $ 😺\u200b", "literal\\n", "line\nfeed", "tab\t", "control\u0085", "byte\udcff"]
            expected, original = [], {}
            for index, name in enumerate(names):
                paths = [root / f"{index}-{name}.mp4", root / f"{index}-other.mp4"]
                expected.append(set(map(str, paths)))
                for order, path in enumerate(paths):
                    original[path] = f"duplicate group {index}".encode() * 100
                    path.write_bytes(original[path])
                    os.utime(path, ns=(1000000000 + order, 1000000000 + order))
            result = subprocess.run([executable, "group", str(root), "--format", "json", "--hash-fn", "blake3"],
                                    check=True, capture_output=True)
            report = decode(result.stdout)
            actual = [set(decode_path(path) for path in group["files"]) for group in report["groups"]]
            self.assertCountEqual(expected, actual)
            identity = directory_identity(root)
            found = candidates_from_report(result.stdout, identity, all_content=True)
            self.assertEqual([{"keep_path": f"0-{names[0]}.mp4", "remove_path": "0-other.mp4"}], found.pairs)
            self.assertEqual(5, len(found.reviews))
            for review in found.reviews:
                self.assertEqual(2, len(validate_review(review, identity)))
            self.assertEqual(original, {path: path.read_bytes() for path in root.iterdir()})

    def test_unsupported_name_never_hides_a_bad_report_or_bypasses_bounds(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "media"
            root.mkdir()
            for name in ("normal.mp4", "literal\\name.mp4", "second.mp4"):
                (root / name).write_bytes(b"duplicate")
            identity = directory_identity(root)
            literal = str(root / "literal\\name.mp4").replace("\\", "\\\\")
            normal = str(root / "normal.mp4")
            outside = Path(directory) / "outside.mp4"
            outside.write_bytes(b"duplicate")
            (root / "link\\name.mp4").symlink_to(outside)
            for bad in (str(outside), str(root) + "/../outside.mp4", str(root) + "/./normal.mp4",
                        str(root) + "//normal.mp4", str(root / "link\\name.mp4").replace("\\", "\\\\"),
                        str(root) + r"/\u110000.mp4", str(root) + r"/null\x00.mp4"):
                body = encode({"groups": [{"file_len": 9, "files": [literal, bad]}]})
                with self.subTest(bad=bad), self.assertRaises((InvalidData, OSError)):
                    candidates_from_report(body, identity, all_content=True)
            with self.assertRaisesRegex(InvalidData, "Repeated"):
                candidates_from_report(encode({"groups": [{"file_len": 9, "files": [normal, str(root) + r"/\x6eormal.mp4"]}]}),
                                       identity, all_content=True)
            groups = [{"file_len": 9, "files": [literal, normal]},
                      {"file_len": 9, "files": [str(root / "second.mp4"), normal, str(root / "third.mp4")]}]
            with patch("stash_ingest.dedupe_candidates.MAX_PAIRS", 2), self.assertRaisesRegex(InvalidData, "exceeds"):
                candidates_from_report(encode({"groups": groups}), identity, all_content=True)

    def test_saved_reviews_are_checked_before_use(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = {"root_uuid": "00000000-0000-4000-8000-000000000001", "root": directory_identity(root)}
            review = {"reason": "unsupported_media_path", "file_len": 1,
                      "files": [str(root / "normal.mp4"), str(root) + r"/literal\\name.mp4"]}
            state, _ = private_directory(root / "state")
            journal = Journal.create(state, config, [], discovery_reviews=[review])
            with journal_database(state) as db:
                original = decode(db.execute("SELECT manifest FROM runs").fetchone()[0])
            changes = [None, [review, review], [{**review, "reason": "ignored"}],
                       [{**review, "files": [str(root / "a.mp4"), str(root / "b.mp4")]}],
                       [{**review, "files": ["/outside.mp4", review["files"][1]]}]]
            for reviews in changes:
                manifest = copy.deepcopy(original)
                manifest["discovery_reviews"] = reviews
                body = encode(manifest)
                with journal_database(state) as db:
                    db.execute("UPDATE runs SET manifest=?,sha256=?", (body, digest(body)))
                with self.subTest(reviews=reviews), self.assertRaises(InvalidData):
                    Journal(state, config)
            self.assertTrue(journal.report()["finished"])
            self.assertFalse(journal.report()["all_removed"])
