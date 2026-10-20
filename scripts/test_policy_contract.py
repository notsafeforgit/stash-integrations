import copy
import json
from pathlib import Path
import subprocess
import sys
import unittest

from check_policy_contract import SCHEMA, read_review, validate_review, validate_transition


class CompatibilityReviewTests(unittest.TestCase):
    def setUp(self):
        self.identity = {"execution_contract": "stash-worker-execution-v1", "capture_contract": "stash-gallery-v1"}
        self.review = {"schema": SCHEMA, **self.identity, "implementation_sha256": "a" * 64,
                       "decision": "introduce", "reason": "The initial explicit contract preserves existing queued command semantics."}

    def test_current_sources_have_a_review(self):
        subprocess.run([sys.executable, str(Path(__file__).with_name("check_policy_contract.py"))], check=True)

    def test_unreviewed_source_change_fails_without_affecting_runtime_policy(self):
        validate_review(self.review, self.identity, "a" * 64)
        with self.assertRaisesRegex(ValueError, "source changed"):
            validate_review(self.review, self.identity, "b" * 64)
        changed = {**self.review, "implementation_sha256": "b" * 64, "decision": "compatible",
                   "reason": "Comment and diagnostics changes do not alter accepted commands or cursor handling."}
        validate_review(changed, self.identity, "b" * 64)
        validate_transition(changed, self.review)
        self.assertEqual(changed["execution_contract"], self.review["execution_contract"])

    def test_contract_change_requires_a_new_version_and_explicit_decision(self):
        changed = {**self.review, "execution_contract": "stash-worker-execution-v2", "decision": "contract-change"}
        validate_transition(changed, self.review)
        with self.assertRaises(ValueError):
            validate_review(changed, self.identity, "a" * 64)
        validate_review(changed, {**self.identity, "execution_contract": "stash-worker-execution-v2"}, "a" * 64)
        for patch in ({"decision": "compatible"}, {"execution_contract": "stash-worker-execution-v1"},
                      {"execution_contract": "stash-worker-execution-v1", "capture_contract": "stash-gallery-v2", "decision": "compatible"}):
            with self.subTest(patch=patch), self.assertRaises(ValueError):
                validate_transition({**changed, **patch}, self.review)

    def test_invalid_unknown_and_ambiguous_reviews_are_rejected(self):
        for patch in ({"schema": SCHEMA + "-future"}, {"reason": ""}, {"decision": "automatic"},
                      {"implementation_sha256": None}, {"execution_contract": None}, {"execution_contract": "stash-worker-execution-v0"},
                      {"execution_contract": "stash-worker-execution-v2"}, {"extra": True}):
            with self.subTest(patch=patch), self.assertRaises(ValueError):
                validate_review({**self.review, **patch}, self.identity, "a" * 64)
        with self.assertRaises(ValueError):
            read_review(b'{"schema":"first","schema":"second"}')
        with self.assertRaises(ValueError):
            read_review(b' ' * 4097)
        validate_transition(self.review, None)
        with self.assertRaises(ValueError):
            validate_transition({**self.review, "decision": "compatible"}, None)
        validate_transition(copy.deepcopy(self.review), self.review)
        self.assertEqual(read_review(json.dumps(self.review).encode()), self.review)
