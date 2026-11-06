from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml

from check_action_pins import check_definition, check_repository, pinned


SHA = "0123456789abcdef0123456789abcdef01234567"


class ActionPinTests(unittest.TestCase):
    def test_immutable_and_local_references(self):
        for ref in (
            f"actions/checkout@{SHA}", f"owner/repo/sub/action@{SHA}",
            f"owner/repo/.github/workflows/build.yml@{SHA}",
            "./.github/actions/setup", "./.github/workflows/reuse.yml",
            "docker://ghcr.io/owner/image@sha256:" + "a" * 64,
        ):
            with self.subTest(ref=ref):
                self.assertTrue(pinned(ref))

    def test_mutable_and_invalid_references(self):
        for ref in (
            "actions/checkout@v6", "actions/checkout@v6.0.0", "owner/repo@main",
            "owner/repo@latest", "owner/repo@0123456", "owner/repo", None, 123,
            f"owner/repo@{SHA}a", "owner/repo@" + "g" * 40,
            "owner/repo@${{ inputs.ref }}", "./${{ inputs.action }}",
            "owner/repo/.github/workflows/build.yml@main",
            "docker://alpine:3", "docker://alpine@sha256:1234", f"docker://alpine@{SHA}",
        ):
            with self.subTest(ref=ref):
                self.assertFalse(pinned(ref))

    def test_yaml_syntax_and_action_locations(self):
        cases = (
            (f"jobs:\n  test:\n    steps:\n      - uses: 'actions/checkout@{SHA}' # v6\n", 0),
            ("jobs: {test: {steps: [{'uses': 'actions/checkout@v6'}]}}", 1),
            ("jobs:\n  test:\n    steps:\n      - uses: >-\n          actions/checkout@v6\n", 1),
            ("jobs: {test: {uses: 'owner/repo/.github/workflows/test.yml@main'}}", 1),
            ("runs: {using: composite, steps: [{uses: 'actions/cache@v5'}]}", 1),
            ("runs: {using: docker, image: 'docker://alpine:3'}", 1),
            ("runs: {using: docker, image: Dockerfile}", 0),
            ("jobs:\n  test:\n    steps:\n      - &setup {uses: 'actions/checkout@v6'}\n      - *setup\n      - <<: *setup\n        name: Merged step\n", 3),
            (f"jobs:\n  test:\n    steps:\n      - &setup {{uses: 'actions/checkout@v6'}}\n      - <<: *setup\n        uses: 'actions/checkout@{SHA}'\n", 1),
            ("jobs:\n  test:\n    steps:\n      - run: |\n          echo 'uses: owner/repo@main'\n        env: {uses: example}\n", 0),
        )
        for data, expected in cases:
            with self.subTest(data=data):
                self.assertEqual(len(check_definition(data)), expected)

    def test_rejects_malformed_yaml(self):
        for data in (
            "", "jobs: [", "jobs: []", "jobs: {test: {steps: {uses: 'owner/repo@main'}}}",
            "jobs: {}\n---\njobs: {test: {uses: 'owner/repo@main'}}",
            "jobs: {}\njobs: {}", "jobs: {test: {steps: [*missing]}}",
            "jobs: {test: {steps: [{uses: x, uses: y}]}}",
        ):
            with self.subTest(data=data), self.assertRaises((ValueError, yaml.YAMLError)):
                check_definition(data)

    def test_repository_discovers_new_workflows_and_composite_actions(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            subprocess.run(["git", "init", "-q", str(root)], check=True)

            def write(name, data):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(data)

            write(".gitignore", "ignored/\n")
            bad_action = "runs: {using: composite, steps: [{uses: 'owner/repo@main'}]}"
            write("ignored/action.yml", bad_action)
            write(".github/ISSUE_TEMPLATE/example.yml", "not a workflow")
            self.assertTrue(check_repository(root))
            write(".github/workflows/check.yml", "jobs: {test: {uses: './.github/workflows/reuse.yaml'}}")
            write(".github/workflows/reuse.yaml", f"jobs: {{test: {{steps: [{{uses: 'actions/checkout@{SHA}'}}]}}}}")
            self.assertEqual(check_repository(root), [])
            paths = (".github/actions/setup/action.yml", "custom/test/action.yaml")
            for path in paths:
                write(path, bad_action)
            problems = check_repository(root)
            self.assertEqual(len(problems), len(paths))
            for path in paths:
                self.assertTrue(any(message.startswith(path + ":") for message in problems))


if __name__ == "__main__":
    unittest.main()
