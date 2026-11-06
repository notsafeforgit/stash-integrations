"""Reject mutable references in GitHub workflows and local action definitions."""

import argparse
from pathlib import Path
import re
import subprocess
import sys

import yaml


COMMIT_REF = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:/[^@\s]+)?@[a-fA-F0-9]{40}")
DIGEST_REF = re.compile(r"docker://[^@\s]+@sha256:[a-fA-F0-9]{64}")


class UniqueKeyLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        # Reject duplicate explicit keys, while allowing YAML merge overrides.
        keys = set()
        for key_node, _ in node.value:
            if key_node.tag == "tag:yaml.org,2002:merge":
                continue
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, (str, int, float, bool, type(None))):
                raise ValueError("YAML mapping keys must be scalars")
            if key in keys:
                raise ValueError(f"duplicate YAML key: {key!r}")
            keys.add(key)
        return super().construct_mapping(node, deep=deep)


def pinned(ref):
    return isinstance(ref, str) and "${{" not in ref and bool(
        ref.startswith("./") or COMMIT_REF.fullmatch(ref) or DIGEST_REF.fullmatch(ref)
    )


def check_definition(data):
    document = yaml.load(data, Loader=UniqueKeyLoader)
    if not isinstance(document, dict):
        raise ValueError("expected a YAML mapping")
    problems = []

    def check(location, ref):
        if not pinned(ref):
            problems.append(
                f"{location}: {ref!r} must use a full 40-character commit SHA "
                "(or a SHA-256 digest for docker:// actions)"
            )

    def check_steps(location, steps):
        if not isinstance(steps, list):
            raise ValueError(f"{location} must be a list")
        for index, step in enumerate(steps):
            if not isinstance(step, dict):
                raise ValueError(f"{location}[{index}] must be a mapping")
            if "uses" in step:
                check(f"{location}[{index}].uses", step["uses"])

    jobs = document.get("jobs", {})
    if not isinstance(jobs, dict):
        raise ValueError("jobs must be a mapping")
    for name, job in jobs.items():
        if not isinstance(job, dict):
            raise ValueError(f"jobs.{name} must be a mapping")
        if "uses" in job:
            check(f"jobs.{name}.uses", job["uses"])
        check_steps(f"jobs.{name}.steps", job.get("steps", []))
    runs = document.get("runs", {})
    if not isinstance(runs, dict):
        raise ValueError("runs must be a mapping")
    check_steps("runs.steps", runs.get("steps", []))
    image = runs.get("image", "")
    if isinstance(image, str) and image.startswith("docker://"):
        check("runs.image", image)
    return problems


def is_definition(path):
    return path.name in {"action.yml", "action.yaml"} or (
        path.as_posix().startswith(".github/workflows/")
        and path.suffix in {".yml", ".yaml"}
    )


def check_repository(root):
    # Include uncommitted new files, excluding ignored dependency checkouts.
    paths = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=root,
    ).decode().split("\0")
    problems = []
    checked = 0
    for name in sorted(set(paths)):
        if not is_definition(Path(name)):
            continue
        path = root / name
        if not path.exists():
            continue  # A tracked file can be deleted in the working tree.
        checked += 1
        try:
            problems.extend(f"{name}: {message}" for message in check_definition(path.read_text()))
        except (OSError, ValueError, yaml.YAMLError) as error:
            problems.append(f"{name}: {error}")
    if not checked:
        problems.append("no workflow or action definitions found; run from the repository root")
    return problems


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", nargs="?", type=Path, default=Path("."))
    args = parser.parse_args()
    problems = check_repository(args.root)
    if problems:
        print("\n".join(problems), file=sys.stderr)
        return 1
    print("All external workflow and action references are pinned.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
