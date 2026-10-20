"""Release review gate: source provenance must have an explicit compatibility decision.

This file is a release check, never a runtime policy or an allowlist of module
hashes. Runtime admission uses the versioned contract; exact source changes are
reviewed here without incidentally changing retained queue identities.
"""

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys

SCHEMA = "stash-worker-compatibility-review-v1"
FIELDS = {"schema", "execution_contract", "capture_contract", "implementation_sha256", "decision", "reason"}
CONTRACT = re.compile(r"stash-worker-execution-v([1-9][0-9]*)\Z")
DIGEST = re.compile(r"[a-f0-9]{64}\Z")


def validate_review(review, identity, implementation):
    if not isinstance(review, dict) or set(review) != FIELDS or review["schema"] != SCHEMA:
        raise ValueError("Unsupported worker compatibility review format")
    version = review["execution_contract"]
    if not isinstance(version, str) or not CONTRACT.fullmatch(version):
        raise ValueError("Malformed execution contract")
    if any(review[key] != identity[key] for key in ("execution_contract", "capture_contract")):
        raise ValueError("Review does not describe this implementation's supported contracts")
    if (not isinstance(review["implementation_sha256"], str) or not DIGEST.fullmatch(review["implementation_sha256"])
            or review["implementation_sha256"] != implementation):
        raise ValueError("Producer source changed without an updated compatibility review")
    if review["decision"] not in ("introduce", "compatible", "contract-change"):
        raise ValueError("Review requires an explicit compatibility decision")
    if not isinstance(review["reason"], str) or not 20 <= len(review["reason"].strip()) <= 1024:
        raise ValueError("Review must explain why queued work remains compatible or needs an upgrade")


def validate_transition(current, previous):
    if previous is None:
        if current["decision"] != "introduce" or current["execution_contract"] != "stash-worker-execution-v1":
            raise ValueError("First compatibility review must introduce execution contract v1")
        return
    if current == previous:
        return
    if not isinstance(previous, dict) or previous.get("schema") != SCHEMA:
        raise ValueError("Unsupported previous compatibility review")
    match = CONTRACT.fullmatch(previous.get("execution_contract", ""))
    if match is None:
        raise ValueError("Invalid previous execution contract")
    old_version = int(match[1])
    new_version = int(CONTRACT.fullmatch(current["execution_contract"])[1])
    same = all(current[key] == previous[key] for key in ("execution_contract", "capture_contract"))
    if current["decision"] == "compatible" and same:
        return
    if current["decision"] == "contract-change" and new_version > old_version:
        return
    raise ValueError("Compatibility decision does not match the contract transition")


def source_identity(root):
    sys.path.insert(0, str(root / "integrations/gallery-dl/src"))
    from stash_ingest.runtime import CAPTURE_VERSION, EXECUTION_CONTRACT, implementation_identity
    return {"capture_contract": CAPTURE_VERSION, "execution_contract": EXECUTION_CONTRACT}, implementation_identity()


def read_review(data):
    # Duplicate members must not make a review mean different things to tools.
    def members(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("Duplicate compatibility review member")
            value[key] = item
        return value
    if len(data) > 4096:
        raise ValueError("Compatibility review exceeds 4 KiB")
    return json.loads(data, object_pairs_hook=members)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", help="GitHub event base SHA, for checking the reviewed contract transition")
    parser.add_argument("--print-identity", action="store_true", help="Print provenance to include in a reviewed declaration")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    identity, implementation = source_identity(root)
    if args.print_identity:
        print(json.dumps({**identity, "implementation_sha256": implementation}, sort_keys=True))
        return
    current = read_review((root / "policy-compatibility.json").read_bytes())
    validate_review(current, identity, implementation)
    if args.base and set(args.base) != {"0"}:
        if not re.fullmatch(r"[a-f0-9]{40}", args.base):
            raise ValueError("Review base must be an exact source revision")
        base = subprocess.run(["git", "show", f"{args.base}:policy-compatibility.json"], cwd=root,
                              capture_output=True, check=False)
        if base.returncode:
            # Only an absent review at a known ancestor is an initial transition.
            subprocess.run(["git", "cat-file", "-e", f"{args.base}^{{commit}}"], cwd=root, check=True)
            names = subprocess.check_output(["git", "ls-tree", "--name-only", args.base, "policy-compatibility.json"], cwd=root)
            if names.strip():
                raise ValueError("Could not read the previous compatibility review")
        validate_transition(current, read_review(base.stdout) if base.returncode == 0 else None)
    print(f"Reviewed {identity['execution_contract']}; exact producer source provenance matches")


if __name__ == "__main__":
    main()
