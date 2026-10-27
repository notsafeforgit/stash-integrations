"""Retain a completed, verified server copy before fallible archive packing.

The caller serializes access and owns this private directory. Identity is fixed
before any download, and only an incomplete child of that owned intent is reset.
Replays still hash every component; a prior deep scan applies only to those exact
bytes. The exporter independently checks their hashes again while packing.
"""
from contextlib import closing
import hashlib
from pathlib import Path
import shutil

from .artwork_pins import directory
from .bundle import connect_readonly, database_metadata, verification_mode
from .component_stage import digest_file
from .storage import InvalidArchive, decode_json, json_bytes, open_regular, publish_bytes, sync_directory


def read(path):
    with open_regular(path) as stream:
        body = stream.read((16 << 20) + 1)
    if len(body) > 16 << 20:
        raise InvalidArchive("Retained server capture record exceeds its size limit")
    return decode_json(body)


def cached_capture(client, cache, *, reserve, verification="deep"):
    verification_mode(verification)
    cache = Path(cache)
    directory(cache, private=True)
    sealed = decode_json(client.seal(reserve=reserve))
    from .checkpoint_release import checkpoint_digest
    client.existing_only = True
    client.expected_checkpoint_sha256 = checkpoint_digest(sealed)
    intent = {"version": 1, "server": client.server, "checkpoint": sealed,
              "verification": verification}
    intent_path = cache / "intent.json"
    if intent_path.exists():
        if read(intent_path) != intent:
            raise InvalidArchive("Retained server capture belongs to a different checkpoint or policy")
    else:
        if any(cache.iterdir()):
            raise InvalidArchive("Unowned server capture cache requires inspection")
        publish_bytes(intent_path, json_bytes(intent))
    destination = cache / "server"
    receipt_path = cache / "verified.json"
    if not receipt_path.exists():
        if destination.exists() or destination.is_symlink():
            directory(destination, private=True)
            shutil.rmtree(destination)
        result = client.capture(destination, reserve=reserve, verification=verification)
        if read(destination / "checkpoint.json") != sealed:
            raise InvalidArchive("Retained server capture changed during its copy")
        native, metadata, components, expected = result
        publish_bytes(receipt_path, json_bytes({"intent_sha256": hashlib.sha256(json_bytes(intent)).hexdigest(),
                                               "sqlite": metadata}))
        return result
    directory(destination, private=True)
    receipt = read(receipt_path)
    if (not isinstance(receipt, dict) or set(receipt) != {"intent_sha256", "sqlite"}
            or receipt["intent_sha256"] != hashlib.sha256(json_bytes(intent)).hexdigest()
            or read(destination / "checkpoint.json") != sealed):
        raise InvalidArchive("Retained server verification differs from its sealed capture")
    components, expected = [], {}
    for entry in sealed["components"]:
        path = destination / entry["name"]
        if digest_file(path) != (entry["sha256"], entry["bytes"]):
            raise InvalidArchive("Retained server component changed after verification")
        name = "library" if entry["role"] == "library" else entry["name"]
        expected[(entry["role"], name)] = (entry["sha256"], entry["bytes"])
        if entry["role"] != "library":
            components.append({"role": entry["role"], "name": name, "path": path})
    native = destination / "library.sqlite"
    with closing(connect_readonly(native)) as db:
        metadata = database_metadata(db, "library", verification="capture")
    if metadata != receipt["sqlite"]:
        raise InvalidArchive("Retained server database identity changed")
    components.append({"role": "operating_state", "name": "server-checkpoint.json",
                       "path": destination / "checkpoint.json"})
    expected[("operating_state", "server-checkpoint.json")] = digest_file(destination / "checkpoint.json")
    sync_directory(cache)
    return native, metadata, components, expected
