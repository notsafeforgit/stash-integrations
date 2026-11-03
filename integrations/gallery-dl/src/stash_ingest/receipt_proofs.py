"""Versioned compact acknowledgement evidence; full receipts remain native-owned."""

import hashlib
import json

from .encoding import InvalidData, decode, encode

MAGIC = b"\x00SAR\x01"
SIZE = len(MAGIC) + 32
IDENTITY = ("event_uuid", "sha256", "collection_uuid", "collection_revision", "root_uuid", "run_uuid", "kind")


def is_compact(raw):
    return isinstance(raw, bytes) and raw.startswith(MAGIC)


def proof(value):
    from .outbox_history import receipt_record
    raw = (json.dumps(receipt_record(value), ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")
    return MAGIC + hashlib.sha256(raw).digest()


def native_record(row):
    from .outbox_history import receipt_record
    if row is None:
        raise InvalidData("Acknowledged event is missing its native receipt")
    value = dict(row)
    value["sha256"] = value.pop("digest")
    value["result"] = decode(value["result"].encode("utf-8"), 16384)
    return receipt_record(value)


def retained_record(row, producer, native=None):
    from .outbox_history import receipt_record
    raw = row["receipt"]
    if is_compact(raw):
        if native is None or len(raw) != SIZE or proof(native) != raw:
            raise InvalidData("Compact acknowledgement differs from its native receipt")
        saved = receipt_record(native)
    else:
        saved = receipt_record(decode(raw, 65536))
        if native is not None and encode(saved, 65536) != encode(receipt_record(native), 65536):
            raise InvalidData("Local acknowledgement differs from its native receipt")
    if saved["producer_uuid"] != producer or any(saved[key] != row[key] for key in IDENTITY):
        raise InvalidData("Local acknowledgement differs from its event")
    return saved
