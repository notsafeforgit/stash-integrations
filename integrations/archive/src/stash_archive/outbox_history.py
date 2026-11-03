"""Verify compact producer retirement proofs against authoritative native receipts."""

from collections import defaultdict
from datetime import timezone
import hashlib

from .receipts import checked_json, identifier, objects, timestamp
from .storage import HEX, InvalidArchive, json_bytes

FIELDS = ("producer_uuid", "event_uuid", "sha256", "credential_uuid", "collection_uuid",
          "collection_revision", "root_uuid", "run_uuid", "kind", "post_uuid",
          "capture_uuid", "job_uuid", "result", "committed_at")


def validate_history_state(queue):
    states = queue.execute("SELECT id,last_sequence FROM outbox_history_state").fetchall()
    maximum = queue.execute("SELECT coalesce(max(seq),0) FROM events").fetchone()[0]
    retired = queue.execute("SELECT coalesce(max(maximum_sequence),0) FROM retired_event_runs").fetchone()[0]
    if (len(states) != 1 or states[0][0] != 1 or type(states[0][1]) is not int
            or states[0][1] < max(maximum, retired, 0)):
        raise InvalidArchive("Producer history lost its event sequence high-water mark")


def verify_retired_runs(library, queue, producer):
    validate_history_state(queue)
    proofs = {}
    for row in objects(queue, "SELECT * FROM retired_event_runs ORDER BY run_uuid"):
        identifier(row["run_uuid"])
        if (row["state"] not in ("succeeded", "cancelled") or type(row["revision"]) is not int or row["revision"] < 1
                or type(row["receipt_count"]) is not int or row["receipt_count"] < 1
                or not isinstance(row["receipts_sha256"], str) or not HEX.fullmatch(row["receipts_sha256"])
                or type(row["maximum_sequence"]) is not int or row["maximum_sequence"] < 1
                or type(row["retired_at"]) not in (int, float) or row["retired_at"] <= 0):
            raise InvalidArchive("Invalid producer retirement proof")
        native = library.execute("SELECT state,revision,updated_at_ms FROM source_runs WHERE uuid=?", (row["run_uuid"],)).fetchall()
        if native != [(row["state"], row["revision"], row["closed_at_ms"])]:
            raise InvalidArchive("Retired producer history no longer matches its terminal source run")
        proofs[row["run_uuid"]] = row
    totals = defaultdict(lambda: [0, hashlib.sha256()])
    if proofs:
        for row in objects(library, "SELECT * FROM ingest_receipts WHERE producer_uuid=? ORDER BY event_uuid", (producer,)):
            if row["run_uuid"] not in proofs:
                continue
            row["sha256"] = row["digest"]
            row["result"] = checked_json(row["result"], 16384)
            row["committed_at"] = timestamp(row["committed_at"], sqlite=True).astimezone(timezone.utc).isoformat()
            count = totals[row["run_uuid"]]
            count[0] += 1
            count[1].update(json_bytes({key: row.get(key) for key in FIELDS}))
    for run, proof in proofs.items():
        count, digest = totals[run]
        if count != proof["receipt_count"] or digest.hexdigest() != proof["receipts_sha256"]:
            raise InvalidArchive("Retired producer receipts are missing or differ from the native library")
    return {"runs": len(proofs), "events": sum(p["receipt_count"] for p in proofs.values()), "receipts_verified": True}
