"""Retire verified delivery history of terminal source runs, retaining compact proofs.

The native library owns accepted receipts. Unfinished runs and every unresolved
event remain in the producer. No network request runs inside a SQLite transaction.
"""

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import itertools
import json
from pathlib import Path
import sqlite3
import time

from .encoding import InvalidData, decode, encode, identifier

FIELDS = ("producer_uuid", "event_uuid", "sha256", "credential_uuid", "collection_uuid",
          "collection_revision", "root_uuid", "run_uuid", "kind", "post_uuid",
          "capture_uuid", "job_uuid", "result", "committed_at")
TERMINAL = ("succeeded", "cancelled")
BATCH = 2000


def migrate(db):
    db.execute("""CREATE TABLE outbox_history_state(
        id INTEGER PRIMARY KEY CHECK(id=1), last_sequence INTEGER NOT NULL CHECK(last_sequence>=0))""")
    db.execute("INSERT INTO outbox_history_state VALUES(1,(SELECT coalesce(max(seq),0) FROM events))")
    db.execute("""CREATE TABLE retired_event_runs(
        run_uuid TEXT PRIMARY KEY, state TEXT NOT NULL CHECK(state IN ('succeeded','cancelled')),
        revision INTEGER NOT NULL CHECK(revision>0), closed_at_ms INTEGER NOT NULL,
        receipt_count INTEGER NOT NULL CHECK(receipt_count>0), receipts_sha256 TEXT NOT NULL,
        maximum_sequence INTEGER NOT NULL CHECK(maximum_sequence>0), retired_at REAL NOT NULL)""")
    db.execute("CREATE INDEX retained_event_runs ON events(run_uuid,event_uuid)")
    db.execute("""CREATE TRIGGER retired_event_run_no_insert BEFORE INSERT ON events
        WHEN EXISTS(SELECT 1 FROM retired_event_runs WHERE run_uuid=NEW.run_uuid)
        BEGIN SELECT RAISE(ABORT,'source run delivery history is retired'); END""")
    db.execute("""CREATE TRIGGER retired_event_run_no_update BEFORE UPDATE ON events
        WHEN EXISTS(SELECT 1 FROM retired_event_runs WHERE run_uuid=OLD.run_uuid)
        BEGIN SELECT RAISE(ABORT,'retired event history is immutable'); END""")
    db.execute("""CREATE TRIGGER outbox_event_high_water AFTER INSERT ON events
        BEGIN UPDATE outbox_history_state SET last_sequence=max(last_sequence,NEW.seq) WHERE id=1; END""")
    for operation in ("UPDATE", "DELETE"):
        db.execute(f"""CREATE TRIGGER retired_run_no_{operation.lower()} BEFORE {operation} ON retired_event_runs
            BEGIN SELECT RAISE(ABORT,'run retirement proofs are immutable'); END""")
    db.execute("PRAGMA user_version=24")


def receipt_record(value):
    if not isinstance(value, dict):
        raise InvalidData("Invalid native receipt")
    result = {key: value.get(key) for key in FIELDS}
    for key in ("producer_uuid", "event_uuid", "credential_uuid", "collection_uuid", "run_uuid"):
        identifier(result[key])
    for key in ("root_uuid", "post_uuid", "capture_uuid", "job_uuid"):
        if result[key] is not None:
            identifier(result[key])
    from .events import sha256
    if (not sha256(result["sha256"]) or type(result["collection_revision"]) is not int
            or result["collection_revision"] < 1 or not isinstance(result["result"], dict)
            or result["kind"] not in ("source.capture", "file.completed", "attachment.download", "account.profile")):
        raise InvalidData("Invalid retained receipt fields")
    try:
        stamp = datetime.fromisoformat(result["committed_at"])
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        result["committed_at"] = stamp.astimezone(timezone.utc).isoformat()
    except (ValueError, TypeError):
        raise InvalidData("Invalid receipt commit time") from None
    return result


def native_receipts(library, producer, selected):
    """One ordered scan, including when many thousands of runs are retired."""
    totals = defaultdict(lambda: [0, hashlib.sha256()])
    sql = "SELECT * FROM ingest_receipts WHERE producer_uuid=? ORDER BY event_uuid"
    for source in library.execute(sql, (producer,)):
        if source["run_uuid"] not in selected:
            continue
        row = dict(source)
        row["sha256"] = row.pop("digest")
        row["result"] = decode(row["result"].encode("utf-8"), 16384)
        record = receipt_record(row)
        item = totals[row["run_uuid"]]
        item[0] += 1
        item[1].update(encode(record, 65536) + b"\n")
    return {run: {"receipt_count": count, "receipts_sha256": digest.hexdigest()}
            for run, (count, digest) in totals.items()}


def inspect(queue, library, endpoint, *, keep_days=1, now=None):
    from .endpoint import origin
    from .outbox import APPLICATION_ID
    if type(keep_days) is not int or not 1 <= keep_days <= 3650:
        raise InvalidData("Keep between 1 and 3650 days of completed history")
    now = time.time() if now is None else now
    cutoff = now - keep_days * 86400
    if (queue.execute("PRAGMA application_id").fetchone()[0] != APPLICATION_ID
            or queue.execute("PRAGMA user_version").fetchone()[0] not in range(16, 25)):
        raise InvalidData("Unsupported producer history version")
    binding = queue.execute("SELECT endpoint,producer FROM binding WHERE id=1").fetchone()
    if binding is None or origin(binding["endpoint"]) != origin(endpoint):
        raise InvalidData("Outbox origin differs from the selected library")
    producer = identifier(binding["producer"])
    if (library.execute("SELECT lineage FROM native_schema WHERE singleton=1").fetchone()[0]
            != "org.notsafeforgit.stash.native-archive"
            or not library.execute("SELECT 1 FROM ingest_producers WHERE uuid=?", (producer,)).fetchone()):
        raise InvalidData("Native library does not contain this producer")
    runs = {r["uuid"]: dict(r) for r in library.execute("""SELECT uuid,state,revision,updated_at_ms
        FROM source_runs WHERE state IN ('succeeded','cancelled')""")}
    retired = ({r["run_uuid"]: dict(r) for r in queue.execute("SELECT * FROM retired_event_runs")}
               if queue.execute("PRAGMA user_version").fetchone()[0] >= 24 else {})
    candidates, protected = {}, 0
    resumable = []
    for row in queue.execute("""SELECT run_uuid,count(*) AS count,max(seq) AS maximum_sequence,
            sum(state!='acknowledged' OR body IS NOT NULL OR receipt IS NULL) AS unfinished,
            max(coalesce(acknowledged_at,created_at)) AS latest FROM events GROUP BY run_uuid"""):
        if row["run_uuid"] in retired:
            if row["unfinished"] or row["maximum_sequence"] > retired[row["run_uuid"]]["maximum_sequence"]:
                raise InvalidData("Retired source run contains new or unfinished events")
            resumable.append(row["run_uuid"])
        elif (row["run_uuid"] in runs and runs[row["run_uuid"]]["updated_at_ms"] <= int(cutoff * 1000)
                and not row["unfinished"] and row["latest"] <= cutoff):
            candidates[row["run_uuid"]] = dict(row)
        else:
            protected += row["count"]
    selected = set(candidates) | set(retired)
    native = native_receipts(library, producer, selected) if selected else {}
    for run_uuid, proof in retired.items():
        run = runs.get(run_uuid)
        if (run is None or (run["state"], run["revision"], run["updated_at_ms"])
                != (proof["state"], proof["revision"], proof["closed_at_ms"])
                or native.get(run_uuid) != {k: proof[k] for k in ("receipt_count", "receipts_sha256")}):
            raise InvalidData("Existing retirement proof no longer matches the native library")
    plans, mismatches = [], []
    # A single ordered scan also handles previews of pre-migration databases,
    # which do not yet have the run index. Never scan the whole queue per run.
    rows = queue.execute("""SELECT seq,event_uuid,sha256,kind,collection_uuid,collection_revision,
        root_uuid,run_uuid,parent_uuid,receipt FROM events ORDER BY run_uuid,event_uuid""") if candidates else []
    for run_uuid, records in itertools.groupby(rows, key=lambda row: row["run_uuid"]):
        if run_uuid not in candidates:
            continue
        candidate = candidates[run_uuid]
        digest, count, receipt_bytes = hashlib.sha256(), 0, 0
        for row in records:
            saved = receipt_record(decode(row["receipt"], 65536))
            expected = {key: row[key] for key in ("event_uuid", "sha256", "collection_uuid", "collection_revision",
                                                 "root_uuid", "run_uuid", "kind")}
            if saved["producer_uuid"] != producer or any(saved[k] != v for k, v in expected.items()):
                raise InvalidData("Local acknowledgement differs from its event")
            if row["parent_uuid"] is not None:
                parent = queue.execute("SELECT run_uuid,seq,state FROM events WHERE event_uuid=?", (row["parent_uuid"],)).fetchone()
                if parent is None or parent["run_uuid"] != run_uuid or parent["seq"] >= row["seq"] or parent["state"] != "acknowledged":
                    raise InvalidData("Retirement would cross an unresolved event dependency")
            digest.update(encode(saved, 65536) + b"\n")
            count += 1
            receipt_bytes += len(row["receipt"])
        proof = {"receipt_count": count, "receipts_sha256": digest.hexdigest()}
        if count != candidate["count"] or native.get(run_uuid) != proof:
            mismatches.append(run_uuid)
            protected += count
            continue
        run = runs[run_uuid]
        plans.append({"run_uuid": run_uuid, "state": run["state"], "revision": run["revision"],
                      "closed_at_ms": run["updated_at_ms"], **proof,
                      "maximum_sequence": candidate["maximum_sequence"], "retired_at": now,
                      "receipt_bytes": receipt_bytes, "latest": candidate["latest"]})
    return {"format": "stash-outbox-history-retirement-v1", "producer": producer, "endpoint": origin(endpoint),
            "keep_days": keep_days, "checked_at": now, "runs": plans, "mismatched_runs": mismatches,
            "protected_events": protected, "resumable_runs": resumable}


def apply(box, plan, *, batch_size=BATCH):
    if (plan.get("format") != "stash-outbox-history-retirement-v1" or plan.get("producer") != box.producer
            or plan.get("endpoint") != box.endpoint or not 1 <= batch_size <= BATCH):
        raise InvalidData("Retirement plan does not identify this outbox")
    retired = sum(finish_retirement(box, run, batch_size=batch_size) for run in plan.get("resumable_runs", []))
    for proof in plan["runs"]:
        with box.transaction():
            previous = box.db.execute("SELECT * FROM retired_event_runs WHERE run_uuid=?", (proof["run_uuid"],)).fetchone()
            keys = ("run_uuid", "state", "revision", "closed_at_ms", "receipt_count", "receipts_sha256", "maximum_sequence")
            if previous is not None:
                if any(previous[k] != proof[k] for k in keys):
                    raise InvalidData("Retirement proof changed")
            else:
                row = box.db.execute("""SELECT count(*) AS count,max(seq) AS seq,
                    sum(state!='acknowledged' OR body IS NOT NULL OR receipt IS NULL) AS unfinished,
                    max(coalesce(acknowledged_at,created_at)) AS latest FROM events WHERE run_uuid=?""", (proof["run_uuid"],)).fetchone()
                if (row["count"] != proof["receipt_count"] or row["seq"] != proof["maximum_sequence"]
                        or row["unfinished"] or row["latest"] != proof["latest"]):
                    raise InvalidData("Outbox run changed after verification; inspect again")
                columns = (*keys, "retired_at")
                box.db.execute(f"INSERT INTO retired_event_runs({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
                               tuple(proof[k] for k in columns))
        retired += finish_retirement(box, proof["run_uuid"], batch_size=batch_size)
    return retired


def finish_retirement(box, run_uuid, *, batch_size=BATCH):
    """Children have larger sequences; each short transaction removes leaves first."""
    deleted = 0
    while True:
        with box.transaction():
            if not box.db.execute("SELECT 1 FROM retired_event_runs WHERE run_uuid=?", (run_uuid,)).fetchone():
                raise InvalidData("No verified retirement exists")
            changed = box.db.execute("""DELETE FROM events WHERE seq IN (
                SELECT seq FROM events WHERE run_uuid=? ORDER BY seq DESC LIMIT ?)""", (run_uuid, batch_size)).rowcount
        deleted += changed
        if changed < batch_size:
            return deleted


def summary(plan):
    return {key: plan[key] for key in ("keep_days", "protected_events", "mismatched_runs")} | {
        "eligible_runs": len(plan["runs"]), "eligible_events": sum(r["receipt_count"] for r in plan["runs"]),
        "receipt_bytes": sum(r["receipt_bytes"] for r in plan["runs"])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--outbox", required=True, type=Path)
    parser.add_argument("--library", required=True, type=Path)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--keep-days", type=int, default=1)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    with sqlite3.connect(args.outbox.resolve().as_uri() + "?mode=ro", uri=True) as queue, \
            sqlite3.connect(args.library.resolve().as_uri() + "?mode=ro", uri=True) as library:
        queue.row_factory = library.row_factory = sqlite3.Row
        for connection in (queue, library):
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
        plan = inspect(queue, library, args.endpoint, keep_days=args.keep_days)
    queue.close()
    library.close()
    if args.report:
        args.report.write_text(json.dumps(plan, sort_keys=True) + "\n")
    result = summary(plan)
    if args.apply:
        from .outbox import Outbox
        box = Outbox(args.outbox, plan["endpoint"], plan["producer"], create=False)
        try:
            result["deleted_events"] = apply(box, plan)
        finally:
            box.close()
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
