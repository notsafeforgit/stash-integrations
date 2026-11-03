"""Compact old acknowledged receipts independently of crawl completion.

Every batch is checked against a held native snapshot before replacing local
receipt copies. Event identities, dependency rows and unfinished payloads stay put.
An interrupted pass can resume without a separate ledger or extra receipt copies.
"""

import time

from .encoding import InvalidData
from .receipt_proofs import MAGIC, SIZE, native_record, proof, retained_record

BATCH = 2000


def compact(box, library, *, keep_days=1, now=None, apply=False, batch_size=BATCH):
    if (type(keep_days) is not int or not 1 <= keep_days <= 3650
            or type(batch_size) is not int or not 1 <= batch_size <= BATCH):
        raise InvalidData("Invalid acknowledgement retention interval or batch size")
    if (not library.in_transaction or library.execute("PRAGMA query_only").fetchone()[0] != 1
            or box.db.execute("PRAGMA user_version").fetchone()[0] not in ((25,) if apply else (24, 25))):
        raise InvalidData("Compaction requires producer schema 25 and a held read-only native snapshot")
    if (library.execute("SELECT lineage FROM native_schema WHERE singleton=1").fetchone()[0]
            != "org.notsafeforgit.stash.native-archive"
            or not library.execute("SELECT 1 FROM ingest_producers WHERE uuid=?", (box.producer,)).fetchone()):
        raise InvalidData("Native library does not contain this producer")
    cutoff = (time.time() if now is None else now) - keep_days * 86400
    after = 0
    result = {"eligible_events": 0, "compacted_events": 0, "receipt_bytes": 0, "saved_receipt_bytes": 0}
    while True:
        rows = box.db.execute("""SELECT seq,event_uuid,sha256,kind,collection_uuid,collection_revision,
            root_uuid,run_uuid,receipt FROM events e WHERE seq>? AND state='acknowledged'
            AND body IS NULL AND acknowledged_at<=?
            AND NOT (length(receipt)=? AND substr(receipt,1,?)=?)
            AND NOT EXISTS(SELECT 1 FROM retired_event_runs r WHERE r.run_uuid=e.run_uuid)
            ORDER BY seq LIMIT ?""", (after, cutoff, SIZE, len(MAGIC), MAGIC, batch_size)).fetchall()
        if not rows:
            return result
        replacements = []
        # Native reads never run inside the producer write transaction.
        for row in rows:
            server = library.execute("SELECT * FROM ingest_receipts WHERE producer_uuid=? AND event_uuid=?",
                                     (box.producer, row["event_uuid"])).fetchone()
            saved = retained_record(row, box.producer, native_record(server))
            replacements.append((proof(saved), row["seq"], row["event_uuid"], row["receipt"]))
            result["eligible_events"] += 1
            result["receipt_bytes"] += len(row["receipt"])
        if apply:
            with box.transaction():
                for replacement in replacements:
                    changed = box.db.execute("""UPDATE events SET receipt=? WHERE seq=? AND event_uuid=?
                        AND receipt=? AND state='acknowledged' AND body IS NULL
                        AND NOT EXISTS(SELECT 1 FROM retired_event_runs r WHERE r.run_uuid=events.run_uuid)""",
                                             replacement).rowcount
                    result["compacted_events"] += changed
                    result["saved_receipt_bytes"] += changed * (len(replacement[3]) - SIZE)
        after = rows[-1]["seq"]
