"""Retire the discovery archive from producer outboxes (schema 26).

Stash schema 1000142 deletes the native account-listing and candidate-detail jobs.
Local discovery rows that still own a lease, a page body or reserved bytes are
unacknowledged evidence, so this migration refuses to run and leaves the outbox
unchanged. Finished rows without bodies only describe native outcomes that the
server no longer retains, and their cursors have no remaining meaning.
"""

from .encoding import InvalidData

EXECUTION_TABLES = ("discovery_executions", "discovery_detail_executions")
RETIRED_TABLES = EXECUTION_TABLES + ("discovery_dispatch", "discovery_collection_dispatch",
                                     "discovery_detail_dispatch", "discovery_detail_collection_dispatch")
RETIRED_COLUMNS = ("discovery_delivery_after", "discovery_detail_delivery_after")


def migrate(db):
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_schema WHERE type='table'")}
    for table in EXECUTION_TABLES:
        if table not in tables:
            continue
        unfinished = db.execute(f"""SELECT count(*) FROM {table}
            WHERE phase IN ('active','review') OR body IS NOT NULL OR reserved_bytes<>0""").fetchone()[0]
        if unfinished:
            raise InvalidData(f"Discovery retirement refused: {unfinished} unacknowledged {table} row(s) "
                              "still hold work or bytes; resolve them with the schema 25 producer first")
    for table in RETIRED_TABLES:
        if table in tables:
            db.execute(f"DROP TABLE {table}")
    columns = {row[1] for row in db.execute("PRAGMA table_info(worker_dispatch)")}
    # Upgrades from schema 10 through 12 still have the pre-rename enrichment cursor.
    if "delivery_after" in columns:
        db.execute("ALTER TABLE worker_dispatch RENAME COLUMN delivery_after TO enrichment_delivery_after")
    for column in RETIRED_COLUMNS:
        if column in columns:
            db.execute(f"ALTER TABLE worker_dispatch DROP COLUMN {column}")
