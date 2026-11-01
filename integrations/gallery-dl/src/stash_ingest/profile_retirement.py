"""Explicit local retirement of reviewed profile reports; never a server ACK."""

from .encoding import InvalidData, digest, identifier
from .outbox import Conflict


def migrate(db):
    # A small additive ledger avoids rewriting the event table or its payloads.
    db.execute('''CREATE TABLE profile_event_retirements(
        event_uuid TEXT PRIMARY KEY REFERENCES events(event_uuid),
        sha256 TEXT NOT NULL CHECK(length(sha256)=64),
        superseding_revision INTEGER NOT NULL CHECK(superseding_revision>1),
        reason TEXT NOT NULL CHECK(length(trim(reason)) BETWEEN 20 AND 1024),
        retired_at REAL NOT NULL CHECK(retired_at>0))''')
    db.execute('''CREATE TRIGGER profile_retirement_scope BEFORE INSERT ON profile_event_retirements
        WHEN NOT EXISTS(SELECT 1 FROM events e WHERE e.event_uuid=NEW.event_uuid
            AND e.sha256=NEW.sha256 AND e.kind='account.profile' AND e.state='review'
            AND e.error_code='event_rejected_409' AND e.parent_uuid IS NULL
            AND e.body IS NOT NULL AND e.receipt IS NULL
            AND e.collection_revision<NEW.superseding_revision)
        OR EXISTS(SELECT 1 FROM events WHERE parent_uuid=NEW.event_uuid)
        BEGIN SELECT RAISE(ABORT,'Profile retirement scope changed'); END''')
    for operation in ('UPDATE', 'DELETE'):
        db.execute(f'''CREATE TRIGGER profile_retirement_no_{operation.lower()}
            BEFORE {operation} ON profile_event_retirements
            BEGIN SELECT RAISE(ABORT,'Profile retirement history is immutable'); END''')
        db.execute(f'''CREATE TRIGGER retired_profile_no_{operation.lower()}
            BEFORE {operation} ON events
            WHEN EXISTS(SELECT 1 FROM profile_event_retirements WHERE event_uuid=OLD.event_uuid)
            BEGIN SELECT RAISE(ABORT,'Retired profile evidence is immutable'); END''')


def retire_profile(box, event_uuid, sha256, superseding_revision, reason, *, apply=False):
    """Record an operator-reviewed obsolete revision without dropping evidence.

    The operator verifies the newer collection definition before requesting this
    local disposition. No collection, scrape, receipt or media state is changed.
    """
    identifier(event_uuid)
    if (not isinstance(sha256, str) or len(sha256) != 64
            or any(c not in '0123456789abcdef' for c in sha256)
            or type(superseding_revision) is not int or superseding_revision < 2
            or not isinstance(reason, str) or not 20 <= len(reason.strip()) <= 1024
            or any(ord(c) < 32 for c in reason) or type(apply) is not bool):
        raise InvalidData('Retirement requires an exact digest, newer revision and review reason')
    reason = reason.strip()
    with box.transaction():
        row = box.db.execute('SELECT * FROM events WHERE event_uuid=?', (event_uuid,)).fetchone()
        if (row is None or row['kind'] != 'account.profile' or row['state'] != 'review'
                or row['error_code'] != 'event_rejected_409' or row['parent_uuid'] is not None
                or row['sha256'] != sha256 or row['body'] is None or row['receipt'] is not None
                or digest(row['body']) != sha256 or row['collection_revision'] >= superseding_revision
                or box.db.execute('SELECT 1 FROM events WHERE parent_uuid=? LIMIT 1', (event_uuid,)).fetchone()):
            raise Conflict('Only the exact reviewed profile rejected under an older revision can be retired')
        existing = box.db.execute('SELECT * FROM profile_event_retirements WHERE event_uuid=?', (event_uuid,)).fetchone()
        if existing and (existing['sha256'], existing['superseding_revision'], existing['reason']) != (sha256, superseding_revision, reason):
            raise Conflict('Profile already has a different immutable retirement decision')
        if apply and existing is None:
            box.db.execute('INSERT INTO profile_event_retirements VALUES(?,?,?,?,?)',
                           (event_uuid, sha256, superseding_revision, reason, box.clock()))
            existing = box.db.execute('SELECT * FROM profile_event_retirements WHERE event_uuid=?', (event_uuid,)).fetchone()
        return {'state': 'retired' if existing else 'preview', 'event_uuid': event_uuid,
                'sha256': sha256, 'collection_uuid': row['collection_uuid'],
                'collection_revision': row['collection_revision'], 'superseding_revision': superseding_revision,
                'reason': reason, 'retired_at': existing['retired_at'] if existing else None,
                'retained_bytes': len(row['body']), 'server_acknowledged': False}
