"""Verify local profile retirement separately from native server receipts."""

import math

from .receipts import objects
from .storage import InvalidArchive


def validate_retirements(queue):
    if queue.execute('''SELECT 1 FROM profile_event_retirements r LEFT JOIN events e ON e.event_uuid=r.event_uuid
            WHERE e.event_uuid IS NULL OR e.kind!='account.profile' OR e.state!='review'
            OR e.error_code!='event_rejected_409' OR e.sha256!=r.sha256 OR e.parent_uuid IS NOT NULL
            OR e.body IS NULL OR e.receipt IS NOT NULL OR e.collection_revision>=r.superseding_revision
            OR EXISTS(SELECT 1 FROM events child WHERE child.parent_uuid=r.event_uuid) LIMIT 1''').fetchone():
        raise InvalidArchive('Invalid retired profile scope or retained event')


def retirement_for(queue, row):
    if row['state'] != 'review':
        return None
    records = list(objects(queue, '''SELECT event_uuid,sha256,superseding_revision,retired_at,
        CASE WHEN length(CAST(reason AS BLOB))<=4096 THEN reason END AS reason
        FROM profile_event_retirements WHERE event_uuid=?''', (row['event_uuid'],)))
    if not records:
        return None
    record, = records
    reason = record['reason']
    if (type(record['superseding_revision']) is not int or record['superseding_revision'] <= row['collection_revision']
            or not isinstance(reason, str) or not 20 <= len(reason.strip()) <= 1024
            or any(ord(c) < 32 for c in reason)
            or not isinstance(record['retired_at'], (float, int))
            or not math.isfinite(record['retired_at']) or record['retired_at'] <= 0):
        raise InvalidArchive('Invalid retained profile retirement decision')
    return record
