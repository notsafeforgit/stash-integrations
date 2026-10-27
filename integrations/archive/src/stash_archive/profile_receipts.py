"""Retain independent profile evidence across producer/library snapshots."""

from .download_receipts import one
from .receipts import checked_json, identifier, timestamp
from .storage import InvalidArchive, json_bytes


def verify_profile_observation(library, queue, producer, row, event, server):
    if queue.execute('PRAGMA user_version').fetchone()[0] < 22 or row['parent_uuid'] is not None:
        raise InvalidArchive('Profile observations require snapshot version 22 and no post dependency')
    if event is not None and row['body_length'] > 128 << 10:
        raise InvalidArchive('Oversized retained profile observation')
    if server is None:
        return  # The exact pending bytes remain available for first delivery.
    if not library.execute("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='source_account_profiles'").fetchall():
        raise InvalidArchive('Native snapshot is missing its account profile observations')
    profile = one(library, '''SELECT uuid,account_uuid,collection_uuid,collection_revision,observed_at,extractor_version,review,
                  CASE WHEN length(CAST(source AS BLOB))<=65536 THEN source END AS source
                  FROM source_account_profiles WHERE producer_uuid=? AND event_uuid=?''',
                  (producer, row['event_uuid']), 'Native profile receipt is missing its observation')
    result = checked_json(server['result'], 16384)
    expected = {'status': 'recorded', 'profile_uuid': identifier(profile['uuid']), 'media_ingested': False}
    if profile['account_uuid'] is not None:
        expected['account_uuid'] = identifier(profile['account_uuid'])
    if profile['review']:
        expected['review'] = profile['review']
    if (json_bytes(result) != json_bytes(expected)
            or any(server[k] is not None for k in ('post_uuid', 'capture_uuid', 'job_uuid'))
            or any(profile[k] != row[k] for k in ('collection_uuid', 'collection_revision'))):
        raise InvalidArchive('Native profile receipt differs from its observation')
    source = checked_json(profile['source'], 65536)
    if event is not None and (json_bytes(source) != json_bytes(event.get('source'))
            or event.get('extractor_version') != profile['extractor_version']
            or timestamp(event.get('observed_at')) != timestamp(profile['observed_at'], sqlite=True)):
        raise InvalidArchive('Pending profile differs from native history')
