"""Offline validation of subscription scheduling intents and their handoff."""

import hashlib
import math
import re
import sqlite3

from .receipts import checked_json, identifier, objects
from .run_receipts import window
from .storage import HEX, InvalidArchive, json_bytes


def verify_scheduled_calls(queue, version):
    exists = queue.execute("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='scheduled_calls'").fetchone() is not None
    if exists != (version >= 20):
        raise InvalidArchive('Producer schema version would omit or invent scheduling intents')
    if not exists:
        return None
    counts = {state: 0 for state in ('pending', 'resolving', 'recorded', 'empty', 'review')}
    digest = hashlib.sha256()
    try:
        for row in objects(queue, """SELECT uuid,request_sha256,definition_sha256,state,owner,fence,lease_until,
                attempts,available_at,created_at,error_code,http_status,
                CASE WHEN length(definition)<=16384 THEN definition END AS definition FROM scheduled_calls ORDER BY seq"""):
            identifier(row['uuid'])
            spec = checked_json(row['definition'], 16384)
            if (set(spec) != {'service', 'mode', 'source'}
                    or spec['service'] not in ('reddit', 'twitter', 'instagram', 'coomer', 'kemono', 'bluesky', 'tiktok')
                    or spec['mode'] not in ('new', 'top') or spec['service'] != 'reddit' and spec['mode'] != 'new'
                    or not isinstance(row['request_sha256'], str) or not HEX.fullmatch(row['request_sha256'])
                    or hashlib.sha256(row['definition']).hexdigest() != row['definition_sha256']
                    or row['state'] not in counts):
                raise InvalidArchive('Invalid retained scheduling intent')
            source = spec['source']
            if (not isinstance(source, dict) or set(source) != {'root_uuid', 'operation', 'policy_sha256', 'cooldown_seconds', 'window'}
                    or source['operation'] != 'download' or not isinstance(source['policy_sha256'], str)
                    or not HEX.fullmatch(source['policy_sha256']) or type(source['cooldown_seconds']) is not int
                    or not 0 <= source['cooldown_seconds'] <= 86400):
                raise InvalidArchive('Invalid retained scheduling source')
            identifier(source['root_uuid'])
            if window(source['window']) != source['window']:
                raise InvalidArchive('Scheduled window changed')
            if (any(type(row[key]) is not int or row[key] < 0 for key in ('fence', 'attempts'))
                    or any(type(row[key]) not in (int, float) or not math.isfinite(row[key]) for key in ('available_at', 'created_at'))
                    or row['error_code'] is not None and (not isinstance(row['error_code'], str)
                        or re.fullmatch('[a-z0-9_]{1,64}', row['error_code']) is None)
                    or row['http_status'] is not None and (type(row['http_status']) is not int or not 100 <= row['http_status'] <= 599)):
                raise InvalidArchive('Invalid scheduling retry state')
            if row['state'] == 'resolving':
                identifier(row['owner'])
                if type(row['lease_until']) not in (int, float) or not math.isfinite(row['lease_until']):
                    raise InvalidArchive('Invalid scheduling lease')
            elif row['owner'] is not None or row['lease_until'] is not None:
                raise InvalidArchive('Invalid inactive scheduling lease')
            recorded = queue.execute('SELECT request_sha256,definition,definition_sha256 FROM source_calls WHERE uuid=?', (row['uuid'],)).fetchone()
            if (recorded is not None) != (row['state'] == 'recorded'):
                raise InvalidArchive('Scheduled request lost its atomic caller handoff')
            if recorded:
                call = checked_json(recorded[1], 8 << 20)
                if (recorded[0] != row['request_sha256'] or hashlib.sha256(recorded[1]).hexdigest() != recorded[2]
                        or {k: v for k, v in call.items() if k != 'subscriptions'} != source or 'subscriptions' not in call):
                    raise InvalidArchive('Scheduled request changed its caller definition')
                count = queue.execute('SELECT target_count FROM source_calls WHERE uuid=?', (row['uuid'],)).fetchone()[0]
                targets = queue.execute('SELECT target_url FROM source_call_targets WHERE call_uuid=?', (row['uuid'],)).fetchall()
                if (not isinstance(call['subscriptions'], dict) or not 1 <= count <= 10000
                        or len(targets) != count or {target[0] for target in targets} != set(call['subscriptions'])):
                    raise InvalidArchive('Scheduled request lost its frozen targets')
            counts[row['state']] += 1
            digest.update(json_bytes({key: value for key, value in row.items() if key != 'definition'}))
    except sqlite3.DatabaseError as error:
        raise InvalidArchive('Invalid producer scheduling table') from error
    return {'counts': counts, 'boundary_sha256': digest.hexdigest()}
