"""Offline verification of schema-19 producer skip decisions."""

from datetime import datetime
import hashlib
import sqlite3

from .storage import InvalidArchive, json_bytes


def verify_missing_media(queue, version):
    exists = queue.execute("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='missing_media'").fetchone() is not None
    if exists != (version >= 19):
        raise InvalidArchive('Producer schema version would omit or invent missing-media decisions')
    if not exists:
        return None
    digest = hashlib.sha256()
    counts = {'retrying': 0, 'skipped': 0}
    try:
        rows = queue.execute('SELECT service,item,attempts,first_seen,last_seen,http_status FROM missing_media ORDER BY service,item')
        previous = None
        for service, item, attempts, first, last, code in rows:
            try:
                valid = (service in ('imgur', 'redgifs') and isinstance(item, str) and 0 < len(item) <= 2048
                         and type(attempts) is int and 1 <= attempts <= 3 and type(code) is int and code in (404, 410)
                         and all(isinstance(value, str) and len(value) <= 64
                                 and datetime.fromisoformat(value).utcoffset() is not None for value in (first, last)))
            except ValueError:
                valid = False
            if not valid or previous == (service, item):
                raise InvalidArchive('Invalid producer missing-media observation')
            previous = service, item
            counts['skipped' if attempts == 3 else 'retrying'] += 1
            digest.update(json_bytes({'service': service, 'item': item, 'attempts': attempts,
                                      'first_seen': first, 'last_seen': last, 'http_status': code}))
    except sqlite3.DatabaseError as error:
        raise InvalidArchive('Invalid producer missing-media table') from error
    return {'counts': counts, 'boundary_sha256': digest.hexdigest()}
