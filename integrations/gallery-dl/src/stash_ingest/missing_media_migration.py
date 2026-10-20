"""Upgrade-only reader for v0.3's adjacent missing-media database.

The caller holds the producer migration transaction. Import once when advancing
schema 18 to 19; leave the old file untouched on both success and rollback.
New-schema readers never consult or update it. Stop old writers before upgrading,
as with any producer schema change.
"""

from contextlib import closing
from datetime import datetime
import sqlite3
import stat

from .encoding import InvalidData
from .missing_media import initialize


def migrate(box, previous_version):
    initialize(box.db)
    path = box.path.with_name(box.path.name + '.missing-media.sqlite')
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if previous_version != 18 or not stat.S_ISREG(metadata.st_mode):
        raise InvalidData('Legacy missing-media state requires its original schema-18 outbox and a regular file')
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=15)) as source:
        source.execute('PRAGMA query_only=ON')
        source.execute('BEGIN')
        if (source.execute('PRAGMA application_id').fetchone()[0] != 0x534D4953
                or source.execute('PRAGMA user_version').fetchone()[0] != 1):
            raise InvalidData('Unsupported or foreign legacy missing-media state')
        if source.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
            raise InvalidData('Corrupt legacy missing-media state')
        if source.execute('SELECT endpoint,producer FROM binding').fetchall() != [(box.endpoint, box.producer)]:
            raise InvalidData('Legacy missing-media state identifies a different producer')
        for row in source.execute('SELECT service,item,attempts,first_seen,last_seen,http_status FROM missing_media'):
            service, item, attempts, first, last, code = row
            try:
                valid = (service in ('imgur', 'redgifs') and isinstance(item, str) and 0 < len(item) <= 2048
                         and type(attempts) is int and 1 <= attempts <= 3 and type(code) is int and code in (404, 410)
                         and all(isinstance(value, str) and len(value) <= 64
                                 and datetime.fromisoformat(value).utcoffset() is not None for value in (first, last)))
            except ValueError:
                valid = False
            if not valid:
                raise InvalidData('Invalid legacy missing-media observation')
            box.db.execute('INSERT INTO missing_media VALUES(?,?,?,?,?,?)', row)
