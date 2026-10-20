"""Bounded retries for a known external attachment, never a missing account.

Only exact single-item Imgur/Redgifs API 404/410 responses qualify. After
three observations the producer retains a skip decision across restarts.
Authentication, listing, rate-limit and transport failures remain retryable.
"""

from contextlib import contextmanager
import os
import re
import sqlite3
from urllib.parse import urlsplit

from . import reddit_external
from .encoding import InvalidData, utc_now
from .retention import retain

MAX_ATTEMPTS = 3
APPLICATION_ID = 0x534D4953  # SMIS: expendable skip observations, not a producer queue.


class MissingMedia(Exception):
    def __init__(self, status):
        self.status = status
        super().__init__('Linked media remains missing after bounded retries')


def initialize(db):
    db.execute('''CREATE TABLE missing_media (
        service TEXT NOT NULL CHECK(service IN ('imgur','redgifs')), item TEXT NOT NULL,
        attempts INTEGER NOT NULL CHECK(attempts BETWEEN 1 AND 3),
        first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
        http_status INTEGER NOT NULL CHECK(http_status IN (404,410)),
        PRIMARY KEY(service,item))''')


@contextmanager
def journal(box):
    # This cache may be discarded without losing a caller, capture or receipt.
    # Keep it separate so old producer/archive readers remain compatible.
    path = box.path.with_name(box.path.name + '.missing-media.sqlite')
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    os.close(fd)
    db = sqlite3.connect(path, timeout=15)
    db.row_factory = sqlite3.Row
    try:
        db.execute('PRAGMA synchronous=FULL')
        db.execute('BEGIN IMMEDIATE')
        version = db.execute('PRAGMA user_version').fetchone()[0]
        application = db.execute('PRAGMA application_id').fetchone()[0]
        if version == 0 and application == 0 and not db.execute("SELECT 1 FROM sqlite_master WHERE type='table'").fetchone():
            initialize(db)
            db.execute('CREATE TABLE binding(endpoint TEXT NOT NULL,producer TEXT NOT NULL)')
            db.execute('INSERT INTO binding VALUES(?,?)', (box.endpoint, box.producer))
            db.execute(f'PRAGMA application_id={APPLICATION_ID}')
            db.execute('PRAGMA user_version=1')
        elif version != 1 or application != APPLICATION_ID:
            raise InvalidData('Unsupported or foreign missing-media cache')
        bindings = db.execute('SELECT endpoint,producer FROM binding').fetchall()
        if len(bindings) != 1 or tuple(bindings[0]) != (box.endpoint, box.producer):
            raise InvalidData('Missing-media cache identifies a different producer')
        yield db
        db.commit()
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()

def target(category, url, metadata):
    if category != 'reddit':
        return None
    data = retain(dict(metadata, category='reddit'))
    if data.get('comment'):
        return None
    media = data
    parents = media.get('crosspost_parent_list')
    if parents:
        if not isinstance(parents, list) or not isinstance(parents[-1], dict):
            return None
        media = parents[-1]
    ref, requested = reddit_external.target(media), reddit_external.linked(url)
    if ref is None or requested is None or ref != requested[0] or media.get('comment'):
        return None
    parsed = urlsplit(url)
    if ref['namespace'] == 'native:imgur':
        service, item = 'imgur', ref['value']
    elif parsed.hostname == 'i.imgur.com':
        match = re.fullmatch(r'/([A-Za-z0-9]{5}|[A-Za-z0-9]{7})[sbtmlh]?\.[A-Za-z0-9]+', parsed.path)
        if not match:
            return None
        service, item = 'imgur', match[1]
    elif ref['namespace'] == 'native:redgifs':
        service, item = 'redgifs', ref['value']
    else:
        item = reddit_external.redgifs_image_id(url)
        if not item:
            return None
        service = 'redgifs'
    return {'service': service, 'item': item, 'attachment': ref, 'metadata': data}


def matches(error, url, bound, extractor):
    return getattr(error, 'status', None) in (404, 410) and request_matches(url, bound, extractor)


def request_matches(url, bound, extractor):
    if bound is None or extractor.category != bound['service'] or extractor.subcategory != 'image':
        return False
    parsed = urlsplit(url)
    host, paths = (('api.imgur.com', ('/post/v1/media/', '/3/image/')) if bound['service'] == 'imgur'
                   else ('api.redgifs.com', ('/v2/gifs/',)))
    return (parsed.scheme == 'https' and parsed.netloc == host and not parsed.query
            and parsed.path in {prefix + bound['item'] for prefix in paths})


def status(row):
    return {'state': 'skipped', 'reason': 'not_found', 'service': row['service'], 'item': row['item'],
            'attempts': row['attempts'], 'first_seen': row['first_seen'], 'observed_at': row['last_seen'],
            'http_status': row['http_status']}


def cached(box, bound):
    if bound is None:
        return None
    with journal(box) as db:
        row = db.execute('SELECT * FROM missing_media WHERE service=? AND item=? AND attempts=?',
                         (bound['service'], bound['item'], MAX_ATTEMPTS)).fetchone()
        return status(row) if row else None


def failed(box, bound, http_status):
    with journal(box) as db:
        now = utc_now()
        db.execute('''INSERT INTO missing_media(service,item,attempts,first_seen,last_seen,http_status)
            VALUES(?,?,1,?,?,?) ON CONFLICT(service,item) DO UPDATE SET
            attempts=min(3,attempts+1),last_seen=excluded.last_seen,http_status=excluded.http_status''',
                       (bound['service'], bound['item'], now, now, http_status))
        row = db.execute('SELECT * FROM missing_media WHERE service=? AND item=?',
                         (bound['service'], bound['item'])).fetchone()
        return status(row) if row['attempts'] == MAX_ATTEMPTS else None


def recovered(box, bound):
    with journal(box) as db:
        db.execute('DELETE FROM missing_media WHERE service=? AND item=? AND attempts<?',
                       (bound['service'], bound['item'], MAX_ATTEMPTS))
