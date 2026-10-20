"""Bounded retries for a known external attachment, never a missing account.

Only exact single-item Imgur/Redgifs API 404/410 responses qualify. After
three observations the producer retains a skip decision across restarts.
Authentication, listing, rate-limit and transport failures remain retryable.
"""

import re
from urllib.parse import urlsplit

from . import reddit_external
from .encoding import utc_now
from .retention import retain

MAX_ATTEMPTS = 3


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
    with box.transaction():
        db = box.db
        row = db.execute('SELECT * FROM missing_media WHERE service=? AND item=? AND attempts=?',
                         (bound['service'], bound['item'], MAX_ATTEMPTS)).fetchone()
        return status(row) if row else None


def failed(box, bound, http_status):
    with box.transaction():
        db = box.db
        now = utc_now()
        db.execute('''INSERT INTO missing_media(service,item,attempts,first_seen,last_seen,http_status)
            VALUES(?,?,1,?,?,?) ON CONFLICT(service,item) DO UPDATE SET
            attempts=min(3,attempts+1),last_seen=excluded.last_seen,http_status=excluded.http_status''',
                       (bound['service'], bound['item'], now, now, http_status))
        row = db.execute('SELECT * FROM missing_media WHERE service=? AND item=?',
                         (bound['service'], bound['item'])).fetchone()
        return status(row) if row['attempts'] == MAX_ATTEMPTS else None


def recovered(box, bound):
    with box.transaction():
        db = box.db
        db.execute('DELETE FROM missing_media WHERE service=? AND item=? AND attempts<?',
                       (bound['service'], bound['item'], MAX_ATTEMPTS))
