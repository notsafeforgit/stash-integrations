"""Bounded, resumable registration of equal-byte files before native dedupe.

No file is deleted here. Native intake enforces removal fences and unique media
ownership; native dedupe independently proves bytes before a merge/removal.
"""

import hashlib
import os
from pathlib import Path
import stat
import uuid

from .client import Unavailable
from .dedupe import journal_database
from .dedupe_client import MEDIA_POLICY
from .encoding import InvalidData, decode, encode
from .intake_client import saved_request, status

MAX_PENDING = 8


class Review(Exception):
    def __init__(self, reason):
        self.reason = reason


def media_kind(path):
    suffix = Path(path).suffix.lower()
    if suffix in {'.jpg', '.jpeg', '.png', '.webp', '.avif'}:
        return 'image'
    if suffix in {'.mp4', '.mkv', '.webm', '.mov', '.m4v', '.avi', '.wmv'}:
        return 'scene'
    raise Review('intake_requires_review')


def fingerprint(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def equal_files(root, pair, check):
    """Bound every path and recheck descriptors after hashing; hints are not proof."""
    digests, snapshots = [], []
    for key in ('keep_path', 'remove_path'):
        check()
        path = root / pair[key]
        if path.resolve(strict=True) != path or not path.is_relative_to(root):
            raise Review('intake_requires_review')
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as stream:
            before = os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size <= 0:
                raise Review('different_or_empty_files')
            digest = hashlib.sha256()
            while body := stream.read(1 << 20):
                check()
                digest.update(body)
            if fingerprint(os.fstat(stream.fileno())) != fingerprint(before) or fingerprint(path.lstat()) != fingerprint(before):
                raise Review('file_requires_rescan')
        digests.append(digest.digest())
        snapshots.append(fingerprint(before))
    if digests[0] != digests[1] or snapshots[0][2] != snapshots[1][2]:
        raise Review('different_or_empty_files')
    if snapshots[0][:2] == snapshots[1][:2]:
        raise Review('same_filesystem_entry')
    return snapshots


class Registration:
    def __init__(self, journal, client, root):
        self.journal, self.client = journal, client
        self.root = Path(root).resolve(strict=True)
        self.collection = journal.config['intake_collection_uuid']
        self.waiting = False

    def load(self, path):
        with journal_database(self.journal.directory) as db:
            row = db.execute('SELECT length(body),length(result) FROM registrations WHERE run_uuid=? AND path=?',
                             (self.journal.run_uuid, path)).fetchone()
            if row is None:
                return None
            if row[0] > 8192 or row[1] is not None and row[1] > 65536:
                raise InvalidData('Duplicate intake journal exceeds its bounds')
            body, result = db.execute('SELECT body,result FROM registrations WHERE run_uuid=? AND path=?',
                                     (self.journal.run_uuid, path)).fetchone()
        request = saved_request(decode(body, 8192))
        if (request['relative_path'] != path or request['collection_uuid'] != self.collection
                or request.get('deduplication_policy') != MEDIA_POLICY):
            raise InvalidData('Duplicate intake changed its saved scope')
        result = decode(result, 65536) if result is not None else None
        if result is not None and result != {'review': 'intake_preview_changed'}:
            status(result, request)
        return request, result

    def save(self, request, result=None):
        path = request['relative_path']
        with journal_database(self.journal.directory) as db:
            self.journal.guard(db)
            body = encode(saved_request(request), 8192)
            if result is None:
                db.execute('INSERT INTO registrations(run_uuid,path,body) VALUES(?,?,?)',
                           (self.journal.run_uuid, path, body))
            else:
                cursor = db.execute('UPDATE registrations SET result=? WHERE run_uuid=? AND path=? AND body=?',
                                    (encode(result, 65536), self.journal.run_uuid, path, body))
                if cursor.rowcount != 1:
                    raise InvalidData('Duplicate intake request changed')

    def recover(self, pair, check):
        ready = True
        for path in (pair['keep_path'], pair['remove_path']):
            saved = self.load(path)
            if saved is None:
                continue
            request, result = saved
            if result and result.get('review'):
                raise Review(result['review'])
            if result is None or result['state'] in ('queued', 'running'):
                check()
                result = self.client.receipt(request)
                if result is None:
                    check()
                    try:
                        result = self.client.apply(request)
                    except Unavailable as error:
                        if error.status != 409 or error.code != 'intake_preview_changed':
                            raise
                        self.save(request, {'review': error.code})
                        raise Review(error.code) from None
                self.save(request, result)
            if result['state'] in ('failed', 'cancelled'):
                raise Review('intake_failed')
            if result['state'] != 'succeeded':
                ready, self.waiting = False, True
        return ready

    def prepare(self, pair, check):
        # Do not retain thousands of jobs behind an intake queue. A later bounded
        # maintenance turn recovers these requests before admitting more.
        with journal_database(self.journal.directory) as db:
            pending = db.execute("SELECT count(*) FROM registrations WHERE run_uuid=? AND (result IS NULL OR json_extract(result,'$.state') IN ('queued','running'))",
                                 (self.journal.run_uuid,)).fetchone()[0]
        if pending >= MAX_PENDING:
            self.waiting = True
            return False
        paths = (pair['keep_path'], pair['remove_path'])
        kinds = [media_kind(path) for path in paths]
        if kinds[0] != kinds[1]:
            raise Review('intake_requires_review')
        try:
            snapshots = equal_files(self.root, pair, check)
        except FileNotFoundError:
            raise Review('file_not_found') from None
        for path, kind, snapshot in zip(paths, kinds, snapshots):
            if self.load(path) is not None:
                continue
            check()
            try:
                preview = self.client.preview({'collection_uuid': self.collection, 'relative_path': path,
                                               'media_kind': kind, 'deduplication_policy': MEDIA_POLICY}, pair['root_uuid'])
            except Unavailable as error:
                if error.status in (404, 409):
                    raise Review('intake_requires_review') from None
                raise
            if preview['size'] != snapshot[2] or fingerprint((self.root / path).lstat()) != snapshot:
                raise Review('file_requires_rescan')
            if preview.get('existing_file_uuid'):
                continue
            if type(preview.get('saved_post_count')) is not int or preview['saved_post_count'] < 1:
                raise Review('saved_metadata_required')
            request = {k: preview[k] for k in ('collection_uuid', 'relative_path', 'media_kind', 'signature', 'deduplication_policy')}
            request['request_uuid'] = str(uuid.uuid4())
            self.save(request)  # durable intent before the first admission
            # One publication at a time within a pair lets native verified-byte
            # identity select the first owner for the second copy.
            return self.recover(pair, check) and self.prepare(pair, check)
        return True
