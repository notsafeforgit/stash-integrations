"""Keep finished Reddit posts in the downloader archive, without a missing-file ledger."""

from datetime import datetime
import re

from .encoding import InvalidData
from . import source

PREFIX = 'stash:completed:reddit:'
IDENTITY = re.compile(r'[a-z0-9]{1,32}')


def key(identity):
    if not isinstance(identity, str) or not IDENTITY.fullmatch(identity):
        raise InvalidData('Completed archive post has no Reddit identity')
    return PREFIX + identity


def contains(archive, identity):
    if archive is None:
        return False
    archive.cursor.execute(archive._stmt_select, (key(identity),))
    return bool(archive.cursor.fetchone())


def record(archive, identity):
    if archive is None:
        raise InvalidData('Finishing a post requires its configured download archive')
    archive.add({archive._cache_key: key(identity)})
    # Memory-mode archives must also commit before native work can finish.
    archive.finalize()
    archive.connection.commit()
    if not contains(archive, identity):
        raise InvalidData('Completed post archive write was not durable')


def install_recursive_lookup(target, current_archive):
    """Respect completed posts when Reddit follows a crosspost's original."""
    original = getattr(target, 'submissions', None)
    if original is None:
        return

    def submissions():
        api = target.api
        if not getattr(api, '_native_completed_posts', False):
            api._native_completed_posts = True
            fetch = api.submission

            def submission(identity):
                if contains(current_archive(), identity):
                    # gallery-dl consumes this internal no-media result without
                    # making a request or reporting a successful media fetch.
                    skipped = {'id': identity, '_media': False, '_native_archive_complete': True}
                    # With parent-directory enabled, upstream emits the first
                    # comment as a directory even for an empty-media result.
                    return skipped, (skipped,)
                return fetch(identity)

            api.submission = submission
        return original()

    target.submissions = submissions


def forget(box, entries):
    with box.transaction():
        for service, item in entries:
            box.db.execute('DELETE FROM missing_media WHERE service=? AND item=?', (service, item))
            if service == 'redgifs':
                box.db.execute('DELETE FROM deleted_media WHERE service=? AND item=?', (service, item))


def prune_old_cache(box):
    # These are transient observations, not a permanent list of missing media.
    cutoff = box.clock() - 48 * 60 * 60
    with box.transaction():
        box.db.execute('DELETE FROM missing_media WHERE unixepoch(last_seen)<=?', (cutoff,))
        box.db.execute('DELETE FROM deleted_media WHERE unixepoch(observed_at)<=?', (cutoff,))


class Post:
    def __init__(self, data, archive):
        self.data = dict(data)
        self.identity = data.get('id')
        key(self.identity)
        self.archive = archive
        self.already = contains(archive, self.identity)
        self.pending = False
        self.handled = False
        self.terminal = set()
        self.cache = set()
        media = data
        parents = data.get('crosspost_parent_list')
        if isinstance(parents, list) and parents and isinstance(parents[-1], dict):
            media = parents[-1]
        # Reddit explicitly omits unavailable gallery members from Url messages.
        # Their declared failed status is terminal; unknown omissions are not.
        metadata = media.get('media_metadata')
        if isinstance(metadata, dict):
            for identity, entry in metadata.items():
                if isinstance(entry, dict) and entry.get('status') == 'failed':
                    self.terminal.add(('native:reddit', identity))
                    self.handled = True

    def completed(self, attachment):
        self.handled = True
        self.terminal.add((attachment['namespace'], attachment['value']))

    def missing(self, service, item, attachment):
        self.handled = True
        self.cache.add((service, item))
        if attachment:
            self.completed(attachment)

    def finish(self, box):
        if self.already or self.archive is None or self.pending or not self.handled:
            return
        from .recent import complete_manifest
        if not complete_manifest(self.data, 'reddit', self.terminal):
            return
        record(self.archive, self.identity)
        forget(box, self.cache)
        self.already = True


def retry_archive(producer, retained_source, *, finish=False):
    """Check/finalize the original archive without any website or media request."""
    from gallery_dl import archive as gdl_archive, extractor, path
    from . import missing_media

    producer.check(turn=False)
    if not isinstance(retained_source, dict):
        raise InvalidData('Post retry has no retained archive context')
    data, category = source._context(retained_source)
    identity = source.post(retained_source)
    expected = 'https://www.reddit.com/comments/' + identity['value'] + '/'
    if category != 'reddit' or producer.lease.run.get('retrieval_url') != expected:
        raise InvalidData('Post retry archive context does not match its retrieval')
    target = data.get('source_extractor_url') or expected
    selected = extractor.find(target)
    if selected is None or selected.category != 'reddit':
        raise InvalidData('Post retry archive context is not a Reddit extractor')
    values = dict(data, category='reddit', subcategory=selected.subcategory)
    if isinstance(values.get('date'), str):
        try:
            values['date'] = datetime.fromisoformat(values['date'].replace('Z', '+00:00'))
        except ValueError:
            raise InvalidData('Post retry archive date is invalid') from None
    pathfmt = path.PathFormat(selected)
    pathfmt.set_directory(values)
    producer.relative(pathfmt.realdirectory)
    cfg = selected.config
    archive_path = cfg('archive')
    if not archive_path:
        raise InvalidData('Post retry requires its configured download archive')
    table = cfg('archive-table')
    prefix = cfg('archive-prefix')
    if prefix is None:
        prefix = selected.category if table is None else ''
    archive_format = cfg('archive-format')
    if archive_format is None:
        archive_format = selected.archive_fmt
    archive = gdl_archive.connect(archive_path, prefix, archive_format,
                                  table, cfg('archive-mode'), cfg('archive-reuse'),
                                  cfg('archive-pragma'), pathfmt)
    try:
        archived = contains(archive, identity['value'])
        if finish and not archived:
            record(archive, identity['value'])
            archived = True
        if archived:
            target = missing_media.target('reddit', data.get('url', ''), data)
            if target:
                forget(producer.outbox, {(target['service'], target['item'])})
        return archived
    finally:
        archive.close()
