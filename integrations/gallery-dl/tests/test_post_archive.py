"""Completion markers are durable, whole-post decisions in the existing archive."""

from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

from gallery_dl import archive as gdl_archive, config, extractor
from gallery_dl.extractor import reddit
from gallery_dl.extractor.common import Message
from stash_ingest import post_archive
from stash_ingest.gallery import NativeDownloadJob
from stash_ingest.outbox import Outbox
from helpers import PRODUCER


class PostArchiveTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.box = Outbox(self.root / 'outbox.sqlite', 'http://example.invalid', PRODUCER)
        self.addCleanup(self.box.close)

    def archive(self, mode=None):
        archive = gdl_archive.connect(str(self.root / 'archive.sqlite'), 'reddit', '{filename}', mode=mode)
        self.addCleanup(archive.close)
        return archive

    def test_album_requires_every_attachment_or_explicit_unavailable_status(self):
        archive = self.archive()
        data = {'id': 'abc123', 'is_gallery': True, 'gallery_data': {'items': [{'media_id': 'one'}, {'media_id': 'two'}]}}
        post = post_archive.Post(data, archive)
        post.completed({'namespace': 'native:reddit', 'value': 'one'})
        post.finish(self.box)
        self.assertFalse(post_archive.contains(archive, 'abc123'))
        post.completed({'namespace': 'native:reddit', 'value': 'two'})
        post.finish(self.box)
        self.assertTrue(post_archive.contains(archive, 'abc123'))
        data['id'] = 'def456'
        data['media_metadata'] = {'two': {'status': 'failed'}}
        post = post_archive.Post(data, archive)
        post.completed({'namespace': 'native:reddit', 'value': 'one'})
        post.finish(self.box)
        self.assertTrue(post_archive.contains(archive, 'def456'))

    def test_pending_retry_prevents_completion_even_if_other_files_are_archived(self):
        archive = self.archive()
        post = post_archive.Post({'id': 'abc123'}, archive)
        post.completed({'namespace': 'native:reddit', 'value': 'one'})
        post.pending = True
        post.finish(self.box)
        self.assertFalse(post_archive.contains(archive, 'abc123'))

    def test_memory_archive_marker_is_committed_before_acknowledgement(self):
        archive = self.archive('memory')
        post_archive.record(archive, 'abc123')
        with closing(sqlite3.connect(self.root / 'archive.sqlite')) as db:
            self.assertEqual(db.execute('SELECT entry FROM archive').fetchall(), [('stash:completed:reddit:abc123',)])

    def test_missing_cache_retired_only_after_archive_commit(self):
        archive = self.archive()
        self.box.db.execute("INSERT INTO missing_media VALUES('imgur','missing',3,'2026-11-03T00:00:00+00:00','2026-11-03T00:00:00+00:00',404)")
        post = post_archive.Post({'id': 'abc123'}, archive)
        post.missing('imgur', 'missing', None)
        with patch('stash_ingest.post_archive.record', side_effect=sqlite3.OperationalError('disk full')):
            with self.assertRaises(sqlite3.OperationalError):
                post.finish(self.box)
        self.assertEqual(self.box.db.execute('SELECT count(*) FROM missing_media').fetchone()[0], 1)
        post.finish(self.box)
        self.assertEqual(self.box.db.execute('SELECT count(*) FROM missing_media').fetchone()[0], 0)
        self.assertEqual(self.box.db.execute('SELECT count(*) FROM events').fetchone()[0], 0)

    def test_cache_expiry_does_not_accumulate_old_missing_records(self):
        self.box.db.execute("INSERT INTO missing_media VALUES('imgur','old',3,'2000-01-01T00:00:00Z','2000-01-01T00:00:00Z',404)")
        self.box.db.execute("INSERT INTO deleted_media VALUES('redgifs','old','2000-01-01T00:00:00Z',410,1)")
        post_archive.prune_old_cache(self.box)
        for table in ('missing_media', 'deleted_media'):
            self.assertEqual(self.box.db.execute('SELECT count(*) FROM ' + table).fetchone()[0], 0)

    def test_recursive_post_lookup_honors_durable_completion(self):
        archive = self.archive()
        post_archive.record(archive, 'reviewedgallery')
        fetch = Mock(return_value=({'id': 'available'}, ()))
        target = SimpleNamespace(api=SimpleNamespace(submission=fetch), submissions=lambda: ())
        post_archive.install_recursive_lookup(target, lambda: archive)
        target.submissions()
        self.assertFalse(target.api.submission('reviewedgallery')[0]['_media'])
        fetch.assert_not_called()
        self.assertEqual(target.api.submission('available'), ({'id': 'available'}, ()))
        fetch.assert_called_once_with('available')
        target.submissions()
        self.assertEqual(target.api.submission('reviewedgallery')[0]['_media'], False)
        self.assertEqual(fetch.call_count, 1)

    def test_recursive_lookup_before_archive_initialization_fetches_normally(self):
        fetch = Mock(return_value=({'id': 'initial'}, ()))
        target = SimpleNamespace(api=SimpleNamespace(submission=fetch), submissions=lambda: ())
        post_archive.install_recursive_lookup(target, lambda: None)
        target.submissions()
        self.assertEqual(target.api.submission('initial'), ({'id': 'initial'}, ()))
        fetch.assert_called_once_with('initial')

    def test_upstream_crosspost_recursion_skips_archived_original_and_continues(self):
        archive = self.archive()
        post_archive.record(archive, 'abc123')
        for parent_directory in (False, True):
            with self.subTest(parent_directory=parent_directory):
                config.clear()
                self.addCleanup(config.clear)
                config.set(('extractor', 'reddit'), 'recursion', 1)
                config.set(('extractor', 'reddit'), 'parent-directory', parent_directory)
                target = extractor.find('https://www.reddit.com/user/fixture/')
                base = {'created_utc': 1700000000, 'is_video': False, 'is_self': False}
                target.submissions = lambda: iter([
                    (dict(base, id='crosspost', url='https://www.reddit.com/gallery/abc123'), ()),
                    (dict(base, id='nextpost', url='https://www.reddit.com/gallery/def456'), ()),
                ])
                fetch = Mock(return_value=(dict(base, id='def456', url='https://i.redd.it/available.jpg'), ()))
                api = SimpleNamespace(comments=False, submission=fetch)
                current = [None]
                post_archive.install_recursive_lookup(target, lambda: current[0])
                with patch.object(reddit, 'RedditAPI', return_value=api):
                    messages = []
                    for message in target.items():
                        current[0] = archive
                        messages.append(message)
                fetch.assert_called_once_with('def456')
                media = [message[1] for message in messages if message[0] == Message.Url]
                self.assertEqual(media, ['https://i.redd.it/available.jpg'])
                skipped = [message[2] for message in messages if message[2].get('_native_archive_complete')]
                self.assertEqual(len(skipped), int(parent_directory))
                for directory in skipped:
                    NativeDownloadJob.handle_directory.__wrapped__(object.__new__(NativeDownloadJob), directory)
