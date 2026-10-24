"""Real Erome extractor output through the native download and retry boundary."""
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gallery_dl import config
from gallery_dl.extractor.erome import EromeAlbumExtractor
from stash_ingest import source, web_media
from stash_ingest.encoding import InvalidData
from stash_ingest.source_window import SourceWindow, published, validate_keywords
import test_gallery

URLS = ['https://cdn.example/first.jpeg', 'https://cdn.example/movie.mp4', 'https://cdn.example/last.jpeg']


def page(urls=URLS):
    html = '<meta property="og:title" content="Example album"><div class="user-profile"><a href="https://www.erome.com/example">'
    for url in urls:
        html += '<div class="media-group">'
        html += f'<source src="{url}">' if url.endswith('.mp4') else f'<img data-src="{url}">'
        html += '</div>'
    return html


class EromeGalleryTests(unittest.TestCase):
    def setUp(self):
        test_gallery.GalleryTests.setUp(self)
        self.lease.run['target_url'] = 'https://www.erome.com/a/Example1'
        self.producer.window = SourceWindow({'since': None, 'until': '2026-10-23T00:00:00Z', 'basis': 'traversal'})
        config.set(('extractor',), 'filename', None)

    task = test_gallery.GalleryTests.task
    events = test_gallery.GalleryTests.events
    archive_count = test_gallery.GalleryTests.archive_count

    def test_mixed_album_order_and_reordered_retry_have_stable_attachments(self):
        with patch.object(EromeAlbumExtractor, 'request', return_value=SimpleNamespace(text=page())):
            task = self.task(fixture=EromeAlbumExtractor)
            self.assertEqual(task.run(), 0)
        captures = self.events(kinds=('source.capture',))
        self.assertEqual(len(captures), 3)
        self.assertEqual(len(self.events(kinds=('file.completed',))), 3)
        self.assertEqual(self.archive_count(), 3)
        marker = captures[0]['source']['web_media']
        self.assertTrue(marker['album'])
        self.assertFalse(marker['complete'])
        self.assertEqual([i['url'] for i in marker['items']], URLS)
        self.assertEqual([i['kind'] for i in marker['items']], ['image', 'video', 'image'])
        for number, capture in enumerate(captures, 1):
            self.assertEqual(capture['source']['num'], number)
            self.assertEqual(capture['source']['web_media'], marker)
            self.assertEqual(capture['metadata']['title'], 'Example album')
            self.assertNotIn('published_at', capture['metadata'])
            self.assertEqual(capture['post'], {'namespace':'native:erome', 'value':'Example1'})
        identities = {c['source']['web_media_url']: source.attachment(c['source']) for c in captures}
        with patch.object(EromeAlbumExtractor, 'request', return_value=SimpleNamespace(text=page(list(reversed(URLS))))):
            retry = self.task(fixture=EromeAlbumExtractor)
            retry.download = lambda _: self.fail('An archived source attachment must not download again after reordering')
            self.assertEqual(retry.run(), 0)
        self.assertEqual(self.archive_count(), 3)
        for capture in self.events(kinds=('source.capture',))[3:]:
            self.assertEqual(source.attachment(capture['source']), identities[capture['source']['web_media_url']])

    def test_replaced_album_slot_gets_new_bytes_and_preserves_original_file(self):
        with patch.object(EromeAlbumExtractor, 'request', return_value=SimpleNamespace(text=page())):
            self.assertEqual(self.task(fixture=EromeAlbumExtractor).run(), 0)
        before = self.events(kinds=('file.completed',))
        original_paths = {event['relative_path'] for event in before}
        replacement = 'https://cdn.example/new.jpeg'
        with patch.object(EromeAlbumExtractor, 'request', return_value=SimpleNamespace(text=page([replacement, *URLS[1:]]))):
            retry = self.task(fixture=EromeAlbumExtractor)
            download = retry.download
            fetched = []
            def changed_slot(url):
                fetched.append(url)
                return download(url)
            retry.download = changed_slot
            self.assertEqual(retry.run(), 0)
        self.assertEqual(fetched, [replacement])
        self.assertEqual(self.archive_count(), 4)
        after = self.events(kinds=('file.completed',))[len(before):]
        self.assertEqual(len(after), 3)
        self.assertNotIn(after[0]['relative_path'], original_paths)
        self.assertTrue(all((self.media / path).is_file() for path in original_paths))

    def test_failed_file_is_not_archived_and_retry_fetches_only_missing_file(self):
        with patch.object(EromeAlbumExtractor, 'request', return_value=SimpleNamespace(text=page())):
            task = self.task(fixture=EromeAlbumExtractor)
            download = task.download
            task.download = lambda url: False if url == URLS[1] else download(url)
            self.assertNotEqual(task.run(), 0)
        self.assertEqual(self.archive_count(), 2)
        self.assertEqual(len(self.events(kinds=('file.completed',))), 2)
        with patch.object(EromeAlbumExtractor, 'request', return_value=SimpleNamespace(text=page())):
            retry = self.task(fixture=EromeAlbumExtractor)
            download = retry.download
            fetched = []
            def only_missing(url):
                fetched.append(url)
                return download(url)
            retry.download = only_missing
            self.assertEqual(retry.run(), 0)
        self.assertEqual(fetched, [URLS[1]])
        self.assertEqual(self.archive_count(), 3)

    def test_duplicate_or_empty_album_never_publishes_partial_success(self):
        for urls in ([], [URLS[0], URLS[0]]):
            with self.subTest(urls=urls), patch.object(EromeAlbumExtractor, 'request', return_value=SimpleNamespace(text=page(urls))):
                task = self.task(fixture=EromeAlbumExtractor)
                self.assertNotEqual(task.run(), 0)
        self.assertEqual(self.events(), [])

    def test_source_time_and_identity_cannot_be_invented_by_configuration(self):
        with self.assertRaises(InvalidData):
            published({'album_id':'Example1', 'date':'2026-10-01T00:00:00Z'}, 'erome')
        self.assertNotIn('published_at', source.metadata({'category':'erome', 'date':'2026-10-01T00:00:00Z'}))
        for name in ('album_id', 'web_media_id', 'count'):
            config.set(('extractor',), 'keywords', {name:'replacement'})
            with self.assertRaises(InvalidData):
                validate_keywords(EromeAlbumExtractor.from_url(self.lease.run['target_url']))
        config.set(('extractor',), 'keywords', None)
        for filename in ('{album_id}_{num}.{extension}', {'': '{filename}.{extension}'}):
            config.set(('extractor',), 'filename', filename)
            with self.assertRaises(InvalidData):
                web_media.install_erome(EromeAlbumExtractor.from_url(self.lease.run['target_url']))
        config.set(('extractor',), 'filename', None)
        for archive_format in ('{album_id}_{num}', '{web_media_id}'):
            config.set(('extractor',), 'archive-format', archive_format)
            with self.assertRaises(InvalidData):
                web_media.install_erome(EromeAlbumExtractor.from_url(self.lease.run['target_url']))
