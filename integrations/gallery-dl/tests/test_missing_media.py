import json
from contextlib import closing
from unittest.mock import patch

from gallery_dl import config
from gallery_dl.extractor.imgur import ImgurImageExtractor
from gallery_dl.extractor.common import Message
from requests import Response

from stash_ingest import missing_media
from stash_ingest.gallery import NativeDownloadJob
from stash_ingest.outbox import Outbox
from stash_ingest.runs import SourceFailure
import test_gallery
from test_producer import reddit_data


class LinkedImgurFixture(test_gallery.Fixture):
    def items(self):
        for item in self.records:
            data = dict(item)
            yield Message.Directory, '', data
            yield Message.Queue, data['url'], {**data, '_extractor': ImgurImageExtractor}


class MissingMediaTests(test_gallery.unittest.TestCase):
    setUp = test_gallery.GalleryTests.setUp
    task = test_gallery.GalleryTests.task
    events = test_gallery.GalleryTests.events
    archive_count = test_gallery.GalleryTests.archive_count

    def missing_count(self):
        with self.box.transaction():
            db = self.box.db
            return db.execute('SELECT count(*) FROM missing_media').fetchone()[0]

    def run_posts(self, codes):
        calls = []
        dead = reddit_data('missing', url='https://i.imgur.com/Missing.jpg')
        good = reddit_data('present', url='https://i.imgur.com/Present.jpg')

        def request(session, method, url, **kwargs):
            calls.append(url)
            response = Response()
            response.url = url
            response.status_code = codes.pop(0) if url.endswith('/Missing') else 200
            response._content_consumed = True
            response._content = json.dumps({'id': 'Present' if url.endswith('/Present') else 'Missing',
                'metadata': {}, 'media': [{'id': 'Present' if url.endswith('/Present') else 'Missing',
                    'ext': 'jpg', 'is_animated': False, 'created_at': '2026-10-01T00:00:00Z',
                    'metadata': {}}]}).encode()
            return response

        def download(child, url):
            child.pathfmt.part_enable()
            with child.pathfmt.open('wb') as output:
                output.write(b'available image')
            return True

        config.set(('extractor', 'reddit'), 'parent-metadata', '_reddit')
        config.set(('extractor',), 'retries', 0)
        with patch('requests.sessions.Session.request', request), patch.object(NativeDownloadJob, 'download', download):
            status = self.task(dead, good, fixture=LinkedImgurFixture).run()
        return status, calls

    def test_missing_item_retries_three_times_then_next_post_downloads_and_restart_keeps_skip(self):
        status, calls = self.run_posts([404, 404, 410])
        self.assertEqual(status, 0)
        self.assertEqual(sum(u.endswith('/Missing') for u in calls), 3)
        self.assertIsNone(self.producer.source_failure)
        events = self.events(kinds=None)
        skipped = [e for e in events if e['kind'] == 'source.capture' and e['source'].get('native_media_status')]
        self.assertEqual(len(skipped), 1)
        self.assertEqual(skipped[0]['source']['native_media_status']['state'], 'skipped')
        self.assertEqual(skipped[0]['source']['native_media_status']['attempts'], 3)
        failures = [e for e in events if e['kind'] == 'attachment.download' and e['state'] == 'failed']
        self.assertEqual(len(failures), 1)
        self.assertEqual(len(self.events(kinds=('file.completed',))), 1)
        self.assertEqual(self.archive_count(), 1, 'a skip is never a successful download')
        bound = missing_media.target('reddit', 'https://i.imgur.com/Missing.jpg', reddit_data(url='https://i.imgur.com/Missing.jpg'))
        with closing(Outbox(self.box.path, self.box.endpoint, self.box.producer)) as reopened:
            self.assertEqual(missing_media.cached(reopened, bound)['attempts'], 3)
        status, calls = self.run_posts([])
        self.assertEqual(status, 0)
        self.assertFalse(any(u.endswith('/Missing') for u in calls))

    def test_success_before_limit_downloads_and_clears_failed_observations(self):
        status, calls = self.run_posts([404, 200])
        self.assertEqual(status, 0)
        self.assertEqual(sum(u.endswith('/Missing') for u in calls), 2)
        self.assertEqual(self.missing_count(), 0)
        self.assertEqual(len(self.events(kinds=('file.completed',))), 2)

    def test_authentication_rate_limit_and_server_errors_are_not_skipped(self):
        for code, expected in [(403, 'access_denied'), (429, 'rate_limited'), (500, 'extraction_failed')]:
            with self.subTest(code=code):
                self.producer.source_failure = None
                with self.assertRaises(SourceFailure) as failure:
                    self.run_posts([code])
                self.assertEqual(failure.exception.code, expected)
                self.assertEqual(self.missing_count(), 0)

    def test_only_exact_single_item_api_request_is_skippable(self):
        from gallery_dl import exception
        from types import SimpleNamespace
        bound = missing_media.target('reddit', 'https://imgur.com/Missing', reddit_data(url='https://imgur.com/Missing'))
        extractor = SimpleNamespace(category='imgur', subcategory='image')
        response = Response(); response.status_code = 404
        error = exception.HttpError(response=response)
        self.assertTrue(missing_media.matches(error, 'https://api.imgur.com/post/v1/media/Missing', bound, extractor))
        for url in ['https://api.imgur.com/post/v1/media/Another', 'https://api.imgur.com/oauth2/token',
                    'https://api.imgur.com/post/v1/albums/Missing', 'https://foreign.invalid/post/v1/media/Missing']:
            self.assertFalse(missing_media.matches(error, url, bound, extractor))
        extractor.subcategory = 'user'
        self.assertFalse(missing_media.matches(error, 'https://api.imgur.com/post/v1/media/Missing', bound, extractor))
        album = missing_media.target('reddit', 'https://imgur.com/a/Missing', reddit_data(url='https://imgur.com/a/Missing'))
        self.assertEqual(album['item'], 'album:Missing')
        self.assertIsNone(album['attachment'])
        self.assertFalse(missing_media.matches(error, 'https://api.imgur.com/post/v1/media/Missing', album, extractor))
        self.assertIsNone(missing_media.target('reddit', 'https://imgur.com/Another', reddit_data(url='https://imgur.com/Missing')))
        self.assertIsNone(missing_media.target('imgur', 'https://imgur.com/Missing', reddit_data(url='https://imgur.com/Missing')))

    def test_partial_retry_count_uses_outbox_and_survives_restart(self):
        bound = missing_media.target('reddit', 'https://imgur.com/Missing', reddit_data(url='https://imgur.com/Missing'))
        self.assertIsNone(missing_media.failed(self.box, bound, 404))
        self.assertIsNone(missing_media.failed(self.box, bound, 404))
        with closing(Outbox(self.box.path, self.box.endpoint, self.box.producer)) as reopened:
            self.assertEqual(missing_media.failed(reopened, bound, 410)['attempts'], 3)
        self.assertEqual(self.box.db.execute('PRAGMA user_version').fetchone()[0], 21)
        self.assertFalse(self.box.path.with_name(self.box.path.name + '.missing-media.sqlite').exists())
