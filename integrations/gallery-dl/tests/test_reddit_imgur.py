import copy
import json
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from gallery_dl import config
from gallery_dl.extractor.common import Message
from gallery_dl.extractor.imgur import ImgurAlbumExtractor, ImgurGalleryExtractor, ImgurImageExtractor
from requests import Response

from stash_ingest import missing_media, source
from stash_ingest.encoding import InvalidData
from stash_ingest.gallery import NativeDownloadJob
from stash_ingest.outbox import Outbox
from stash_ingest.retention import retain
from stash_ingest.runs import SourceFailure
import test_gallery
from test_producer import reddit_data


def album(identity='Album12'):
    return {'id': identity, 'is_album': True, 'image_count': 2,
            'created_at': '2020-01-01T00:00:00Z', 'title': 'External album title',
            'media': [{'id': identity, 'ext': extension, 'is_animated': extension == 'gif',
                       'created_at': '2020-01-01T00:00:00Z', 'metadata': {}}
                      for identity, extension in [('First12', 'jpeg'), ('Next123', 'gif')]]}


class LinkedImgurFixture(test_gallery.Fixture):
    def items(self):
        for item in self.records:
            data = copy.deepcopy(item)
            yield Message.Directory, '', data
            cls = (ImgurGalleryExtractor if '/gallery/' in data['url'] else
                   ImgurAlbumExtractor if '/a/' in data['url'] else ImgurImageExtractor)
            yield Message.Queue, data['url'], {**data, '_extractor': cls}


class RedditImgurTests(test_gallery.unittest.TestCase):
    setUp = test_gallery.GalleryTests.setUp
    task = test_gallery.GalleryTests.task
    events = test_gallery.GalleryTests.events
    archive_count = test_gallery.GalleryTests.archive_count

    def run_albums(self, *records, codes=(), change=None, downloads=None):
        config.set(('extractor', 'reddit'), 'parent-metadata', '_reddit')
        config.set(('extractor',), 'retries', 0)
        calls, codes = [], list(codes)

        def request(session, method, url, **kwargs):
            calls.append(url)
            response = Response()
            response.url = url
            response.status_code = codes.pop(0) if url.endswith('/Missing') else 200
            data = album(url.rsplit('/', 1)[-1])
            if change is not None:
                change(data)
            response._content_consumed = True
            response._content = json.dumps(data).encode()
            return response

        def download(child, url):
            if downloads is not None:
                downloads.append(url)
            self.assertEqual(self.events(kinds=None)[-1]['state'], 'started')
            child.pathfmt.part_enable()
            with child.pathfmt.open('wb') as output:
                output.write(b'fixture ' + url.encode())
            return True

        with patch('requests.sessions.Session.request', request), patch.object(NativeDownloadJob, 'download', download):
            status = self.task(*records, fixture=LinkedImgurFixture).run()
        return status, calls

    def test_actual_extractor_keeps_album_membership_order_and_replay(self):
        for route in ('a', 'gallery'):
            with self.subTest(route=route):
                post = reddit_data(id='post', url='https://imgur.com/' + route + '/Album12')
                downloads = []
                for _ in range(2):
                    status, _ = self.run_albums(post, downloads=downloads)
                    self.assertEqual(status, 0)
                self.assertEqual(downloads, [] if route == 'gallery' else
                                 ['https://i.imgur.com/First12.jpg', 'https://i.imgur.com/Next123.mp4'])
        self.assertEqual(self.archive_count(), 2)
        events = self.events()
        self.assertEqual(len(events), 16)
        for captured, completed in zip(events[::2], events[1::2]):
            self.assertEqual(captured['post'], {'namespace': 'native:reddit', 'value': 'post'})
            self.assertEqual(captured['metadata']['title'], 'Caption')
            evidence = captured['source']['album']['imgur_media']
            self.assertEqual(evidence, {'version': 1, 'items': [
                {'id': 'First12', 'kind': 'image'}, {'id': 'Next123', 'kind': 'unknown'}]})
            self.assertEqual(completed['source']['attachment'],
                             {'namespace': 'native:imgur', 'value': captured['source']['id']})
            self.assertEqual(completed['source']['capture_event_uuid'], captured['event_uuid'])

    def test_dead_album_retries_then_later_post_and_restart_continue(self):
        for route in ('a', 'gallery'):
            with self.subTest(route=route):
                dead = reddit_data('missing', url='https://imgur.com/' + route + '/Missing')
                good = reddit_data('present', url='https://imgur.com/a/Present')
                before = len(self.events())
                status, calls = self.run_albums(dead, good, codes=[404, 404, 410])
                self.assertEqual(status, 0)
                self.assertEqual(sum(u.endswith('/Missing') for u in calls), 3)
                captures = self.events()[before:]
                self.assertEqual(captures[0]['source']['native_media_status']['attempts'], 3)
                self.assertEqual([event['kind'] for event in captures],
                                 ['source.capture', 'source.capture', 'file.completed', 'source.capture', 'file.completed'])
                bound = missing_media.target('reddit', dead['url'], dead)
                with closing(Outbox(self.box.path, self.box.endpoint, self.box.producer)) as reopened:
                    self.assertEqual(missing_media.cached(reopened, bound)['attempts'], 3)
                status, calls = self.run_albums(dead, good)
                self.assertEqual(status, 0)
                self.assertFalse(any(u.endswith('/Missing') for u in calls))
        self.assertFalse(any(e['kind'] == 'attachment.download' and e['state'] == 'failed'
                             for e in self.events(kinds=None)), 'missing albums must not invent media identities')

    def test_nested_gallery_album_failure_is_handled_by_owning_reddit_post(self):
        calls = []

        def items(extractor):
            # The gallery dispatch succeeds; only its album endpoint is gone.
            yield Message.Queue, 'https://imgur.com/a/Missing', {'_extractor': ImgurAlbumExtractor}

        dead = reddit_data('missing', url='https://imgur.com/gallery/Missing')
        with patch.object(ImgurGalleryExtractor, 'items', items):
            status, calls = self.run_albums(dead, reddit_data('present', url='https://imgur.com/a/Present'), codes=[404] * 3)
        self.assertEqual(status, 0)
        self.assertEqual(sum(u.endswith('/Missing') for u in calls), 3)
        self.assertEqual(len(self.events(kinds=('file.completed',))), 2)

    def test_recovery_before_limit_preserves_media_and_clears_skip_observations(self):
        status, calls = self.run_albums(reddit_data('post', url='https://imgur.com/a/Missing'), codes=[404, 200])
        self.assertEqual(status, 0)
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.box.db.execute('SELECT count(*) FROM missing_media').fetchone()[0], 0)
        self.assertEqual(len(self.events(kinds=('file.completed',))), 2)

    def test_album_authorization_and_rate_limit_are_retryable_not_missing(self):
        for code in (403, 429, 500):
            with self.subTest(code=code), self.assertRaises(SourceFailure):
                self.run_albums(reddit_data('post', url='https://imgur.com/a/Missing'), codes=[code])
            self.producer.source_failure = None
        self.assertEqual(self.box.db.execute('SELECT count(*) FROM missing_media').fetchone()[0], 0)

    def test_conflicting_album_response_cannot_download_unrelated_media(self):
        downloads = []
        status, _ = self.run_albums(reddit_data('post', url='https://imgur.com/a/Album12'),
                                    change=lambda a: a.update(id='Other12'), downloads=downloads)
        self.assertNotEqual(status, 0)
        self.assertEqual(self.producer.failure_code, 'source_rejected')
        self.assertEqual(downloads, [])

    def test_gifv_permalink_downloads_api_mp4_and_replays(self):
        post = reddit_data(id='post', url='https://i.imgur.com/First12.gifv')
        downloads = []

        def single(data):
            data.update(is_album=False, image_count=1, media=[data['media'][0]])
            data['media'][0].update(ext='gif', is_animated=True)

        for _ in range(2):
            status, _ = self.run_albums(post, change=single, downloads=downloads)
            self.assertEqual(status, 0)
        self.assertEqual(downloads, ['https://i.imgur.com/First12.mp4'])
        expected = source.attachment(self.events()[0]['source'])
        self.assertEqual(expected['namespace'], 'native:reddit')
        self.assertTrue(expected['value'].startswith('url:'))
        self.assertTrue(all(e['source']['attachment'] == expected for e in self.events(kinds=('file.completed',))))

    def test_partial_album_skip_count_survives_interruption(self):
        post = reddit_data('post', url='https://imgur.com/a/Missing')
        bound = missing_media.target('reddit', post['url'], post)
        with closing(Outbox(self.box.path, self.box.endpoint, self.box.producer)) as reopened:
            self.assertIsNone(missing_media.failed(reopened, bound, 404))
        status, calls = self.run_albums(post, codes=[404, 410])
        self.assertEqual(status, 0)
        self.assertEqual(len(calls), 2)
        self.assertEqual(missing_media.cached(self.box, bound)['attempts'], 3)
        # Identical album and image IDs have independent retry histories.
        image = missing_media.target('reddit', 'https://imgur.com/Missing', reddit_data(url='https://imgur.com/Missing'))
        self.assertIsNone(missing_media.cached(self.box, image))

    def test_album_api_matching_never_skips_other_items_or_account_endpoints(self):
        from types import SimpleNamespace
        post = reddit_data(url='https://imgur.com/a/Album12')
        bound = missing_media.target('reddit', post['url'], post)
        extractor = SimpleNamespace(category='imgur', subcategory='album')
        self.assertTrue(missing_media.request_matches('https://api.imgur.com/post/v1/albums/Album12', bound, extractor))
        for url in ('https://api.imgur.com/post/v1/albums/Other12', 'https://api.imgur.com/post/v1/media/Album12',
                    'https://api.imgur.com/3/account/Album12', 'https://foreign.invalid/post/v1/albums/Album12',
                    'https://api.imgur.com/post/v1/albums/Album12?other=1'):
            self.assertFalse(missing_media.request_matches(url, bound, extractor))

    def test_metadata_only_fetch_retains_the_same_ordered_album(self):
        from gallery_dl.extractor.imgur import ImgurAPI
        from stash_ingest.metadata_bundle import Bundle
        from stash_ingest.metadata_fetch import collect
        from stash_ingest.runtime import CAPTURE_VERSION
        from test_metadata_fetch import Post, URL, factory
        post = reddit_data(id='post', url='https://imgur.com/a/Album12')
        messages = [(Message.Directory, '', post), (Message.Queue, post['url'], post)]

        def find(url):
            return factory(Post, url, messages) if url == URL else ImgurAlbumExtractor.from_url(url)

        with patch.object(ImgurAPI, 'album', side_effect=lambda _: album()):
            value = collect(URL, {}, factory=find)
        self.assertEqual(value['pending'], [])
        self.assertEqual(value['unresolved'], [])
        bundle = Bundle(URL, CAPTURE_VERSION, value)
        members = [bundle.metadata(i, with_parent=True) for i, row in enumerate(value['records']) if row['kind'] == 'media']
        self.assertEqual([source.attachment(m)['value'] for m in members], ['First12', 'Next123'])
        self.assertTrue(all(m['album']['imgur_media']['items'] == [
            {'id': 'First12', 'kind': 'image'}, {'id': 'Next123', 'kind': 'unknown'}] for m in members))

    def test_empty_album_does_not_block_the_next_post(self):
        def empty(data):
            if data['id'] == 'Empty12':
                data.update(media=[], image_count=0)
        downloads = []
        status, _ = self.run_albums(reddit_data('empty', url='https://imgur.com/a/Empty12'),
                                    reddit_data('next', url='https://imgur.com/a/Next123'), change=empty, downloads=downloads)
        self.assertEqual(status, 0)
        self.assertEqual(len(downloads), 2)
        self.assertIsNone(self.producer.failure_code)

    def test_shared_server_album_and_attachment_contract(self):
        fixture = Path(__file__).resolve().parents[3] / 'pkg/archive/testdata/reddit-imgur-albums-v1.json'
        for case in json.loads(fixture.read_text())['cases']:
            with self.subTest(case=case['name']):
                original = copy.deepcopy(case['source'])
                for data in (case['source'], retain(case['source'])):
                    if case['attachment'] is None:
                        with self.assertRaises(InvalidData):
                            source.attachment(data)
                    else:
                        self.assertEqual(source.attachment(data), case['attachment'])
                        self.assertEqual(source.post(data), {'namespace': 'native:reddit', 'value': 'post'})
                self.assertEqual(original, case['source'])
