"""Standalone intake and shared identity with Reddit-embedded Redgifs media."""

import copy
from datetime import datetime, timezone
import unittest
from unittest.mock import patch

from gallery_dl import config
from gallery_dl.extractor.redgifs import RedgifsAPI, RedgifsUserExtractor

from stash_ingest import source
from stash_ingest.gallery import NativeDownloadJob
from stash_ingest.encoding import InvalidData
from stash_ingest.retention import retain
from stash_ingest.source_window import SourceWindow, published
import test_gallery


def item(identity='ExampleClip', **extra):
    return {'id': identity, 'userName': 'ActualPublisher', 'createDate': 1768092373,
            'description': 'Original description', 'gallery': None,
            'urls': {'hd': f'https://media.redgifs.com/{identity}.mp4',
                     'sd': f'https://media.redgifs.com/{identity}-mobile.mp4',
                     'poster': f'https://media.redgifs.com/{identity}-poster.jpg'}, **extra}


class RedgifsContractTests(unittest.TestCase):
    def test_renditions_share_identity_and_reddit_context_wins(self):
        data = item(category='redgifs', date='2020-01-01T00:00:00Z')
        ref = {'namespace': 'native:redgifs', 'value': 'exampleclip'}
        for key in ('hd', 'sd'):
            data['_url'] = data['urls'][key]
            for capture in (data, retain(data)):
                self.assertEqual(source.post(capture), ref)
                self.assertEqual(source.attachment(capture), ref)
                self.assertEqual(source.metadata(capture)['published_at'], '2026-01-11T00:46:13.000Z')
                self.assertEqual(source.metadata(capture)['original_text'], 'Original description')
        data['_reddit'] = {'id': 'post', 'url': 'https://www.redgifs.com/watch/exampleclip',
                           'title': 'Reddit caption', 'date': '2026-02-01T00:00:00Z'}
        self.assertEqual(source.post(data), {'namespace': 'native:reddit', 'value': 'post'})
        self.assertEqual(source.attachment(data), ref)
        self.assertEqual(source.metadata(data)['original_text'], 'Reddit caption')
        self.assertEqual(published(data, 'redgifs'), datetime(2026, 2, 1, tzinfo=timezone.utc))

    def test_foreign_renditions_posters_and_invalid_ids_are_rejected(self):
        data = item(category='redgifs')
        for url in (data['urls']['poster'], 'https://media.redgifs.com/OtherClip.mp4', None):
            with self.assertRaises(InvalidData):
                source.attachment({**data, '_url': url})
        for identity in (None, 123, '', '../clip', 'clip-id'):
            with self.assertRaises(InvalidData):
                source.post({**data, 'id': identity})
        for stamp in (True, '1768092373', -1, 1.5, 253402300800):
            with self.assertRaises(InvalidData):
                source.metadata({**data, 'createDate': stamp})
        with self.assertRaises(InvalidData):
            published({**data, 'createDate': None, 'date': '2020-01-01T00:00:00Z'}, 'redgifs')


class RedgifsGalleryTests(unittest.TestCase):
    def setUp(self):
        test_gallery.GalleryTests.setUp(self)
        self.lease.run['target_url'] = 'https://www.redgifs.com/users/feedowner'
        self.producer.window = SourceWindow({'since': None, 'until': '2026-10-08T00:00:00Z', 'basis': 'traversal'})
        config.set(('extractor',), 'filename', '{id}.{extension}')

    task = test_gallery.GalleryTests.task
    events = test_gallery.GalleryTests.events

    def test_profile_downloads_retain_api_publisher_and_archive_replay(self):
        with patch.object(RedgifsAPI, 'user', side_effect=lambda *_: iter([copy.deepcopy(item())])):
            for _ in range(2):
                self.assertEqual(self.task(fixture=RedgifsUserExtractor).run(), 0)
        captures = self.events(kinds=('source.capture',))
        self.assertTrue(captures)
        self.assertTrue(all(c['source']['userName'] == 'ActualPublisher' for c in captures))
        self.assertTrue(all(c['post'] == {'namespace': 'native:redgifs', 'value': 'exampleclip'} for c in captures))
        self.assertTrue(all(c['metadata']['published_at'] == '2026-01-11T00:46:13.000Z' for c in captures))
        started = [e for e in self.events(kinds=('attachment.download',)) if e['state'] == 'started']
        self.assertEqual(len(started), 1, 'a second profile pass must preserve archive skipping')

    def test_profile_uses_reddit_destination_and_shared_archive(self):
        from test_producer import reddit_data
        self.producer.path_prefix = '.'
        self.lease.run['target_url'] = 'https://fixture.invalid/account'
        config.set(('extractor',), 'directory', [])
        config.set(('extractor', 'reddit'), 'directory', ['Custom', '{{saved}} {author}, reddit'])
        config.set(('extractor', 'reddit'), 'parent-directory', True)
        config.set(('extractor', 'reddit'), 'parent-metadata', '_reddit')
        config.set(('extractor', 'redgifs', 'user'), 'native-profile-directory', True)
        config.set(('extractor', 'redgifs', 'user'), 'directory', ['{userName}, redgifs'])
        clip = item()
        post = reddit_data(id='redditpost', url='https://www.redgifs.com/watch/exampleclip')
        downloaded = []

        def download(child, url):
            downloaded.append(url)
            child.pathfmt.part_enable()
            with child.pathfmt.open('wb') as output:
                output.write(b'completed fixture video')
            return True

        with patch.object(RedgifsAPI, 'gif', side_effect=lambda *_: copy.deepcopy(clip)), \
                patch.object(NativeDownloadJob, 'download', download):
            # Archive skips still carry enough destination/profile evidence.
            for _ in range(2):
                self.assertEqual(self.task(post, fixture=test_gallery.LinkedRedgifsFixture).run(), 0)
        captures = self.events(kinds=('source.capture',))
        destination = 'Custom/{saved} publisher, reddit'
        self.assertEqual([c['source']['native_redgifs_directory'] for c in captures], [destination] * 2)
        self.assertEqual(len(downloaded), 1)

        self.lease.run['target_url'] = 'https://www.redgifs.com/users/actualpublisher'
        self.producer.path_prefix = destination
        records = [clip, item('SecondClip')]
        with patch.object(RedgifsAPI, 'user', side_effect=lambda *_: iter(copy.deepcopy(records))):
            for _ in range(2):
                self.assertEqual(self.task(fixture=RedgifsUserExtractor).run(), 0)
        self.assertTrue((self.media / destination / 'ExampleClip.mp4').is_file())
        self.assertTrue((self.media / destination / 'SecondClip.mp4').is_file())
        self.assertFalse((self.media / 'ActualPublisher, redgifs').exists())
        started = [e for e in self.events(kinds=('attachment.download',)) if e['state'] == 'started']
        self.assertEqual(len(started), 2, 'one Reddit download and only the missing profile clip')
        direct = [c for c in self.events(kinds=('source.capture',)) if c['post']['namespace'] == 'native:redgifs']
        self.assertTrue(direct)
        self.assertTrue(all('_reddit' not in c['source'] and 'native_redgifs_directory' not in c['source'] for c in direct))

    def test_gallery_members_keep_independent_ids_publishers_and_dates(self):
        first = item(gallery='gallery-id')
        second = item('SecondClip', gallery='gallery-id', userName='SecondPublisher', createDate=1768178773)
        with patch.object(RedgifsAPI, 'user', return_value=iter([first])), \
                patch.object(RedgifsAPI, 'gallery', return_value={'gifs': [first, second]}):
            self.assertEqual(self.task(fixture=RedgifsUserExtractor).run(), 0)
        captures = self.events(kinds=('source.capture',))
        self.assertEqual([c['post']['value'] for c in captures], ['exampleclip', 'secondclip'])
        self.assertEqual([c['source']['userName'] for c in captures], ['ActualPublisher', 'SecondPublisher'])
        self.assertEqual([c['metadata']['published_at'] for c in captures],
                         ['2026-01-11T00:46:13.000Z', '2026-01-12T00:46:13.000Z'])

    def test_publication_window_uses_each_api_item_date(self):
        self.producer.window = SourceWindow({'since': '2026-01-12T00:00:00Z', 'until': '2026-01-13T00:00:00Z'})
        records = [item(), item('SecondClip', createDate=1768178773)]
        with patch.object(RedgifsAPI, 'user', return_value=iter(records)):
            self.assertEqual(self.task(fixture=RedgifsUserExtractor).run(), 0)
        self.assertEqual([c['post']['value'] for c in self.events(kinds=('source.capture',))], ['secondclip'])


if __name__ == '__main__':
    unittest.main()
