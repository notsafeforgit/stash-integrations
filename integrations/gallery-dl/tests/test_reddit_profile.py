import copy
import html
import json
import unittest
from unittest.mock import Mock, patch

from gallery_dl import config, extractor
from gallery_dl.extractor.reddit import RedditAPI, RedditUserExtractor

from stash_ingest import reddit_profile
from stash_ingest.client import drain_once
from stash_ingest.encoding import InvalidData, decode
from stash_ingest.outbox import Outbox
from stash_ingest.source_window import SourceWindow
import test_gallery
from test_producer import reddit_data


USER = {'id': 'abc123', 'name': 'Example', 'subreddit': {'public_description': 'New bio https://bio.invalid'}}


def profile_html(user=USER):
    identity = html.escape(json.dumps({'profile': {'id': 't2_' + user['id'], 'name': user['name']}}), quote=True)
    link = html.escape(json.dumps({'social_link': {'url': 'https://myredlink.com/example'}}), quote=True)
    return (f'<reddit-page-data data="{identity}"></reddit-page-data>'
            f'<faceplate-tracker source="profile" action="click" noun="social_link" data-faceplate-tracking-context="{link}">'
            '<a href="https://myredlink.com/example">Website</a></faceplate-tracker>'
            '<article><a href="https://unrelated.invalid">Unrelated post</a></article>')


class ProfileParserTests(unittest.TestCase):
    def test_own_profile_controls_only_and_stable_identity_required(self):
        self.assertEqual(reddit_profile.social_links(profile_html(), USER), [{'url': 'https://myredlink.com/example'}])
        for body in ('<a href="https://wrong.invalid">link</a>', profile_html({**USER, 'id': 'wrong'}),
                     profile_html() + profile_html({**USER, 'id': 'another'})):
            with self.assertRaises(InvalidData):
                reddit_profile.social_links(body, USER)

    def test_http_and_myredlink_redirects_do_not_fetch_destination(self):
        landing = '<a id="bncyContinue" href="https://onlyfans.com/example">Continue</a><script>window.BOUNCY_REDIRECT_URL = \'https://onlyfans.com/example\';</script>'
        fetch = Mock(side_effect=[(302, 'https://myredlink.com/example', ''), (200, None, landing)])
        self.assertEqual(reddit_profile.resolve_link('https://bit.ly/example', fetch), 'https://onlyfans.com/example')
        self.assertEqual([c.args[0] for c in fetch.call_args_list], ['https://bit.ly/example', 'https://myredlink.com/example'])
        self.assertIsNone(reddit_profile.resolve_link('https://ordinary.invalid', fetch))
        self.assertEqual(fetch.call_count, 2)

    def test_redirect_bounds_unsafe_targets_ambiguous_pages_and_no_script_execution(self):
        for target in ('http://127.0.0.1/admin', 'http://[::1]/', 'http://localhost/',
                       'https://user:pass@onlyfans.com/example', 'javascript:alert(1)', 'https://bit.ly/a'):
            fetch = Mock(return_value=(302, target, ''))
            self.assertIsNone(reddit_profile.resolve_link('https://bit.ly/a', fetch))
            self.assertEqual(fetch.call_count, 1)
        self.assertIsNone(reddit_profile.redirect_target('<a href="https://onlyfans.com/ad">ad</a><script>location.href=compute()</script>'))
        self.assertIsNone(reddit_profile.redirect_target('<a id="bncyContinue" href="https://a.invalid"></a><meta http-equiv="refresh" content="0;url=https://b.invalid">'))
        self.assertEqual(reddit_profile.redirect_target('<meta http-equiv="refresh" content="0; url=https://onlyfans.com/example">'), 'https://onlyfans.com/example')
        fetch = Mock(side_effect=[(302, f'https://bit.ly/{i}', '') for i in range(5)])
        self.assertIsNone(reddit_profile.resolve_link('https://bit.ly/start', fetch))
        self.assertEqual(fetch.call_count, 5)


class ProfileGalleryTests(unittest.TestCase):
    def setUp(self):
        test_gallery.GalleryTests.setUp(self)
        self.lease.run['target_url'] = 'https://www.reddit.com/user/example/submitted/?sort=new'
        self.producer.path_prefix = '.'
        config.set(('extractor', 'reddit'), 'comments', 0)
        config.set(('extractor', 'reddit'), 'client-id', None)
        extractor.find('https://www.reddit.com/comments/abc123/')
        extractor.find('https://www.reddit.com/r/example/')

    task = test_gallery.GalleryTests.task
    events = test_gallery.GalleryTests.events

    def test_empty_initial_and_incremental_fetches_queue_profile_without_media_cursor(self):
        response = Mock(status_code=200, text=profile_html())
        for mode in ('initial', 'routine'):
            self.producer.scan_mode = mode
            self.producer.window = SourceWindow({'since': '2026-10-01T00:00:00Z', 'until': '2026-10-08T00:00:00Z'})
            with patch.object(RedditAPI, 'user_about', return_value=copy.deepcopy(USER)), \
                    patch.object(RedditAPI, 'submissions_user', return_value=iter(())), \
                    patch.object(RedditUserExtractor, 'request', return_value=response), \
                    patch.object(reddit_profile, 'resolve_link', return_value='https://onlyfans.com/example'):
                self.assertEqual(self.task(fixture=RedditUserExtractor).run(), 0)
        events = self.events(kinds=('account.profile',))
        self.assertEqual(len(events), 2)
        self.assertEqual(self.producer.items_seen, 0)
        self.assertEqual(self.lease.checkpoints, [])
        self.assertEqual(self.events(kinds=('source.capture', 'file.completed')), [])
        self.assertEqual(events[-1]['source']['user']['social_links'], [{'url': 'https://myredlink.com/example'}])
        self.assertEqual(events[-1]['source']['user']['resolved_links'][0]['resolved_url'], 'https://onlyfans.com/example')

    def test_unavailable_social_page_preserves_api_bio(self):
        x = Mock()
        x.request.return_value = Mock(status_code=403)
        with patch.object(reddit_profile, 'resolve_link', return_value=None):
            user = reddit_profile.refresh(x, self.producer, copy.deepcopy(USER))
        self.assertEqual(user, USER)
        x.log.warning.assert_called_once()
        self.producer.observe_profile({'category': 'reddit', 'user': user})
        self.assertEqual(len(self.events(kinds=('account.profile',))), 1)

    def test_filtered_posts_and_archive_overlap_still_refresh_profile(self):
        response = Mock(status_code=200, text=profile_html())
        post = reddit_data(created_utc=1790985600, author_fullname='t2_abc123',
                           is_video=False, is_self=False, selftext_html='', title='Fixture post')
        self.producer.window = SourceWindow({'since': '2026-10-10T00:00:00Z', 'until': '2026-10-11T00:00:00Z'})
        with patch.object(RedditAPI, 'user_about', side_effect=lambda *_: copy.deepcopy(USER)), \
                patch.object(RedditAPI, 'submissions_user', side_effect=lambda *_: iter([(copy.deepcopy(post), ())])), \
                patch.object(RedditUserExtractor, 'request', return_value=response), \
                patch.object(reddit_profile, 'resolve_link', return_value=None):
            self.assertEqual(self.task(fixture=RedditUserExtractor).run(), 0)
            self.assertEqual(self.events(kinds=('source.capture',)), [])
            self.producer.window = SourceWindow({'since': None, 'until': '2027-01-01T00:00:00Z'})
            for _ in range(2):
                self.assertEqual(self.task(fixture=RedditUserExtractor).run(), 0)
        self.assertEqual(len(self.events(kinds=('account.profile',))), 3)
        starts = [e for e in self.events(kinds=('attachment.download',)) if e['state'] == 'started']
        self.assertEqual(len(starts), 1)
        captures = self.events(kinds=('source.capture',))
        self.assertEqual(len(captures), 2)
        self.assertEqual(captures[-1]['source']['user']['social_links'], [{'url': 'https://myredlink.com/example'}])

    def test_pending_profile_survives_restart_and_old_server_then_acknowledges(self):
        event_id = self.producer.observe_profile({'category': 'reddit', 'user': copy.deepcopy(USER)})
        path, endpoint, producer = self.box.path, self.box.endpoint, self.box.producer
        # Preserve exact durable bytes; a server rollout cannot turn unsupported
        # profile events into permanently rejected work.
        body = self.box.db.execute('SELECT body FROM events WHERE event_uuid=?', (event_id,)).fetchone()[0]
        client = Mock(endpoint=endpoint, producer=producer)
        client.capabilities.return_value = {'file_ingestion': True, 'file_transformation_protocol': 1}
        self.assertEqual(drain_once(self.box, client)['retried'], 1)
        client.batch.assert_not_called()
        self.box.close()
        self.box = Outbox(path, endpoint, producer)
        self.addCleanup(self.box.close)
        self.assertEqual(self.box.db.execute('SELECT body FROM events WHERE event_uuid=?', (event_id,)).fetchone()[0], body)
        self.box.db.execute("UPDATE events SET available_at=0")
        deliveries = self.box.claim(self.lease.owner)
        event = decode(body)
        receipt = {k: event[k] for k in ('producer_uuid', 'event_uuid', 'kind', 'collection_uuid', 'collection_revision', 'root_uuid', 'run_uuid')}
        receipt.update(sha256=deliveries[0].sha256, credential_uuid=self.lease.owner,
                       committed_at='2026-10-26T00:00:00Z', result={'status': 'recorded', 'profile_uuid': self.lease.owner, 'media_ingested': False})
        self.box.acknowledge(deliveries[0], receipt)
        self.assertEqual(self.box.receipt(event_id), receipt)

    def test_redirect_requests_use_separate_session_and_shared_budget(self):
        x = Mock()
        x.session.headers = {'Authorization': 'private-reddit-fixture'}
        x.request.return_value = Mock(status_code=200, text=profile_html())
        response = Mock(status_code=200, headers={})
        response.iter_content.return_value = iter([b'<a id="bncyContinue" href="https://onlyfans.com/example">Continue</a>'])
        session = Mock(headers={'Authorization': 'must-be-cleared'})
        session.get.return_value = response
        session.__enter__ = Mock(return_value=session)
        session.__exit__ = Mock(return_value=False)
        with patch.object(reddit_profile.requests, 'Session', return_value=session), \
                patch.object(self.producer, 'request_source', wraps=self.producer.request_source) as budget:
            user = reddit_profile.refresh(x, self.producer, copy.deepcopy(USER))
        self.assertFalse(session.trust_env)
        self.assertNotIn('Authorization', session.headers)
        self.assertEqual(user['resolved_links'], [{'url': 'https://myredlink.com/example', 'resolved_url': 'https://onlyfans.com/example'}])
        session.get.assert_called_once_with('https://myredlink.com/example', allow_redirects=False, timeout=15, stream=True)
        budget.assert_called_once_with('https://myredlink.com/example')


if __name__ == '__main__':
    unittest.main()
