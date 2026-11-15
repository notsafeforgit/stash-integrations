import copy
import json
import unittest
from unittest.mock import patch

from gallery_dl import config, exception, extractor
from gallery_dl.extractor import twitter
from gallery_dl.extractor.twitter import TwitterAPI, TwitterExtractor

from stash_ingest import pagination, twitter_listing
from stash_ingest.discovery_fetch import collect
from stash_ingest.encoding import InvalidData
from stash_ingest.producer import Producer
from stash_ingest.source_window import SourceWindow
from stash_ingest.twitter_listing import PREFIX, decode, encode, initial
import test_producer

MEDIA = 'https://x.com/id:99/media'
BASE = 1_900_000_000_000_000_000
AUTHOR = {'rest_id': '99', 'core': {'screen_name': 'example', 'name': 'Example',
                                    'created_at': 'Thu Oct 01 00:00:00 +0000 2026'},
          'relationship_counts': {'followers': 0, 'following': 0}, 'tweet_counts': {'media_tweets': 0, 'tweets': 0},
          'location': {'location': ''}, 'verification': {'verified': False}, 'privacy': {'protected': False},
          'profile_bio': {'description': ''}, 'legacy': {}}


def tid(number):
    return str(BASE + number)


def tweet(number, *kinds, pinned=False, quoted=None):
    kinds = kinds or ('photo',)
    media = [{'id_str': tid(number) + str(index), 'type': kind, 'original_info': {'width': 1, 'height': 1},
              'media_url_https': f'https://pbs.twimg.com/media/{number}-{index}.jpg',
              **({'video_info': {'variants': [{'bitrate': 1, 'url': f'https://video.twimg.com/{number}.mp4'}]}}
                 if kind == 'video' else {})} for index, kind in enumerate(kinds)]
    result = {'rest_id': tid(number),
              'core': {'user_results': {'result': copy.deepcopy(AUTHOR)}},
              'legacy': {'id_str': tid(number), 'full_text': '', 'entities': {}, 'user_id_str': '99', 'lang': 'en',
                         'created_at': 'Thu Oct 01 00:00:00 +0000 2026',
                         'extended_entities': {'media': media}}}
    if quoted is not None:
        inner = tweet(quoted)['content']['itemContent']['tweet_results']['result']
        result['quoted_status_result'] = {'result': inner}
    return {'entryId': 'tweet-' + tid(number), 'sortIndex': tid(number),
            'content': {'itemContent': {'tweet_results': {'result': result}}}}


def response(entries, cursor=None, *, search=False, pinned=None):
    entries = list(entries)
    if cursor:
        entries.append({'entryId': 'cursor-bottom-0', 'content': {'value': cursor}})
    instructions = [{'type': 'TimelineAddEntries', 'entries': entries}]
    if pinned is not None:
        instructions.insert(0, {'type': 'TimelinePinEntry', 'entry': pinned})
    body = {'timeline': {'instructions': instructions}}
    if search:
        return {'data': {'search_by_raw_query': {'search_timeline': body}}}
    return {'data': {'user': {'result': {'timeline': body}}}}


class Server:
    """Answer real gallery-dl pagination with fixed timeline/search pages."""

    def __init__(self, pages):
        self.pages, self.calls = pages, []

    def __call__(self, endpoint, params, *args, **kwargs):
        name = endpoint.rsplit('/', 1)[-1]
        variables = json.loads(params['variables'])
        key = (name, variables.get('rawQuery'), variables.get('cursor'))
        self.calls.append(key)
        return copy.deepcopy(self.pages.get(key) or response((), search=name == 'SearchTimeline'))


def assign(user=None):
    def user_id(handle):
        target = assign.target
        target._user, target._user_obj = {'name': 'example', 'id': '99'}, user or {}
        return '99'
    return user_id


def ids(rows):
    return [row['rest_id'] for row in rows]


class TwitterListingTests(unittest.TestCase):
    def setUp(self):
        test_producer.ProducerTests.setUp(self)
        list(extractor.extractors())
        config.clear()
        self.addCleanup(config.clear)

    def target(self, server, url=MEDIA, *, user=None, producer=None):
        target = (twitter.TwitterTweetsExtractor if '/timeline' in url or url.endswith('/tweets')
                  else twitter.TwitterMediaExtractor).from_url(url)
        target.initialize()
        assign.target = target
        target.api._user_id = assign(user)
        target.api._call = server
        pagination.install(target, producer or self.producer)
        return target

    def resumed(self, page):
        self.lease.run['progress'] = {'items_seen': 10, 'files_completed': 10, 'cursor': '',
                                      'pagination': {**page, 'item': ''}}
        return Producer(self.box, self.lease, self.root, extractor_version='fixture')

    def full_history_pages(self):
        return {
            ('UserPhotoTimeline', None, None): response([tweet(50), tweet(40, 'photo', 'video')], 'p2'),
            ('UserPhotoTimeline', None, 'p2'): response([tweet(30), tweet(20)]),
            ('UserVideoTimeline', None, None): response([tweet(60, 'video'), tweet(40, 'photo', 'video')], 'v2'),
            ('UserVideoTimeline', None, 'v2'): response([tweet(10, 'video')]),
            ('SearchTimeline', f'from:example max_id:{tid(20)} filter:links', None):
                response([tweet(20), tweet(5), tweet(3)], search=True),
        }

    def test_full_history_reads_each_timeline_then_searches_below_where_both_were_complete(self):
        server = Server(self.full_history_pages())
        target = self.target(server)
        rows = list(target.tweets())
        # The mixed post and the search's inclusive bound repeat earlier posts.
        self.assertEqual(ids(rows), [tid(n) for n in (50, 40, 30, 20, 60, 10, 5, 3)])
        self.assertEqual(server.calls, [
            ('UserPhotoTimeline', None, None), ('UserPhotoTimeline', None, 'p2'),
            ('UserVideoTimeline', None, None), ('UserVideoTimeline', None, 'v2'),
            # Photos ended at 20 and videos at 10: below 20 neither is complete.
            ('SearchTimeline', f'from:example max_id:{tid(20)} filter:links', None),
            # The next search starts below the oldest result of the last one.
            ('SearchTimeline', f'from:example max_id:{int(tid(3)) - 1} filter:links', None)])
        pages = [decode(page['cursor'], target._native_listing.phases) for page in self.lease.pages if page]
        self.assertEqual(self.lease.pages[0]['cursor'], '')
        self.assertEqual([(p['phase'], p['cursor'], p['bound']) for p in pages], [
            ('photos', '', None), ('photos', 'p2', None), ('videos', '', None), ('videos', 'v2', None),
            ('search-links', '', int(tid(20))), ('search-links', '', int(tid(3)) - 1)])
        self.assertEqual(pages[4]['floors'], {'photos': int(tid(20)), 'videos': int(tid(10))})

    def test_unfiltered_search_runs_only_when_the_link_search_finds_nothing(self):
        pages = self.full_history_pages()
        del pages[('SearchTimeline', f'from:example max_id:{tid(20)} filter:links', None)]
        pages[('SearchTimeline', f'from:example max_id:{tid(20)}', None)] = response([tweet(4)], search=True)
        server = Server(pages)
        rows = list(self.target(server).tweets())
        self.assertEqual(ids(rows)[-1], tid(4))
        self.assertEqual([c[1] for c in server.calls if c[0] == 'SearchTimeline'], [
            f'from:example max_id:{tid(20)} filter:links', f'from:example max_id:{tid(20)}',
            f'from:example max_id:{int(tid(4)) - 1}'])

    def test_pinned_and_quoted_posts_do_not_move_the_search_start(self):
        config.set(('extractor', 'twitter'), 'pinned', True)
        server = Server({
            ('UserPhotoTimeline', None, None): response([tweet(50, quoted=1), tweet(30)], pinned=tweet(2)),
        })
        target = self.target(server)
        rows = list(target.tweets())
        self.assertIn(tid(2), ids(rows))
        self.assertIn(tid(1), ids(rows))
        self.assertEqual([c[1] for c in server.calls if c[0] == 'SearchTimeline'][0],
                         f'from:example max_id:{tid(30)} filter:links')

    def test_resume_continues_the_saved_phase_page_and_its_recorded_floors(self):
        state = {**initial('videos'), 'cursor': 'v2', 'last': int(tid(40)), 'floors': {'photos': int(tid(20))}}
        phases = ('photos', 'videos', 'search-links', 'search-all')
        page = {'extractor': 'twitter:media', 'url': MEDIA, 'cursor': encode(state, phases)}
        producer = self.resumed(page)
        server = Server(self.full_history_pages())
        target = self.target(server, producer=producer)
        rows = list(target.tweets())
        self.assertEqual(server.calls[0], ('UserVideoTimeline', None, 'v2'))
        self.assertNotIn('UserPhotoTimeline', [c[0] for c in server.calls])
        self.assertEqual(ids(rows), [tid(n) for n in (10, 20, 5, 3)])
        self.assertFalse(producer.listing_restarted)

    def test_checkpoints_from_the_removed_timeline_endpoints_restart_the_listing(self):
        for saved in ({'extractor': 'twitter:media', 'url': MEDIA, 'cursor': 'old-UserMedia-cursor'},
                      {'extractor': 'twitter:timeline', 'url': 'https://x.com/id:99/timeline', 'cursor': '2_123/abc'}):
            with self.subTest(saved=saved['extractor']):
                self.lease.pages.clear()
                producer = self.resumed(saved)
                server = Server(self.full_history_pages())
                rows = list(self.target(server, producer=producer).tweets())
                self.assertTrue(producer.listing_restarted)
                self.assertIsNone(producer.saved_pagination)
                self.assertEqual(server.calls[0], ('UserPhotoTimeline', None, None))
                self.assertEqual(ids(rows)[0], tid(50))
                self.assertEqual(self.lease.pages[0]['cursor'], '')

    def test_a_saved_first_page_from_an_earlier_release_still_resumes(self):
        producer = self.resumed({'extractor': 'twitter:media', 'url': MEDIA, 'cursor': ''})
        server = Server(self.full_history_pages())
        list(self.target(server, producer=producer).tweets())
        self.assertFalse(producer.listing_restarted)
        self.assertTrue(producer.pagination_started)

    def test_routine_checks_stop_each_timeline_at_its_own_archived_post(self):
        self.producer.scan_mode = 'routine'
        server = Server({
            ('UserPhotoTimeline', None, None): response([tweet(50), tweet(40, 'photo', 'video'), tweet(30)], 'p2'),
            ('UserVideoTimeline', None, None): response([tweet(60, 'video'), tweet(40, 'photo', 'video'),
                                                         tweet(35, 'video'), tweet(15, 'video')], 'v2'),
        })
        target = self.target(server)
        archived = {tid(30), tid(35)}
        seen = []
        for row in target.tweets():
            seen.append(row['rest_id'])
            self.producer.routine_post_items = 1
            self.producer.routine_post_archived = row['rest_id'] in archived
        self.assertEqual(seen, [tid(n) for n in (50, 40, 30, 60, 35)])
        self.assertEqual([c[0] for c in server.calls], ['UserPhotoTimeline', 'UserVideoTimeline'])

    def test_a_routine_video_check_does_not_stop_at_a_post_from_the_photo_timeline(self):
        self.producer.scan_mode = 'routine'
        state = initial('videos')
        page = {'extractor': 'twitter:media', 'url': MEDIA, 'cursor': encode(state, ('photos', 'videos'))}
        producer = self.resumed(page)
        producer.scan_mode = 'routine'
        server = Server({('UserVideoTimeline', None, None): response(
            [tweet(60, 'video'), tweet(40, 'photo', 'video'), tweet(35, 'video'), tweet(15, 'video')])})
        seen = []
        for row in self.target(server, producer=producer).tweets():
            seen.append(row['rest_id'])
            producer.routine_post_items = 1
            producer.routine_post_archived = row['rest_id'] in {tid(40), tid(35)}
        self.assertEqual(seen, [tid(n) for n in (60, 40, 35)])

    def test_restarted_routine_listing_does_not_stop_at_archived_posts(self):
        producer = self.resumed({'extractor': 'twitter:media', 'url': MEDIA, 'cursor': 'old-UserMedia-cursor'})
        producer.scan_mode = 'routine'
        server = Server({('UserPhotoTimeline', None, None): response([tweet(50), tweet(30), tweet(20)])})
        seen = []
        for row in self.target(server, producer=producer).tweets():
            seen.append(row['rest_id'])
            producer.routine_post_items = 1
            producer.routine_post_archived = True
        self.assertEqual(seen, [tid(n) for n in (50, 30, 20)])

    def test_bounded_windows_and_routine_checks_do_not_search_history(self):
        for routine in (False, True):
            with self.subTest(routine=routine):
                self.producer.scan_mode = 'routine' if routine else 'initial'
                self.producer.window = SourceWindow({'since': '2026-10-01T00:00:00Z', 'until': '2027-01-01T00:00:00Z'})
                server = Server(self.full_history_pages())
                list(self.target(server).tweets())
                self.assertNotIn('SearchTimeline', [c[0] for c in server.calls])

    def test_protected_history_is_not_completed_by_an_empty_listing(self):
        server = Server({})
        user = {'privacy': {'protected': True}, 'relationship_perspectives': {'following': False}}
        target = self.target(server, user=user)
        with self.assertRaises(exception.AuthorizationError):
            list(target.tweets())
        self.assertNotIn('SearchTimeline', [c[0] for c in server.calls])
        # Once the account accepts this viewer, the same import runs in full.
        user['relationship_perspectives']['following'] = True
        server = Server(self.full_history_pages())
        self.assertEqual(len(list(self.target(server, user=user).tweets())), 8)

    def test_an_empty_public_account_still_searches_without_a_bound(self):
        server = Server({})
        self.assertEqual(list(self.target(server).tweets()), [])
        self.assertEqual([c[1] for c in server.calls if c[0] == 'SearchTimeline'],
                         ['from:example filter:links', 'from:example'])

    def test_posts_route_and_the_removed_timeline_url_search_after_the_posts_timeline(self):
        server = Server({('UserOriginalsTimeline', None, None): response([tweet(50), tweet(30)])})
        target = self.target(server, 'https://x.com/id:99/timeline')
        self.assertEqual(target.subcategory, 'tweets')
        list(target.tweets())
        self.assertEqual(server.calls[1], ('SearchTimeline', f'from:example max_id:{tid(30)} filter:links', None))

    def test_checkpoint_format_is_canonical_and_bound_to_its_phases(self):
        phases = ('photos', 'videos', 'search-links', 'search-all')
        self.assertEqual(encode(initial('photos'), phases), '')
        state = {**initial('search-links'), 'bound': 7, 'last': 5, 'floors': {'photos': 9}, 'found': True}
        value = encode(state, phases)
        self.assertTrue(value.startswith(PREFIX))
        self.assertEqual(decode(value, phases), state)
        for invalid in (value.replace('{', '{ ', 1), 'legacy-cursor',
                        encode({**state, 'phase': 'videos'}, phases),          # bound outside search
                        encode({**state, 'phase': 'search-all'}, phases),      # found outside link search
                        encode({**initial('videos'), 'floors': {'videos': 3}}, phases),
                        encode({**initial('posts'), 'cursor': 'x'}, ('posts',)).replace('"posts"', '"reposts"')):
            with self.subTest(invalid=invalid), self.assertRaises(InvalidData):
                decode(invalid, phases)

    def test_a_request_outside_the_checkpoint_phase_is_rejected(self):
        target = self.target(Server({}))
        with self.assertRaises(InvalidData):
            target._native_listing.page('/graphql/hash/UserVideoTimeline', {})
        target._native_listing.state['phase'] = 'search-links'
        target._user = {'name': 'example'}
        with self.assertRaises(InvalidData):
            target._native_listing.page('/graphql/hash/SearchTimeline', {'rawQuery': 'from:other'})

    def test_removed_timeline_include_routes_to_its_configured_strategy(self):
        from stash_ingest.recent import twitter_include
        parent = twitter.TwitterUserExtractor.from_url('https://x.com/example')
        self.assertEqual(twitter_include(parent, ['timeline', 'media']), ('media',))
        config.set(('extractor', 'twitter'), 'timeline', {'strategy': 'tweets'})
        self.assertEqual(twitter_include(parent, 'avatar,timeline'), ('avatar', 'tweets'))
        config.set(('extractor', 'twitter'), 'timeline', {'strategy': 'unknown'})
        with self.assertRaises(InvalidData):
            twitter_include(parent, ['timeline'])


class TwitterDiscoveryTests(unittest.TestCase):
    def setUp(self):
        list(extractor.extractors())
        self.addCleanup(config.clear)

    def collect(self, server, cursor=None, settings=None):
        def user_id(api, handle):
            api.extractor._user, api.extractor._user_obj = {'name': 'example', 'id': '99'}, {}
            return '99'
        with patch.object(TwitterAPI, '_call', side_effect=server), patch.object(TwitterAPI, '_user_id', user_id), \
                patch.object(TwitterExtractor, 'login'), patch.object(TwitterExtractor, 'metadata', return_value={}):
            return collect('https://x.com/id:99/timeline', settings or {}, cursor)

    def pages(self, server, settings=None):
        cursor, pages = None, []
        for _ in range(8):
            page = self.collect(server, cursor, settings)
            self.assertNotIn('error', page)
            pages.append(page)
            if page['complete']:
                return pages
            cursor = page['next_cursor']
        self.fail('Discovery did not complete')

    def test_discovery_reads_one_listing_page_and_resumes_through_history_search(self):
        # Discovery lists text posts and reposts, so the removed timeline's
        # automatic strategy read the posts timeline and unfiltered search.
        server = Server({
            ('UserOriginalsTimeline', None, None): response([tweet(50), tweet(40)], 'p2'),
            ('UserOriginalsTimeline', None, 'p2'): response([tweet(30)]),
            ('SearchTimeline', f'from:example max_id:{tid(30)}', None): response([tweet(5)], search=True),
        })
        pages = self.pages(server)
        phases = ('posts', 'search-all')
        self.assertEqual([len([r for r in p['records'] if r['kind'] == 'post']) for p in pages], [2, 1, 1, 0])
        self.assertEqual(decode(pages[0]['next_cursor']['cursor'], phases)['cursor'], 'p2')
        self.assertEqual(decode(pages[1]['next_cursor']['cursor'], phases)['bound'], int(tid(30)))
        self.assertEqual(server.calls, [
            ('UserOriginalsTimeline', None, None), ('UserOriginalsTimeline', None, 'p2'),
            ('SearchTimeline', f'from:example max_id:{tid(30)}', None),
            ('SearchTimeline', f'from:example max_id:{int(tid(5)) - 1}', None)])

    def test_discovery_rejects_a_removed_timeline_cursor(self):
        self.assertEqual(self.collect(Server({}), {'cursor': '1/saved'}), {'error': 'invalid_checkpoint'})


if __name__ == '__main__':
    unittest.main()
