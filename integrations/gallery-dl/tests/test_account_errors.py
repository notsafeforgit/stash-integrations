from types import SimpleNamespace
import json
import unittest
from unittest.mock import Mock

from gallery_dl import exception
from gallery_dl.extractor.twitter import TwitterAPI

from stash_ingest.account_errors import access_reason, missing_code
from stash_ingest.gallery import NativeDownloadJob
from stash_ingest.runs import SourceFailure


class AccountErrorTests(unittest.TestCase):
    def test_real_twitter_pagination_preserves_protected_and_blocked_states(self):
        for legacy in (False, True):
            for blocked, reason in ((False, 'protected'), (True, 'blocked')):
                with self.subTest(legacy=legacy, blocked=blocked):
                    user = {'core': {'screen_name': 'example'}, 'legacy': {'screen_name': 'example'}}
                    flags = {'protected': True, 'blocked_by': blocked, 'following': False}
                    if legacy:
                        user['legacy'].update(flags)
                    else:
                        user.update(privacy={'protected': True}, relationship_perspectives=flags)
                    extractor = SimpleNamespace(category='twitter', subcategory='media', _user_obj=user,
                                                exc=exception, log=Mock(), retweets=False, pinned=False,
                                                _init_cursor=lambda: None, config=Mock(return_value=False))
                    api = object.__new__(TwitterAPI)
                    api.extractor = extractor
                    api.exc = exception
                    api._json_dumps = json.dumps
                    api.features_pagination = {}
                    api.headers = {'x-twitter-auth-type': 'OAuth2Session'}
                    api._call = Mock(return_value={'data': {}})
                    producer = Mock()
                    def fail(code, scope, **details):
                        raise SourceFailure(code, scope, **details)
                    producer.fail_source.side_effect = fail
                    job = SimpleNamespace(extractor=extractor, producer=producer, _native_scope='service:twitter')
                    posts = api._pagination_tweets('/UserMedia', {'count': 50})
                    with self.assertRaises(SourceFailure) as raised:
                        NativeDownloadJob._source_operation(job, next, posts)
                    self.assertEqual(raised.exception.code, 'access_denied')
                    self.assertEqual(raised.exception.account_access, reason)
                    self.assertNotIn('example', str(raised.exception))
                    api._call.assert_called_once()

    def test_access_classification_requires_account_listing_evidence(self):
        user = {'privacy': {'protected': True}, 'relationship_perspectives': {'following': False}}
        extractor = SimpleNamespace(category='twitter', subcategory='media', _user_obj=user)
        denied = exception.AuthorizationError('private details')
        self.assertEqual(access_reason(extractor, denied), 'protected')
        user['relationship_perspectives']['following'] = True
        self.assertIsNone(access_reason(extractor, denied))
        user['relationship_perspectives']['blocked_by'] = True
        self.assertEqual(access_reason(extractor, exception.AuthRequired('cookies', 'timeline')), 'blocked')
        self.assertIsNone(access_reason(extractor, exception.AuthorizationError('Account temporarily locked')))
        self.assertIsNone(access_reason(extractor, exception.HttpError('private details', Mock(status_code=403))))
        for subcategory in ('tweet', 'search', 'home', 'list'):
            extractor.subcategory = subcategory
            self.assertIsNone(access_reason(extractor, denied))
        extractor.subcategory = 'media'
        extractor.category = 'reddit'
        self.assertIsNone(access_reason(extractor, denied))
        extractor.category = 'twitter'
        for unavailable in (None, {}, {'__typename': 'UserUnavailable'}, {'legacy': {}}):
            extractor._user_obj = unavailable
            self.assertIsNone(access_reason(extractor, denied))

    def test_real_twitter_lookup_preserves_structured_unavailable_states(self):
        for reason, message, code in (
                ('Suspended', 'User has been suspended.', 'account_suspended'),
                ('Deactivated', 'This account is unavailable', 'account_deactivated'),
                ('NotFound', 'User not found.', 'account_not_found'),
                ('UnknownFutureReason', 'Private provider details', 'account_unavailable')):
            with self.subTest(reason=reason):
                extractor = SimpleNamespace(category='twitter', subcategory='media',
                                            _user_obj=None, exc=exception)
                def assign(user):
                    extractor._user_obj = user
                    raise KeyError('legacy')
                extractor._assign_user = assign
                extractor.cache = lambda call, *args: call(*args)
                api = object.__new__(TwitterAPI)
                api.extractor = extractor
                result = {'__typename': 'UserUnavailable', 'reason': reason, 'message': message}
                api.user_by_screen_name = Mock(return_value=result)
                api.user_by_rest_id = Mock(return_value=result)
                producer = Mock()
                def fail(code, scope):
                    raise SourceFailure(code, scope)
                producer.fail_source.side_effect = fail
                job = SimpleNamespace(extractor=extractor, producer=producer, _native_scope='service:twitter')
                for identity in ('example', 'id:123456'):
                    with self.assertRaises(SourceFailure) as raised:
                        NativeDownloadJob._source_operation(job, api._user_id_by_screen_name, identity)
                    self.assertEqual(raised.exception.code, code)
                    self.assertEqual(raised.exception.scope, 'service:twitter')
                    self.assertNotIn(message, str(raised.exception))

    def test_missing_accounts_and_generic_content_errors_stay_distinct(self):
        reddit = SimpleNamespace(category='reddit', subcategory='user-submitted')
        twitter = SimpleNamespace(category='twitter', subcategory='media', _user_obj=None)
        self.assertEqual(missing_code(reddit, exception.NotFoundError('Suspended User', False)), 'account_suspended')
        for resource in ('user', 'user-submitted'):
            self.assertEqual(missing_code(reddit, exception.NotFoundError(resource)), 'account_not_found')
        self.assertEqual(missing_code(twitter, exception.NotFoundError('user')), 'account_not_found')
        for extractor in (reddit, twitter):
            self.assertEqual(missing_code(extractor, exception.NotFoundError('post')), 'not_found')
            self.assertEqual(missing_code(extractor, exception.NotFoundError('unknown private URL')), 'not_found')
            self.assertEqual(missing_code(extractor, exception.HttpError('Suspended User', Mock(status_code=404))), 'not_found')
