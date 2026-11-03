from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from gallery_dl import exception
from gallery_dl.extractor.twitter import TwitterAPI

from stash_ingest.account_errors import missing_code
from stash_ingest.gallery import NativeDownloadJob
from stash_ingest.runs import SourceFailure


class AccountErrorTests(unittest.TestCase):
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
