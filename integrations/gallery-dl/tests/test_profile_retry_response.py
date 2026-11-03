import unittest
from unittest.mock import Mock

from stash_ingest.gallery import SourceSession
from stash_ingest.runs import SourceFailure


class ProfileRetryResponseTests(unittest.TestCase):
    def test_only_explicit_profile_errors_change_retry_classification(self):
        profile = 'https://api.redgifs.com/v2/users/example/search'
        cases = [
            (profile, 404, {'error': {'description': 'User account deleted.'}}, 'account_deleted'),
            (profile, 400, {'message': 'User account deleted.'}, 'account_deleted'),
            (profile, 410, {'message': 'User account deleted.'}, 'account_deleted'),
            (profile, 400, {'error': {'description': 'Page number must be between 1 and 100'}}, 'source_history_limit'),
            (profile, 404, {'message': 'Not found'}, None),
            (profile, 500, {'message': 'User account deleted.'}, None),
            (profile, 404, {'gif': {'description': 'User account deleted.'}}, None),
            ('https://api.redgifs.com/v2/gifs/example', 404, {'message': 'User account deleted.'}, None),
            ('https://api.redgifs.com/v2/auth/temporary', 400, {'message': 'Page number must be between 1 and 100'}, None),
        ]
        for url, status, data, expected in cases:
            with self.subTest(url=url, status=status, data=data):
                response = Mock(status_code=status, headers={}, json=Mock(return_value=data))
                producer = Mock()
                producer.lease.run = {'operation': 'download', 'target_url': 'https://www.redgifs.com/users/Example'}
                producer.request_source.return_value = {'source_scope': 'service:redgifs'}
                def fail(code, scope):
                    raise SourceFailure(code, scope)
                producer.fail_source.side_effect = fail
                session = SourceSession(producer, Mock(request=Mock(return_value=response)))
                if expected:
                    with self.assertRaises(SourceFailure) as raised:
                        session.request('GET', url)
                    self.assertEqual(raised.exception.code, expected)
                    response.close.assert_called_once()
                else:
                    self.assertIs(session.request('GET', url), response)
                    producer.fail_source.assert_not_called()

    def test_deletion_of_another_or_embedded_account_cannot_retire_the_parent(self):
        for target in ('https://www.reddit.com/user/example/', 'https://www.redgifs.com/users/different',
                       'https://www.redgifs.com/watch/example', 'https://redgifs.com/users/example/collections'):
            with self.subTest(target=target):
                response = Mock(status_code=404, headers={}, json=Mock(return_value={'message': 'User account deleted.'}))
                producer = Mock()
                producer.lease.run = {'operation': 'download', 'target_url': target}
                producer.request_source.return_value = {'source_scope': 'service:redgifs'}
                def fail(code, scope):
                    raise SourceFailure(code, scope)
                producer.fail_source.side_effect = fail
                session = SourceSession(producer, Mock(request=Mock(return_value=response)))
                with self.assertRaises(SourceFailure) as raised:
                    session.request('GET', 'https://api.redgifs.com/v2/users/example/search')
                self.assertEqual(raised.exception.code, 'not_found')
