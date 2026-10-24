import errno
import json
import unittest
from urllib.error import HTTPError, URLError

from stash_ingest.client import Unavailable
from stash_ingest.command_errors import describe


class CommandErrorTests(unittest.TestCase):
    def test_api_failures_distinguish_retry_from_review_without_exposing_secrets(self):
        for status, retry in ((401, False), (403, False), (404, False), (409, False),
                              (408, True), (429, True), (500, True), (503, True)):
            for error in (Unavailable('private-token', status),
                          HTTPError('https://private-token@example.invalid', status, 'private-token', {}, None)):
                with self.subTest(status=status, error=type(error).__name__):
                    value = describe(error)
                    self.assertEqual(value['retryable'], retry)
                    self.assertEqual(value['http_status'], status)
                    self.assertNotIn('private-token', json.dumps(value))
                    if isinstance(error, HTTPError):error.close()

    def test_transport_storage_and_programming_failures_are_distinct(self):
        for error, code, retry in ((URLError('private-token'), 'transport_unavailable', True),
                                   (OSError(errno.ENOSPC, 'private-token'), 'storage_unavailable', True),
                                   (OSError(errno.ELOOP, 'private-token'), 'storage_unavailable', False),
                                   (RuntimeError('private-token'), 'internal_error', False)):
            value = describe(error)
            self.assertEqual((value['code'], value['retryable']), (code, retry))
            self.assertNotIn('private-token', json.dumps(value))
