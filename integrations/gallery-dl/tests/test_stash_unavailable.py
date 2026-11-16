"""A Stash restart is a skipped scheduled run, never a failure or a rejection."""

from contextlib import redirect_stderr, redirect_stdout
from email.message import Message
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from stash_ingest import dedupe_host, intake_folder
from stash_ingest.client import Unavailable, stash_unavailable
from stash_ingest.dedupe_client import DeduplicationClient
from stash_ingest.encoding import encode
from stash_ingest.intake_client import IntakeClient


def http_error(code, content_type="text/plain"):
    headers = Message()
    headers["Content-Type"] = content_type
    return HTTPError("http://127.0.0.1:8009/api", code, "error", headers, io.BytesIO(b"server going away"))


class Opener:
    def __init__(self, error):
        self.error = error

    def open(self, request, timeout):
        raise self.error


class StashUnavailableTests(unittest.TestCase):
    def clients(self, directory):
        key = Path(directory) / "key"
        key.write_text("key\n")
        key.chmod(0o600)
        return (IntakeClient("http://127.0.0.1:8009", key_file=str(key)),
                DeduplicationClient("http://127.0.0.1:8009", key_file=str(key)))

    def test_gateway_and_unavailable_statuses_are_stash_unavailable(self):
        with tempfile.TemporaryDirectory() as directory:
            for client in self.clients(directory):
                for code in (502, 503, 504):
                    with self.subTest(client=type(client).__name__, code=code):
                        client.opener = Opener(http_error(code))
                        with self.assertRaises(Unavailable) as raised:
                            client.request("GET", "/x")
                        self.assertEqual(("stash_unavailable", code), (raised.exception.code, raised.exception.status))
                        self.assertTrue(stash_unavailable(raised.exception))

    def test_dropped_connections_are_unavailable_and_other_errors_stay_rejections(self):
        with tempfile.TemporaryDirectory() as directory:
            intake, dedupe = self.clients(directory)
            for client in (intake, dedupe):
                client.opener = Opener(ConnectionResetError(104, "reset"))
                with self.assertRaises(Unavailable) as raised:
                    client.request("GET", "/x")
                self.assertTrue(stash_unavailable(raised.exception))
            for client, rejected in ((intake, "local_intake_rejected"), (dedupe, "deduplication_request_rejected")):
                for code in (400, 500):
                    with self.subTest(client=type(client).__name__, code=code):
                        client.opener = Opener(http_error(code))
                        with self.assertRaises(Unavailable) as raised:
                            client.request("GET", "/x")
                        self.assertEqual(rejected, raised.exception.code)
                        self.assertFalse(stash_unavailable(raised.exception))

    def test_scheduled_commands_skip_while_stash_is_unavailable_and_fail_otherwise(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "config.json"
            config.write_bytes(encode({}))
            for module, function, rejected in (
                    (intake_folder, "run", "local_intake_rejected"),
                    (dedupe_host, "execute", "deduplication_request_rejected")):
                load = "decode" if module is intake_folder else "load"
                for error, status, output in (
                        (Unavailable("network_unavailable"), 0, {"skipped": True, "reason": "network_unavailable"}),
                        (Unavailable("stash_unavailable", 503), 0, {"skipped": True, "reason": "stash_unavailable"}),
                        (Unavailable(rejected, 500), 1, None)):
                    with self.subTest(command=module.__name__, error=error.code), \
                            patch.object(module, load, return_value={}), \
                            patch.object(module, function, side_effect=error), \
                            redirect_stdout(io.StringIO()) as out, redirect_stderr(io.StringIO()) as err:
                        self.assertEqual(status, module.main(["--config", str(config)]))
                        if output is None:
                            self.assertEqual("", out.getvalue())
                            self.assertIn(rejected, err.getvalue())
                        else:
                            self.assertEqual(output, json.loads(out.getvalue()))


if __name__ == "__main__":
    unittest.main()
