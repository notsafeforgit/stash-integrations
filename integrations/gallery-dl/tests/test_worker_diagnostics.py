import errno
import io
import json
from contextlib import closing, redirect_stderr, redirect_stdout
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from stash_ingest.cli import main
from stash_ingest.client import Client
from stash_ingest.command_errors import describe, report
from stash_ingest.encoding import InvalidData, encode
from stash_ingest.outbox import Outbox
from stash_ingest.worker import Delivery
from helpers import PRODUCER, RUN, capture


class WorkerDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='private-path-')
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'private-outbox.sqlite'
        self.endpoint = 'http://example.invalid'
        self.args = ['--outbox', str(self.path), '--endpoint', self.endpoint,
                     '--producer', PRODUCER]

    def invoke(self, command, extra):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            status = command([*self.args, *extra])
        return status, out.getvalue(), err.getvalue()

    def diagnostic(self, stderr):
        value = json.loads(stderr)
        self.assertEqual(value['event'], 'stash_worker_error')
        for private in (str(self.path), 'private-message', 'private-token'):
            self.assertNotIn(private, stderr)
        self.assertLessEqual(len(value['frames']), 8)
        for frame in value['frames']:
            self.assertEqual(set(frame), {'module', 'function', 'line'})
            self.assertTrue(frame['module'].startswith('stash_ingest.'))
        return value

    def fast_busy_timeout(self):
        connect = sqlite3.connect

        def open_database(*args, **kwargs):
            kwargs['timeout'] = 0.03
            return connect(*args, **kwargs)

        return patch('stash_ingest.outbox.sqlite3.connect', side_effect=open_database)

    def test_cli_real_writer_contention_reports_sqlite_code_and_location_then_reopens(self):
        event = encode(capture())
        with closing(Outbox(self.path, self.endpoint, PRODUCER)) as owner:
            owner.enqueue(event)
            owner.db.execute('BEGIN IMMEDIATE')
            try:
                with self.fast_busy_timeout():
                    status, stdout, stderr = self.invoke(main, ['status'])
            finally:
                owner.db.execute('ROLLBACK')
            self.assertEqual((status, stdout), (1, ''))
            value = self.diagnostic(stderr)
            self.assertEqual((value['command'], value['stage']), ('status', 'open_outbox'))
            self.assertEqual(value['error']['sqlite_code'], sqlite3.SQLITE_BUSY)
            self.assertEqual(value['error']['sqlite_name'], 'SQLITE_BUSY')
            self.assertTrue(any(f['module'] == 'stash_ingest.outbox' and f['function'] == 'transaction'
                                for f in value['frames']))
            self.assertEqual(owner.db.execute('SELECT body FROM events').fetchone()[0], event)
        status, stdout, stderr = self.invoke(main, ['status'])
        self.assertEqual((status, stderr), (0, ''))
        self.assertEqual(json.loads(stdout)['counts']['pending'], 1)

    def test_cli_real_missing_directory_reports_errno_without_private_path(self):
        self.path = self.path / 'missing.sqlite'
        self.args[1] = str(self.path)
        status, stdout, stderr = self.invoke(main, ['status'])
        self.assertEqual((status, stdout), (1, ''))
        value = self.diagnostic(stderr)
        self.assertEqual(value['stage'], 'open_outbox')
        self.assertEqual((value['error']['errno'], value['error']['os_error']), (errno.ENOENT, 'ENOENT'))

    def test_cli_corrupt_database_reports_notadb_without_attempting_recovery(self):
        content = b'private-token not a SQLite database'
        self.path.write_bytes(content)
        status, stdout, stderr = self.invoke(main, ['status'])
        self.assertEqual((status, stdout), (1, ''))
        value = self.diagnostic(stderr)
        self.assertEqual(value['error']['sqlite_name'], 'SQLITE_NOTADB')
        self.assertFalse(value['error']['retryable'])
        self.assertEqual(self.path.read_bytes(), content)

    def test_cli_command_failure_reports_errno_and_preserves_failure_exit(self):
        error = OSError(errno.ENOSPC, 'private-message private-token', str(self.path))
        with patch('stash_ingest.cli.drain_once', side_effect=error):
            status, stdout, stderr = self.invoke(main, ['drain'])
        self.assertEqual((status, stdout), (1, ''))
        value = self.diagnostic(stderr)
        self.assertEqual((value['command'], value['stage']), ('drain', 'command'))
        self.assertEqual((value['error']['errno'], value['error']['os_error']), (errno.ENOSPC, 'ENOSPC'))

    def test_background_open_contention_reports_before_source_boundary_and_retains_event(self):
        event = encode(capture())
        client = Client(self.endpoint, PRODUCER, timeout=1)
        with closing(Outbox(self.path, self.endpoint, PRODUCER)) as owner:
            owner.enqueue(event)
            owner.db.execute('BEGIN IMMEDIATE')
            stderr = io.StringIO()
            try:
                with self.fast_busy_timeout(), redirect_stderr(stderr):
                    delivery = Delivery(owner, client, run_uuid=RUN)
                    delivery.start()
                    delivery.thread.join(5)
                    self.assertFalse(delivery.thread.is_alive())
                    delivery.close()
            finally:
                owner.db.execute('ROLLBACK')
            value = self.diagnostic(stderr.getvalue())
            self.assertEqual(value['stage'], 'delivery_open_outbox')
            self.assertEqual(value['run_uuid'], RUN)
            self.assertEqual(value['error']['sqlite_name'], 'SQLITE_BUSY')
            self.assertEqual(delivery.failure, 'outbox_delivery_failed')
            with self.assertRaises(InvalidData):
                delivery.check()
            self.assertEqual(owner.db.execute('SELECT body FROM events').fetchone()[0], event)

    def test_background_drain_failure_reports_its_cause_and_run_identity(self):
        client = Client(self.endpoint, PRODUCER, timeout=1)
        error = PermissionError(errno.EACCES, 'private-message private-token', str(self.path))
        with closing(Outbox(self.path, self.endpoint, PRODUCER)) as box:
            stderr = io.StringIO()
            with patch('stash_ingest.worker.drain_once', side_effect=error), redirect_stderr(stderr):
                delivery = Delivery(box, client, run_uuid=RUN)
                delivery.start()
                delivery.thread.join(5)
                self.assertFalse(delivery.thread.is_alive())
                delivery.close()
            value = self.diagnostic(stderr.getvalue())
            self.assertEqual((value['stage'], value['run_uuid']), ('delivery_drain', RUN))
            self.assertEqual(value['error']['os_error'], 'EACCES')
            self.assertEqual(delivery.failure, 'outbox_delivery_failed')

    def test_extended_sqlite_code_is_not_collapsed_to_primary_code(self):
        with closing(sqlite3.connect(':memory:')) as db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('CREATE TABLE parent(id PRIMARY KEY)')
            db.execute('CREATE TABLE child(id REFERENCES parent(id))')
            try:
                db.execute('INSERT INTO child VALUES(?)', ('private-token',))
            except sqlite3.Error as error:
                value = describe(error)
            else:
                self.fail('Expected a real foreign-key constraint failure')
        self.assertEqual(value['sqlite_code'], sqlite3.SQLITE_CONSTRAINT_FOREIGNKEY)
        self.assertEqual(value['sqlite_name'], 'SQLITE_CONSTRAINT_FOREIGNKEY')
        self.assertFalse(value['retryable'])
        self.assertNotIn('private-token', json.dumps(value))

    def test_unexpected_exception_report_excludes_message_cause_and_local_values(self):
        stderr = io.StringIO()
        try:
            try:
                raise OSError(errno.EACCES, 'private-token', str(self.path))
            except OSError as cause:
                raise RuntimeError('private-message') from cause
        except RuntimeError as error:
            with redirect_stderr(stderr):
                report(error, stage='delivery_drain')
        value = self.diagnostic(stderr.getvalue())
        self.assertEqual(value['error']['type'], 'RuntimeError')
        self.assertEqual(value['error']['code'], 'internal_error')
