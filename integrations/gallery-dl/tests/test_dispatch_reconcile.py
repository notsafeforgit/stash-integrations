from contextlib import closing
import copy
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from stash_ingest.collection_dispatch import CollectionDispatcher
from stash_ingest.dispatch_reconcile import inventory, reconcile
from stash_ingest.encoding import InvalidData
from stash_ingest.enrichment_configuration import EnrichmentConfiguration, SCHEMA
from stash_ingest.enrichment_dispatch import EnrichmentDispatcher
from stash_ingest.enrichment_journal import EnrichmentJournal
from stash_ingest.outbox import Outbox, Conflict
from helpers import PRODUCER
from test_enrichment_execution import execution_fixture
import test_enrichment_dispatch as fixtures


class DispatchReconcileTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.profiles = self.root / 'profiles'
        self.profiles.mkdir()
        self.profile = self.profiles / 'metadata.json'
        self.document = {'schema': SCHEMA, 'source_category': 'reddit', 'bindings': {}, 'gallery': {}}
        self.profile.write_text(json.dumps(self.document))
        self.current = EnrichmentConfiguration(self.profile).policy_sha256
        self.transport = SimpleNamespace(endpoint='http://fixture.invalid', producer=PRODUCER)
        self.collection = str(uuid.uuid4())
        self.now = 1000
        self.box = Outbox(self.root / 'outbox.sqlite', self.transport.endpoint, PRODUCER, clock=lambda: self.now)
        self.addCleanup(self.box.close)
        self.native = sqlite3.connect(':memory:')
        self.native.row_factory = sqlite3.Row
        self.addCleanup(self.native.close)
        self.native.executescript('''
CREATE TABLE native_schema(singleton INTEGER PRIMARY KEY,lineage TEXT);
INSERT INTO native_schema VALUES(1,'org.notsafeforgit.stash.native-archive');
CREATE TABLE ingest_producers(uuid TEXT PRIMARY KEY);
CREATE TABLE archive_jobs(uuid TEXT PRIMARY KEY,kind TEXT,state TEXT,arguments TEXT,fence INTEGER,result TEXT,error_code TEXT);
CREATE TABLE enrichment_checkpoint_receipts(job_uuid TEXT,revision INTEGER,digest TEXT,fence INTEGER,record_count INTEGER,pending_count INTEGER,unresolved_count INTEGER);
CREATE TABLE enrichment_publications(job_uuid TEXT,checkpoint_revision INTEGER,fence INTEGER,completion_uuid TEXT);
CREATE TABLE enrichment_completions(uuid TEXT,target_uuid TEXT,expected_revision INTEGER,capture_count INTEGER);
CREATE TABLE metadata_worker_policy_upgrades(id INTEGER PRIMARY KEY,kind TEXT,original_policy_sha256 TEXT,policy_sha256 TEXT,request_uuid TEXT);
CREATE TABLE metadata_worker_attempt_policies(job_uuid TEXT,fence INTEGER,policy_sha256 TEXT);
''')
        self.native.execute('INSERT INTO ingest_producers VALUES(?)', (PRODUCER,))

    def cursor(self, policy, **progress):
        value = EnrichmentDispatcher(self.box, self.transport, str(uuid.uuid4()), SimpleNamespace(policy_sha256=policy))
        value.save(value.state(), **progress)
        CollectionDispatcher(self.box, self.transport, SimpleNamespace(policy_sha256=policy))
        return value

    def native_job(self, job, policy, state='queued'):
        arguments = copy.deepcopy(execution_fixture()['job']['arguments'])
        arguments['policy_sha256'] = policy
        self.native.execute("INSERT INTO archive_jobs(uuid,kind,state,arguments,fence) VALUES(?,'post.enrich',?,?,1)", (job, state, json.dumps(arguments)))
        return arguments

    def approval(self, old, new):
        self.native.execute("INSERT INTO metadata_worker_policy_upgrades(kind,original_policy_sha256,policy_sha256,request_uuid) VALUES('post.enrich',?,?,?)", (old, new, str(uuid.uuid4())))

    def run_cleanup(self, apply=True, directories=None):
        return reconcile(self.box, self.native, directories or [self.profiles], apply=apply)

    def test_obsolete_partial_cursors_retire_but_current_and_delivery_cursors_survive(self):
        old = self.cursor('a' * 64, job_after=123, failures=2, error_code='network_unavailable')
        current = self.cursor(self.current, job_after=456)
        delivery = self.cursor('0' * 64, local_after=str(uuid.uuid4()))
        before = '\n'.join(self.box.db.iterdump())
        preview = self.run_cleanup(apply=False)
        self.assertEqual(preview['cursors']['enrichment_dispatch']['eligible'], 1)
        self.assertEqual('\n'.join(self.box.db.iterdump()), before)
        preserved = current.state(), delivery.state()
        result = self.run_cleanup()
        self.assertEqual(result['retired_policies'], ['a' * 64])
        self.assertIsNone(old.state())
        self.assertEqual((current.state(), delivery.state()), preserved)
        self.assertEqual(self.run_cleanup()['retired_policies'], [])

    def test_queued_native_jobs_without_local_journals_protect_their_execution_policy(self):
        old = self.cursor('a' * 64)
        self.native_job(str(uuid.uuid4()), 'a' * 64)
        self.assertEqual(self.run_cleanup()['protected_policies'], ['a' * 64])
        self.approval('a' * 64, 'b' * 64)
        self.assertEqual(self.run_cleanup()['protected_policies'], ['a' * 64])
        self.approval('a' * 64, self.current)
        self.assertEqual(self.run_cleanup()['retired_policies'], ['a' * 64])
        self.assertIsNone(old.state())

    def test_approved_queued_job_can_release_original_bookmark_without_editing_its_journal(self):
        fixture = execution_fixture()
        fixture['job']['arguments']['policy_sha256'] = 'a' * 64
        journal = EnrichmentJournal(self.box)
        with journal.execution():
            value = journal.prepare(fixture)
        self.native.execute("INSERT INTO archive_jobs(uuid,kind,state,arguments,fence) VALUES(?,'post.enrich','queued',?,0)",
                            (value.job_uuid, json.dumps(value.definition['arguments'])))
        old = self.cursor('a' * 64, job_after=321)
        self.approval('a' * 64, self.current)
        before = tuple(self.box.db.execute('SELECT * FROM enrichment_executions').fetchone())
        self.assertEqual(self.run_cleanup()['retired_policies'], ['a' * 64])
        self.assertIsNone(old.state())
        self.assertEqual(tuple(self.box.db.execute('SELECT * FROM enrichment_executions').fetchone()), before)

    def test_pending_delivery_and_review_remain_protected_after_upgrade(self):
        fixtures.EnrichmentDispatchTests.stage(self, self.box, policy='a' * 64)
        self.cursor('a' * 64)
        self.approval('a' * 64, self.current)
        before = tuple(self.box.db.execute('SELECT * FROM enrichment_executions').fetchone())
        self.assertEqual(self.run_cleanup()['protection_reasons'], {'pending_delivery': 1})
        self.assertEqual(tuple(self.box.db.execute('SELECT * FROM enrichment_executions').fetchone()), before)
        self.box.db.execute("UPDATE enrichment_executions SET phase='review'")
        self.assertEqual(self.run_cleanup()['protection_reasons'], {'review': 1})

    def test_unresolved_or_mismatched_native_job_preserves_original_progress(self):
        fixture = execution_fixture()
        fixture['job']['arguments']['policy_sha256'] = 'a' * 64
        journal = EnrichmentJournal(self.box)
        with journal.execution():
            value = journal.prepare(fixture)
        self.cursor('a' * 64)
        self.approval('a' * 64, self.current)
        self.assertEqual(self.run_cleanup()['protection_reasons'], {'native_job_missing': 1})
        self.native_job(value.job_uuid, 'a' * 64)
        self.assertEqual(self.run_cleanup()['protection_reasons'], {'native_job_mismatch': 1})

    def test_running_attempt_cannot_be_retired_by_a_later_policy_mapping(self):
        self.cursor('a' * 64)
        job = str(uuid.uuid4())
        self.native_job(job, 'a' * 64, 'running')
        self.native.execute('INSERT INTO metadata_worker_attempt_policies VALUES(?,1,?)', (job, 'a' * 64))
        self.approval('a' * 64, self.current)
        self.assertEqual(self.run_cleanup()['protected_policies'], ['a' * 64])

    def test_finished_upgraded_native_job_does_not_pin_a_stale_local_cursor(self):
        fixture = execution_fixture()
        fixture['job']['arguments']['policy_sha256'] = 'a' * 64
        journal = EnrichmentJournal(self.box)
        with journal.execution():
            value = journal.prepare(fixture)
        self.native.execute("INSERT INTO archive_jobs(uuid,kind,state,arguments,fence) VALUES(?,'post.enrich','failed',?,1)",
                            (value.job_uuid, json.dumps(value.definition['arguments'])))
        self.cursor('a' * 64)
        self.approval('a' * 64, self.current)
        before = tuple(self.box.db.execute('SELECT * FROM enrichment_executions').fetchone())
        self.assertEqual(self.run_cleanup()['retired_policies'], ['a' * 64])
        reconciled = journal.find(value.job_uuid)
        self.assertEqual(reconciled.phase, 'failed')
        self.assertEqual(reconciled.state['native_outcome'], 'failed')
        self.assertEqual(reconciled.definition, value.definition)
        self.assertEqual(self.run_cleanup()['executions']['eligible'], 0)

    def test_all_shared_directories_and_external_dispatcher_references_are_preserved(self):
        second = self.root / 'second'
        second.mkdir()
        other = second / 'other.json'
        other.write_text(json.dumps({**self.document, 'source_category': 'twitter'}))
        policy = EnrichmentConfiguration(other).policy_sha256
        self.cursor(policy)
        self.assertEqual(self.run_cleanup(directories=[self.profiles, second])['retired_policies'], [])
        dispatch = self.profiles / 'dispatch.json'
        dispatch.write_text(json.dumps({'schema': 'stash-gallery-dispatch-v1', 'uuid': str(uuid.uuid4()),
            'profiles': [{'id': 'metadata', 'operation': 'post.enrich', 'profile': str(other)}]}))
        self.assertEqual(self.run_cleanup()['retired_policies'], [])
        other.unlink()
        with self.assertRaises(FileNotFoundError):
            self.run_cleanup()

    def test_plain_gallery_layers_are_inventoried_but_unknown_profile_schemas_block_cleanup(self):
        self.cursor('a' * 64)
        layer = self.profiles / 'layer.json'
        layer.write_text(json.dumps({'extractor': {'sleep-request': 7}}))
        self.assertEqual(self.run_cleanup(apply=False)['retired_policies'], ['a' * 64])
        layer.write_text(json.dumps({'schema': 'stash-gallery-enrichment-unknown'}))
        with self.assertRaises(InvalidData):
            self.run_cleanup()

    def test_bad_empty_or_changing_inventory_rolls_back_retirement(self):
        self.cursor('a' * 64)
        before = '\n'.join(self.box.db.iterdump())
        original_inventory = inventory
        count = 0

        def changed(directories):
            nonlocal count
            count += 1
            if count == 2:
                self.profile.write_text(json.dumps({**self.document, 'source_category': 'twitter'}))
            return original_inventory(directories)

        with patch('stash_ingest.dispatch_reconcile.inventory', side_effect=changed), self.assertRaises(InvalidData):
            self.run_cleanup()
        self.assertEqual('\n'.join(self.box.db.iterdump()), before)
        self.profile.write_text('{broken')
        with self.assertRaises(InvalidData):
            self.run_cleanup()
        self.profile.unlink()
        with self.assertRaises(InvalidData):
            self.run_cleanup()

    def test_other_producer_and_metadata_lock_contention_prevent_retirement(self):
        self.cursor('a' * 64)
        with closing(Outbox(self.box.path, self.transport.endpoint, PRODUCER)) as peer:
            with EnrichmentJournal(peer).execution():
                self.assertEqual(self.run_cleanup(), {'state': 'waiting', 'reason': 'metadata_execution_active'})
        self.native.execute('DELETE FROM ingest_producers')
        with self.assertRaises(InvalidData):
            self.run_cleanup()

    def test_inventory_matches_policy_without_loading_unmounted_private_bindings(self):
        private = self.root / 'private.json'
        private.write_text(json.dumps({'cookies': 'first-secret'}))
        value = {**self.document, 'bindings': {'login': {'kind': 'private', 'file': str(private), 'pointer': '/cookies'}},
                 'gallery': {'extractor': {'reddit': {'cookies': '${stash:login}'}}}}
        self.profile.write_text(json.dumps(value))
        expected = EnrichmentConfiguration(self.profile).policy_sha256
        private.unlink()
        self.assertEqual(EnrichmentConfiguration.inventory_policy(self.profile), expected)
        self.assertEqual(inventory([self.profiles])['policies'], [expected])
        value['bindings']['login']['pointer'] = '/bad~escape'
        self.profile.write_text(json.dumps(value))
        with self.assertRaises(InvalidData):
            inventory([self.profiles])
        value['bindings']['login'] = {'kind': 'private', 'env': 'MISSING_DISPATCH_TEST_LOGIN'}
        self.profile.write_text(json.dumps(value))
        with patch.dict(os.environ, {'MISSING_DISPATCH_TEST_LOGIN': 'secret'}):
            expected = EnrichmentConfiguration(self.profile).policy_sha256
        self.assertEqual(EnrichmentConfiguration.inventory_policy(self.profile), expected)


if __name__ == '__main__':
    unittest.main()

class TerminalExecutionReconcileTests(DispatchReconcileTests):
    def terminal(self, state='succeeded', phase='active', pending=False):
        fixture = execution_fixture()
        fixture['job']['arguments']['policy_sha256'] = self.current
        journal = EnrichmentJournal(self.box)
        with journal.execution():
            value = journal.prepare(fixture)
            receipt = {'job_uuid': value.job_uuid, 'revision': 1, 'sha256': 'd' * 64, 'fence': 1,
                       'record_count': 2, 'pending_count': 0, 'unresolved_count': 0}
            intent = {'kind': 'publish', 'receipt': receipt, 'lease': {'fence': 1, 'owner_uuid': str(uuid.uuid4())}}
            value = journal.change(value, phase=phase, state={**value.state, 'checkpoint': receipt,
                                    'pending': intent if pending else None})
        work = value.definition['arguments']
        publication = {'job_uuid': value.job_uuid, 'checkpoint_revision': 1, 'checkpoint_sha256': 'd' * 64,
                       'fence': 1, 'record_count': 2, 'capture_count': 2, 'unresolved_count': 0,
                       'completion_uuid': str(uuid.uuid4())}
        self.native.execute("INSERT INTO archive_jobs VALUES(?,'post.enrich',?,?,1,?,?)",
                            (value.job_uuid, state, json.dumps(work), json.dumps(publication), 'post_identity_conflict' if state == 'failed' else ''))
        self.native.execute('INSERT INTO enrichment_checkpoint_receipts VALUES(?,1,?,1,2,0,0)', (value.job_uuid, 'd' * 64))
        if state == 'succeeded':
            self.native.execute('INSERT INTO enrichment_publications VALUES(?,1,1,?)', (value.job_uuid, publication['completion_uuid']))
            self.native.execute('INSERT INTO enrichment_completions VALUES(?,?,1,2)', (publication['completion_uuid'], work['target_uuid']))
        return journal, value, publication

    def test_native_success_recovers_lost_response_under_current_policy_without_deleting_evidence(self):
        journal, value, publication = self.terminal(pending=True, phase='review')
        original = '\n'.join(self.box.db.iterdump())
        native = '\n'.join(self.native.iterdump())
        preview = self.run_cleanup(apply=False)
        self.assertEqual(preview['executions']['eligible'], 1)
        self.assertEqual(preview['executions']['reconciled'], 0)
        self.assertEqual('\n'.join(self.box.db.iterdump()), original)
        result = self.run_cleanup()
        after = journal.find(value.job_uuid)
        self.assertEqual(result['executions']['native_outcomes'], {'succeeded': 1})
        self.assertEqual(after.phase, 'completed')
        self.assertEqual(after.state['publication'], publication)
        self.assertEqual(after.state['checkpoint'], value.state['checkpoint'])
        self.assertIsNone(after.state['pending'])
        self.assertEqual('\n'.join(self.native.iterdump()), native)
        self.assertEqual(self.run_cleanup()['executions']['reconciled'], 0)
        self.assertEqual(journal.find(value.job_uuid), after)

    def test_failed_and_cancelled_review_attempts_remain_failed(self):
        for state in ('failed', 'cancelled'):
            with self.subTest(state=state):
                journal, value, _ = self.terminal(state, 'review', pending=True)
                self.run_cleanup()
                after = journal.find(value.job_uuid)
                self.assertEqual(after.phase, 'failed')
                self.assertEqual(after.state['native_outcome'], state)
                self.assertEqual(after.state['error_code'], 'native_job_' + state)
                self.assertEqual(after.state['checkpoint'], value.state['checkpoint'])
                self.assertIsNone(after.state['publication'])
                self.assertIsNone(after.state['pending'])

    def test_missing_or_mismatched_publication_never_manufactures_success(self):
        journal, value, _ = self.terminal()
        for sql in ('UPDATE enrichment_completions SET target_uuid="unrelated"',
                    'DELETE FROM enrichment_publications'):
            self.native.execute(sql)
            self.assertEqual(self.run_cleanup()['executions']['protected'], {'publication_not_confirmed': 1})
            self.assertEqual(journal.find(value.job_uuid), value)

    def test_terminal_job_cannot_discard_unacknowledged_body_or_reservation(self):
        value = fixtures.EnrichmentDispatchTests.stage(self, self.box, policy=self.current)
        journal = EnrichmentJournal(self.box)
        self.native.execute("INSERT INTO archive_jobs(uuid,kind,state,arguments,fence) VALUES(?,'post.enrich','failed',?,1)",
                            (value.job_uuid, json.dumps(value.definition['arguments'])))
        self.assertEqual(self.run_cleanup()['executions']['protected'], {'undelivered_payload_or_reservation': 1})
        self.assertEqual(journal.find(value.job_uuid), value)
        with journal.execution():
            reserved = journal.reserve(journal.prepare(execution_fixture()))
        self.native.execute("INSERT INTO archive_jobs(uuid,kind,state,arguments,fence) VALUES(?,'post.enrich','cancelled',?,1)",
                            (reserved.job_uuid, json.dumps(reserved.definition['arguments'])))
        self.assertEqual(self.run_cleanup()['executions']['protected'], {'undelivered_payload_or_reservation': 2})
        self.assertEqual(journal.find(reserved.job_uuid), reserved)

    def test_unknown_or_unconfirmed_delivery_intent_remains_protected(self):
        journal, value, _ = self.terminal('failed', pending=True)
        self.native.execute('UPDATE enrichment_checkpoint_receipts SET digest=?', ('f' * 64,))
        self.assertEqual(self.run_cleanup()['executions']['protected'], {'checkpoint_not_confirmed': 1})
        self.assertEqual(journal.find(value.job_uuid), value)
        with journal.execution():
            value = journal.change(value, state={**value.state, 'pending': {'kind': 'future_operation'}})
        self.assertEqual(self.run_cleanup()['executions']['protected'], {'unknown_delivery_intent': 1})
        self.assertEqual(journal.find(value.job_uuid), value)

    def test_inventory_change_rolls_back_local_reconciliation_and_cursors_together(self):
        journal, value, _ = self.terminal('failed', 'review', pending=True)
        self.cursor('a' * 64)
        before = '\n'.join(self.box.db.iterdump())
        snapshot = inventory([self.profiles])
        with patch('stash_ingest.dispatch_reconcile.inventory', side_effect=[snapshot, {**snapshot, 'policies': []}]):
            with self.assertRaises(InvalidData):
                self.run_cleanup()
        self.assertEqual('\n'.join(self.box.db.iterdump()), before)
        self.assertEqual(journal.find(value.job_uuid), value)

    def test_journal_validation_failure_rolls_back_the_entire_batch(self):
        journal, value, _ = self.terminal('failed')
        self.terminal('succeeded')
        before = '\n'.join(self.box.db.iterdump())
        change = EnrichmentJournal.change_in_transaction
        calls = 0
        def interrupted(instance, *args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise Conflict('simulated concurrent update')
            return change(instance, *args, **kwargs)
        with patch.object(EnrichmentJournal, 'change_in_transaction', interrupted):
            with self.assertRaises(Conflict):
                self.run_cleanup()
        self.assertEqual('\n'.join(self.box.db.iterdump()), before)
