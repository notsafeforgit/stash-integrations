"""Real producer journal transitions against native acknowledgement table DDL."""

from contextlib import closing
from datetime import datetime
import hashlib
import json
import re
import sqlite3
import sys
import uuid

from stash_ingest.enrichment_journal import EnrichmentJournal
from stash_ingest.encoding import decode, encode
from stash_archive.job_receipts import instant, verify_job_journals
from stash_archive.storage import InvalidArchive
from test_receipts import ReceiptFixture, ROOT, PRODUCER

# Reuse the producer's contract fixture; importing it does not run its tests.
sys.path.insert(0, str(ROOT / "integrations/gallery-dl/tests"))
try:
    from test_enrichment_execution import execution_fixture as enrichment_fixture
finally:
    sys.path.pop(0)

WHEN = "2026-10-04T14:00:00Z"
WHEN_MS = int(datetime.fromisoformat(WHEN).timestamp()) * 1000
JOURNAL_COLUMNS = ("job_uuid TEXT PRIMARY KEY,revision INTEGER,phase TEXT,definition_sha256 TEXT,state_sha256 TEXT,"
                   "body_sha256 TEXT,reserved_bytes INTEGER,definition BLOB,state BLOB,body BLOB")
EMPTY_SHA256 = hashlib.sha256().hexdigest()


class JournalFixture(ReceiptFixture):
    def setUp(self):
        super().setUp()
        # Supporting domain parents are deliberately small, as in ReceiptFixture;
        # the acknowledgement tables below use the actual native migration DDL.
        for field in ("kind TEXT", "arguments TEXT", "state TEXT", "fence INTEGER", "revision INTEGER"):
            self.db.execute("ALTER TABLE archive_jobs ADD COLUMN " + field)
        self.db.executescript("""
            CREATE TABLE enrichment_job_targets(job_uuid TEXT PRIMARY KEY);
            CREATE TABLE enrichment_targets(uuid TEXT PRIMARY KEY,url_uuid TEXT,policy TEXT);
            CREATE TABLE source_post_urls(uuid TEXT PRIMARY KEY,url TEXT);
            CREATE TABLE enrichment_completions(uuid TEXT PRIMARY KEY,capture_count INTEGER);
        """)
        migrations = {
            "1000019_archive_jobs.up.sql": ("archive_job_attempts",),
            "1000051_enrichment_jobs.up.sql": ("enrichment_job_attempts", "enrichment_checkpoint_receipts", "enrichment_checkpoints"),
            "1000052_enrichment_publications.up.sql": ("enrichment_publications",),
            "1000053_enrichment_checkpoint_release.up.sql": ("enrichment_checkpoint_releases",),
        }
        for name, tables in migrations.items():
            source = (ROOT / "pkg/sqlite/migrations" / name).read_text()
            for table in tables:
                ddl = re.search(r"CREATE TABLE " + table + r" \([\s\S]*?\n\);", source)
                self.assertIsNotNone(ddl, table)
                self.db.executescript(ddl.group())
        self.bodies = decode((ROOT / "pkg/archive/testdata/enrichment-transcript-v1.json").read_bytes(), preserve_numbers=True)
        self.db.commit()

    def prepare(self, *, claimed=True, claim_only=False):
        journal = EnrichmentJournal(self.box)
        execution, body = enrichment_fixture(), self.bodies["complete"]
        target, url_id = execution["target"], str(uuid.uuid4())
        self.db.execute("INSERT INTO source_post_urls VALUES(?,?)", (url_id, target["url"]))
        self.db.execute("INSERT INTO enrichment_targets VALUES(?,?,?)", (target["uuid"], url_id, target["policy"]))
        job = execution["job"]
        self.db.execute("INSERT INTO archive_jobs VALUES(?,?,?,?,?,?)", (job["uuid"], job["kind"], json.dumps(job["arguments"]), "queued", 0, 1))
        self.db.execute("INSERT INTO enrichment_job_targets VALUES(?)", (job["uuid"],))
        with journal.execution():
            value = journal.prepare(execution)
            if claimed or claim_only:
                value = journal.claim(value, job)
            if claimed:
                lease = {**job, "state":"running", "revision":2, "fence":1,
                         "owner_uuid":value.state["claim"]["owner_uuid"], "lease_until":"2026-10-04T14:03:00Z"}
                value = journal.claimed(value, lease)
                self.db.execute("UPDATE archive_jobs SET state='running',fence=1,revision=2 WHERE uuid=?", (job["uuid"],))
                self.db.execute("INSERT INTO archive_job_attempts(job_uuid,fence,owner_uuid,started_at_ms) VALUES(?,1,?,?)",
                                (job["uuid"], lease["owner_uuid"], WHEN_MS))
                self.db.execute("INSERT INTO enrichment_job_attempts VALUES(?,1,?)", (job["uuid"], PRODUCER))
        self.db.commit()
        return journal, value, body

    def stage(self, *, body=None):
        journal, value, default = self.prepare()
        with journal.execution():
            value = journal.reserve(value)
            value = journal.checkpoint(value, value.state["lease"], 0, body or default)
        return journal, value

    def receive(self, value, *, revision=1, raw=None):
        raw = value.body if raw is None else raw
        body = decode(raw, preserve_numbers=True)
        receipt = {"job_uuid":value.job_uuid, "fence":1, "sha256":hashlib.sha256(raw).hexdigest(),
                   "record_count":len(body["records"]), "created_at":WHEN}
        receipt.update(revision=revision, pending_count=len(body["pending"]), unresolved_count=len(body["unresolved"]))
        self.db.execute("INSERT INTO enrichment_checkpoint_receipts VALUES(?,?,?,?,?,?,?,?)", (
            value.job_uuid,revision,receipt["sha256"],1,receipt["record_count"],receipt["pending_count"],receipt["unresolved_count"],WHEN))
        self.db.execute("INSERT OR REPLACE INTO enrichment_checkpoints VALUES(?,?,?,?)", (value.job_uuid,revision,raw.decode(),len(raw)))
        self.db.commit()
        return receipt

    def succeed(self, job):
        self.db.execute("UPDATE archive_jobs SET state='succeeded',revision=revision+1 WHERE uuid=?", (job,))
        self.db.execute("UPDATE archive_job_attempts SET outcome='succeeded',ended_at_ms=? WHERE job_uuid=?", (WHEN_MS + 1000, job))

    def finish(self, journal, value, receipt):
        with journal.execution():
            value = journal.acknowledged(value, receipt)
            completed = {"job_uuid":value.job_uuid, "checkpoint_revision":receipt["revision"], "fence":1,
                "checkpoint_sha256":receipt["sha256"], "completion_uuid":str(uuid.uuid4()),
                "record_count":receipt["record_count"], "capture_count":receipt["record_count"],
                "unresolved_count":receipt["unresolved_count"], "created_at":WHEN}
            self.db.execute("INSERT INTO enrichment_completions VALUES(?,?)", (completed["completion_uuid"],completed["capture_count"]))
            self.db.execute("INSERT INTO enrichment_publications VALUES(?,?,?,?,?)", (value.job_uuid,receipt["revision"],1,completed["completion_uuid"],WHEN))
            self.succeed(value.job_uuid)
            self.db.commit()
            return journal.acknowledged(value, completed)

    def counts(self, **kwargs):
        return self.verify(**kwargs)["producers"][0]["job_journals"]["enrichment"]["counts"]


class JobReceiptTests(JournalFixture):
    def test_all_completed_journals_match_after_bodies_are_released_without_writes(self):
        journal, value = self.stage()
        receipt = self.receive(value)
        completed = self.finish(journal, value, receipt)
        self.assertIsNone(completed.body)
        before = list(self.box.db.iterdump())
        report = self.verify()
        self.assertEqual(report["producers"][0]["job_journals"]["enrichment"]["counts"]["completions"], 1)
        self.assertEqual(before, list(self.box.db.iterdump()))
        self.assertEqual(report, self.verify())
        self.assertEqual(self.db.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_lost_replies_keep_exact_bodies_and_original_attempts(self):
        journal, value = self.stage()
        self.receive(value)
        report = self.counts()
        self.assertEqual(report["accepted_pending_bodies"], 1)
        self.assertEqual(report["pending_bodies"], 1)
        self.assertEqual(journal.find(value.job_uuid), value)

    def test_old_native_snapshot_cannot_cover_discarded_job_payloads(self):
        journal, value = self.stage()
        old = self.root / "old.sqlite"
        with closing(sqlite3.connect(old)) as target:
            self.db.backup(target)
        receipt = self.receive(value)
        with journal.execution():
            journal.acknowledged(value, receipt)
        with self.assertRaisesRegex(InvalidArchive, "checkpoint is missing"):
            self.verify(library=old)
        self.verify()

    def test_pending_claims_need_no_invented_attempt(self):
        _, value, _ = self.prepare(claimed=False, claim_only=True)
        self.assertIsNotNone(value.state["claim"])
        self.assertIsNone(value.state["lease"])
        self.assertEqual(self.counts()["active"], 1)

    def test_acknowledged_leases_require_native_attempts(self):
        _, staged = self.stage()
        self.db.execute("DELETE FROM enrichment_job_attempts WHERE job_uuid=?", (staged.job_uuid,))
        self.db.commit()
        with self.assertRaisesRegex(InvalidArchive, "attempt is missing"):
            self.verify()

    def test_changed_native_identity_owner_and_pending_bytes_are_rejected(self):
        journal, value = self.stage()
        owner = value.state["lease"]["owner_uuid"]
        self.db.execute("UPDATE archive_job_attempts SET owner_uuid=?", (str(uuid.uuid4()),))
        self.db.commit()
        with self.assertRaisesRegex(InvalidArchive, "another owner"):
            self.verify()
        self.db.execute("UPDATE archive_job_attempts SET owner_uuid=?", (owner,))
        self.db.commit()
        self.box.db.execute("UPDATE enrichment_executions SET body=zeroblob(length(body))")
        with self.assertRaisesRegex(InvalidArchive, "exact retained bytes"):
            self.verify()

    def test_older_checkpoint_acknowledgement_survives_newer_native_head(self):
        journal, value = self.stage(body=self.bodies["initial"])
        first = self.receive(value)
        with journal.execution():
            acknowledged = journal.acknowledged(value, first)
        from stash_ingest.enrichment_client import checkpoint_bytes
        self.receive(value, revision=2, raw=checkpoint_bytes(self.bodies["complete"]))
        self.assertEqual(self.counts()["checkpoints"], 1)
        self.assertEqual(journal.find(value.job_uuid), acknowledged)

    def test_metadata_checkpoint_requires_retained_body_or_native_release(self):
        journal, value = self.stage()
        receipt = self.receive(value)
        self.finish(journal, value, receipt)
        self.db.execute("DELETE FROM enrichment_checkpoints")
        self.db.commit()
        with self.assertRaisesRegex(InvalidArchive, "no retained body or release"):
            self.verify()
        self.db.execute("INSERT INTO enrichment_checkpoint_releases VALUES(?,1,?,?,?,?)", (value.job_uuid,"a"*64,len(value.body),'[]',WHEN))
        self.db.commit()
        self.assertEqual(self.counts()["completions"], 1)

    def test_timestamps_keep_nanoseconds_and_normalize_timezones(self):
        self.assertEqual(instant("2026-10-04T07:00:00.123456789-07:00"), instant("2026-10-04 14:00:00.123456789", native=True))
        self.assertNotEqual(instant("2026-10-04T14:00:00.123456789Z"), instant("2026-10-04T14:00:00.123456788Z"))

    def test_failure_acknowledgements_preserve_retry_and_reject_changed_history(self):
        journal, value, _ = self.prepare()
        lease = value.state["lease"]
        with journal.execution():
            value = journal.intent(value, lease, "failure", {"error_code":"timeout"})
            receipt = {"job_uuid":value.job_uuid, "producer_uuid":PRODUCER, "owner_uuid":lease["owner_uuid"],
                "fence":1, "outcome":"retry", "error_code":"timeout", "result":{},
                "started_at":WHEN, "ended_at":"2026-10-04T14:00:01Z"}
            self.db.execute("UPDATE archive_job_attempts SET outcome='retry',error_code='timeout',ended_at_ms=? WHERE job_uuid=?",
                            (WHEN_MS + 1000, value.job_uuid))
            self.db.execute("UPDATE archive_jobs SET state='queued',revision=3 WHERE uuid=?", (value.job_uuid,))
            self.db.commit()
            value = journal.acknowledged(value, receipt)
        self.assertEqual(self.counts()["failures"], 1)
        self.assertEqual(value.phase, "active")
        self.assertIsNone(value.state["lease"])
        self.db.execute("UPDATE archive_job_attempts SET ended_at_ms=ended_at_ms+1 WHERE job_uuid=?", (value.job_uuid,))
        self.db.commit()
        with self.assertRaisesRegex(InvalidArchive, "time differs"):
            self.verify()
        self.db.execute("UPDATE archive_job_attempts SET ended_at_ms=ended_at_ms-1 WHERE job_uuid=?", (value.job_uuid,))
        self.db.commit()

    def test_schema_downgrade_cannot_hide_journal_acknowledgements(self):
        self.box.db.execute("PRAGMA user_version=13")
        with self.assertRaisesRegex(InvalidArchive, "omit or invent"):
            self.verify()


class JobRetirementTests(ReceiptFixture):
    """Schema 26 retires the discovery journals; schemas 11..25 still retain them."""

    def journals(self, version, tables):
        queue = sqlite3.connect(self.root / f"journal-{uuid.uuid4().hex}.sqlite")
        self.addCleanup(queue.close)
        for table in tables:
            queue.execute(f"CREATE TABLE {table}({JOURNAL_COLUMNS})")
        queue.execute(f"PRAGMA user_version={version}")
        return verify_job_journals(self.db, queue, PRODUCER, version)

    def assertZeroDiscovery(self, report):
        for family in ("discovery", "discovery_detail"):
            self.assertEqual(set(report[family]["counts"].values()), {0})
            self.assertEqual(report[family]["boundary_sha256"], EMPTY_SHA256)

    def test_version_26_accepts_queue_without_discovery_journals(self):
        self.assertZeroDiscovery(self.journals(26, ["enrichment_executions"]))

    def test_version_26_rejects_retained_discovery_journal_tables(self):
        for table in ("discovery_executions", "discovery_detail_executions"):
            with self.subTest(table=table), self.assertRaisesRegex(InvalidArchive, "omit or invent"):
                self.journals(26, ["enrichment_executions", table])

    def test_version_26_still_requires_enrichment_journal(self):
        with self.assertRaisesRegex(InvalidArchive, "omit or invent"):
            self.journals(26, [])

    def test_version_26_never_reads_native_discovery_jobs(self):
        # The fixture's archive_jobs has no kind column, and no discovery job rows are present.
        statements = []
        self.db.set_trace_callback(statements.append)
        self.addCleanup(self.db.set_trace_callback, None)
        self.assertZeroDiscovery(self.journals(26, ["enrichment_executions"]))
        self.assertEqual([statement for statement in statements if "archive_jobs" in statement], [])

    def test_retained_versions_require_their_discovery_journal_tables(self):
        for version in range(11, 26):
            discovery = ["discovery_executions"] + (["discovery_detail_executions"] if version >= 14 else [])
            with self.subTest(version=version):
                tables = ["enrichment_executions"] + discovery
                self.assertZeroDiscovery(self.journals(version, tables))
                for missing in discovery:
                    with self.assertRaisesRegex(InvalidArchive, "omit or invent"):
                        self.journals(version, [table for table in tables if table != missing])
                if version < 14:
                    with self.assertRaisesRegex(InvalidArchive, "omit or invent"):
                        self.journals(version, tables + ["discovery_detail_executions"])
