"""Reconcile terminal local executions and retire obsolete enrichment cursors.

The caller holds deployment/backup exclusion while inventorying every profile
directory sharing the outbox. Native jobs and original job definitions remain
immutable. Pending source payloads are never discarded by maintenance.
"""

import argparse
from collections import Counter
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import time

from .configuration import _json_file, SCHEMA as WORKER_SCHEMA, LAYERED_SCHEMA, STORAGE_SCHEMA, STORAGE_LAYERED_SCHEMA
from .encoding import InvalidData, decode, identifier
from .endpoint import origin
from .enrichment_configuration import EnrichmentConfiguration, SCHEMA
from .enrichment_journal import EnrichmentJournal
from .enrichment_reconcile import terminal_plan
from .events import sha256
from .outbox import Outbox

TABLES = ('enrichment_dispatch', 'enrichment_collection_dispatch')
MAX_POLICIES = 1024
MAX_PROFILES = 4096
DELIVERY_POLICY = '0' * 64


def inventory(directories):
    """Include direct profiles and dispatcher references, including disabled lanes."""
    if not directories or len(directories) > 32:
        raise InvalidData('Inventory requires all profile directories for this outbox')
    roots = [Path(p).resolve(strict=True) for p in directories]
    if len(set(roots)) != len(roots) or any(not p.is_dir() for p in roots):
        raise InvalidData('Profile inventory directories must be distinct directories')
    files = set()
    for root in roots:
        found = set(root.rglob('*.json'))
        if not found:
            raise InvalidData('An empty profile directory cannot prove policy retirement')
        files.update(p.resolve(strict=True) for p in found)
    if len(files) > MAX_PROFILES:
        raise InvalidData('Profile inventory exceeds its bound')
    documents = {path: _json_file(path) for path in files}
    # A dispatcher may reference a profile outside its own directory. Missing
    # or malformed references must not be mistaken for a retired policy.
    from .worker_dispatch import Profiles, SCHEMA as DISPATCH_SCHEMA
    from .discovery_configuration import SCHEMA as DISCOVERY_SCHEMA
    from .discovery_detail_configuration import SCHEMA as DETAIL_SCHEMA
    known = {SCHEMA, DISPATCH_SCHEMA, DISCOVERY_SCHEMA, DETAIL_SCHEMA,
             WORKER_SCHEMA, LAYERED_SCHEMA, STORAGE_SCHEMA, STORAGE_LAYERED_SCHEMA}
    for path, value in list(documents.items()):
        # Plain gallery-dl layers may live beside their wrapper profiles. They
        # do not independently configure metadata workers, but remain inventoried.
        if (isinstance(value, dict) and 'schema' not in value and isinstance(value.get('extractor'), dict)
                and not (set(value) & {'source_category', 'gallery', 'bindings'})):
            continue
        if not isinstance(value, dict) or value.get('schema') not in known:
            raise InvalidData('Invalid profile inventory document')
        if value['schema'] == DISPATCH_SCHEMA:
            for entry in Profiles(path).entries:
                if entry['operation'] == 'post.enrich':
                    target = Path(entry['profile']).resolve(strict=True)
                    document = _json_file(target)
                    if not isinstance(document, dict) or document.get('schema') != SCHEMA:
                        raise InvalidData('Enrichment dispatcher references a different profile type')
                    documents[target] = document
    if len(documents) > MAX_PROFILES:
        raise InvalidData('Profile inventory exceeds its bound')
    policies = set()
    sources = []
    for path, value in sorted(documents.items()):
        before = path.read_bytes()
        if value.get('schema') == SCHEMA:
            policies.add(EnrichmentConfiguration.inventory_policy(path))
        if path.read_bytes() != before:
            raise InvalidData('Profile changed during policy inventory')
        sources.append((str(path), hashlib.sha256(before).hexdigest()))
    if not policies:
        raise InvalidData('No enrichment profiles found; explicit retirement is required for an empty deployment')
    return {'policies': sorted(policies), 'sources': sources, 'directories': list(map(str, roots))}


def native_context(box, library):
    if (library.execute('SELECT lineage FROM native_schema WHERE singleton=1').fetchone()[0]
            != 'org.notsafeforgit.stash.native-archive'
            or not library.execute('SELECT 1 FROM ingest_producers WHERE uuid=?', (box.producer,)).fetchone()):
        raise InvalidData('Native library does not identify this producer')
    approvals = {}
    for row in library.execute("SELECT original_policy_sha256,policy_sha256,request_uuid FROM metadata_worker_policy_upgrades WHERE kind='post.enrich' ORDER BY id"):
        if not sha256(row['original_policy_sha256']) or not sha256(row['policy_sha256']):
            raise InvalidData('Invalid native metadata policy approval')
        identifier(row['request_uuid'])
        approvals[row['original_policy_sha256']] = row['policy_sha256']
    return approvals


def original_policy(row):
    arguments = decode(row['arguments'].encode() if isinstance(row['arguments'], str) else row['arguments'], 65536)
    if not isinstance(arguments, dict) or not sha256(arguments.get('policy_sha256')):
        raise InvalidData('Invalid native enrichment policy')
    return arguments, arguments['policy_sha256']


def reconcile(box, library, directories, *, apply=False):
    snapshot = inventory(directories)
    configured = set(snapshot['policies'])
    approvals = native_context(box, library)
    journal = EnrichmentJournal(box)
    with journal.execution() as acquired:
        if not acquired:
            return {'state': 'waiting', 'reason': 'metadata_execution_active'}
        # Local terminal acknowledgements and cursor removals commit together.
        # Native jobs and publication checkpoints are read-only evidence.
        with box.transaction():
            plans, executions = terminal_plan(journal, library)
            reconciled = {value.job_uuid for value, _, _ in plans}
            policies = {r[0] for table in TABLES for r in box.db.execute('SELECT DISTINCT policy_sha256 FROM ' + table)}
            if len(policies) > MAX_POLICIES or any(not sha256(p) for p in policies):
                raise InvalidData('Invalid or excessive dispatcher policy inventory')
            candidates = policies - configured - {DELIVERY_POLICY}
            protected = set()
            reasons = Counter()

            def protect(policy, reason):
                if policy in candidates:
                    protected.add(policy)
                    reasons[reason] += 1

            # Native queued work may not have a local journal yet. Only a direct
            # approved replacement that is configured here releases its old cursor.
            for row in library.execute("SELECT uuid,state,arguments,fence FROM archive_jobs WHERE kind='post.enrich' AND state IN ('queued','running')"):
                _, original = original_policy(row)
                effective = approvals.get(original, original)
                if row['state'] == 'running':
                    attempt = library.execute('SELECT policy_sha256 FROM metadata_worker_attempt_policies WHERE job_uuid=? AND fence=?', (row['uuid'], row['fence'])).fetchone()
                    protect(attempt[0] if attempt else original, 'running_attempt')
                if effective not in configured:
                    protect(original, 'native_work_without_configured_replacement')
                    protect(effective, 'native_execution_policy_not_configured')

            for row in box.db.execute("SELECT job_uuid FROM enrichment_executions WHERE phase IN ('active','review')"):
                if row['job_uuid'] in reconciled:
                    continue
                value = journal.find(row['job_uuid'])
                original = value.definition['arguments']['policy_sha256']
                if original not in candidates:
                    continue
                if value.phase == 'review':
                    protect(original, 'review')
                    continue
                if value.state.get('pending') is not None or value.body is not None or value.reserved_bytes:
                    protect(original, 'pending_delivery')
                    continue
                native = library.execute("SELECT uuid,state,arguments,fence FROM archive_jobs WHERE uuid=? AND kind='post.enrich'", (value.job_uuid,)).fetchone()
                if native is None:
                    protect(original, 'native_job_missing')
                    continue
                arguments, _ = original_policy(native)
                if arguments != value.definition['arguments']:
                    protect(original, 'native_job_mismatch')
                elif native['state'] not in ('queued', 'succeeded', 'failed', 'cancelled') or approvals.get(original, original) not in configured:
                    protect(original, 'unfinished_original_policy')

            retired = sorted(candidates - protected)
            # Re-read directories, references, assets and profile bytes just
            # before deletion. Any incomplete/changing inventory aborts the pass.
            if inventory(directories) != snapshot:
                raise InvalidData('Profile inventory changed before retirement')
            if apply:
                for value, phase, state in plans:
                    journal.change_in_transaction(value, phase=phase, state=state, body=None, reserved=0)
            executions['reconciled'] = len(plans) if apply else 0
            counts = {}
            for table in TABLES:
                total = box.db.execute('SELECT count(*) FROM ' + table).fetchone()[0]
                eligible = 0
                if retired:
                    slots = ','.join('?' for _ in retired)
                    eligible = box.db.execute(f'SELECT count(*) FROM {table} WHERE policy_sha256 IN ({slots})', retired).fetchone()[0]
                    if apply:
                        box.db.execute(f'DELETE FROM {table} WHERE policy_sha256 IN ({slots})', retired)
                counts[table] = {'before': total, 'eligible': eligible, 'deleted': eligible if apply else 0,
                                 'remaining': total - eligible if apply else total}
            return {'state': 'applied' if apply else 'preview', 'configured_policies': len(configured),
                    'retired_policies': retired, 'protected_policies': sorted(protected),
                    'protection_reasons': dict(reasons), 'cursors': counts, 'executions': executions,
                    'inventory_sha256': hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest()}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--outbox', required=True, type=Path)
    parser.add_argument('--library', required=True, type=Path)
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--profile-directory', action='append', required=True, type=Path)
    parser.add_argument('--report', type=Path)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args(argv)
    with closing(sqlite3.connect(args.outbox.resolve(strict=True).as_uri() + '?mode=ro', uri=True)) as db:
        db.execute('PRAGMA query_only=ON')
        endpoint, producer = db.execute('SELECT endpoint,producer FROM binding WHERE id=1').fetchone()
    if origin(endpoint) != origin(args.endpoint):
        raise InvalidData('Outbox endpoint differs from selected deployment')
    with closing(sqlite3.connect(args.library.resolve(strict=True).as_uri() + '?mode=ro', uri=True)) as library, \
            closing(Outbox(args.outbox, endpoint, producer, create=False)) as box:
        library.row_factory = sqlite3.Row
        library.execute('PRAGMA query_only=ON')
        library.execute('BEGIN')
        result = reconcile(box, library, args.profile_directory, apply=args.apply)
    if args.report:
        # One current report per outbox; do not accumulate another receipt history.
        from .encoding import encode
        import os
        import tempfile
        body = encode({'checked_at': time.time(), **result}) + b'\n'
        fd, temporary = tempfile.mkstemp(prefix='.policy-retention-', dir=args.report.parent)
        try:
            with os.fdopen(fd, 'wb') as target:
                target.write(body)
                target.flush()
                os.fsync(target.fileno())
            os.replace(temporary, args.report)
        finally:
            Path(temporary).unlink(missing_ok=True)
    print(json.dumps(result, sort_keys=True))
    return 2 if result['state'] == 'waiting' else 0


if __name__ == '__main__':
    raise SystemExit(main())
