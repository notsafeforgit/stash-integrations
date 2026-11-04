"""Reconcile local delivery state against immutable native terminal outcomes.

Only native publications prove success. A failed attempt remains failed even
when another job has since completed its target. Undelivered source bodies and
reservations always survive; accepted checkpoints remain owned by the server.
"""

from collections import Counter

from .client import Unavailable
from .encoding import InvalidData, decode
from .enrichment_client import EnrichmentClient


def receipt_matches(library, receipt, job):
    try:
        EnrichmentClient._receipt(receipt, job)
    except Unavailable:
        return False
    row = library.execute('''SELECT job_uuid,revision,digest AS sha256,fence,
        record_count,pending_count,unresolved_count FROM enrichment_checkpoint_receipts
        WHERE job_uuid=? AND revision=?''', (job, receipt['revision'])).fetchone()
    return row is not None and all(receipt.get(k) == row[k] for k in row.keys())


def publication(library, native, work):
    try:
        raw = native['result']
        result = decode(raw.encode() if isinstance(raw, str) else raw, 16384)
        EnrichmentClient._completion(result, native['uuid'], work=work)
    except (InvalidData, Unavailable, TypeError):
        return None
    row = library.execute('''SELECT p.job_uuid,p.checkpoint_revision,p.fence,p.completion_uuid,
        r.digest AS checkpoint_sha256,r.record_count,r.unresolved_count,e.capture_count
        FROM enrichment_publications p
        JOIN enrichment_checkpoint_receipts r ON r.job_uuid=p.job_uuid AND r.revision=p.checkpoint_revision
        JOIN enrichment_completions e ON e.uuid=p.completion_uuid
        WHERE p.job_uuid=? AND e.target_uuid=? AND e.expected_revision=?''',
        (native['uuid'], work['target_uuid'], work['target_revision'])).fetchone()
    if (row is None or result['fence'] != native['fence']
            or any(result.get(k) != row[k] for k in row.keys())):
        return None
    return result


def terminal_plan(journal, library):
    """Caller owns the execution lock and native snapshot/outbox transaction."""
    journal._require_lock()
    plans, protected, outcomes = [], Counter(), Counter()
    rows = journal.db.execute("SELECT job_uuid FROM enrichment_executions WHERE phase IN ('active','review')").fetchall()
    for row in rows:
        value = journal.find(row['job_uuid'])
        native = library.execute("SELECT uuid,state,arguments,fence,result,error_code FROM archive_jobs WHERE uuid=? AND kind='post.enrich'",
                                 (value.job_uuid,)).fetchone()
        if native is None:
            protected['native_job_missing'] += 1
            continue
        if native['state'] not in ('succeeded', 'failed', 'cancelled'):
            protected['native_job_unfinished'] += 1
            continue
        raw = native['arguments']
        arguments = decode(raw.encode() if isinstance(raw, str) else raw, 65536)
        if value.definition.get('job_uuid') != value.job_uuid or arguments != value.definition['arguments']:
            protected['native_job_mismatch'] += 1
            continue
        if value.body is not None or value.reserved_bytes:
            protected['undelivered_payload_or_reservation'] += 1
            continue
        pending = value.state.get('pending')
        if pending is not None and (not isinstance(pending, dict) or pending.get('kind') not in ('publish', 'failure')):
            protected['unknown_delivery_intent'] += 1
            continue
        receipts = [value.state.get('checkpoint')]
        if pending and pending['kind'] == 'publish':
            receipts.append(pending.get('receipt'))
            if receipts[-1] is None:
                protected['checkpoint_not_confirmed'] += 1
                continue
        if any(not receipt_matches(library, receipt, value.job_uuid) for receipt in receipts if receipt is not None):
            protected['checkpoint_not_confirmed'] += 1
            continue
        state = {**value.state, 'pending': None, 'native_outcome': native['state'],
                 'native_error_code': native['error_code'] or ''}
        phase = 'failed'
        if native['state'] == 'succeeded':
            result = publication(library, native, arguments)
            if result is None:
                protected['publication_not_confirmed'] += 1
                continue
            state.update(publication=result, error_code='')
            # A prior reuse hint must not replace this job's native publication.
            state.pop('reuse_completion', None)
            phase = 'completed'
        else:
            state['error_code'] = 'native_job_' + native['state']
        plans.append((value, phase, state))
        outcomes[native['state']] += 1
    return plans, {'examined': len(rows), 'eligible': len(plans), 'native_outcomes': dict(outcomes),
                   'protected': dict(protected)}
