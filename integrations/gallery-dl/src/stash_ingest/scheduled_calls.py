"""Durable subscription scheduling, before the first Stash API request."""

import uuid

from .client import Unavailable
from .encoding import InvalidData, decode, digest, encode, identifier
from .events import sha256
from .outbox import Capacity, Conflict, LeaseLost
from .source_calls import SourceCalls, definition
from .subscriptions import subscribed_targets

SERVICES = ('reddit', 'redgifs', 'twitter', 'instagram', 'coomer', 'kemono', 'bluesky', 'tiktok')


def migrate(db):
    db.execute("""CREATE TABLE scheduled_calls(
        seq INTEGER PRIMARY KEY, uuid TEXT NOT NULL UNIQUE, request_sha256 TEXT NOT NULL,
        definition BLOB NOT NULL, definition_sha256 TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('pending','resolving','recorded','empty','review')),
        owner TEXT, fence INTEGER NOT NULL DEFAULT 0, lease_until REAL,
        attempts INTEGER NOT NULL DEFAULT 0, available_at REAL NOT NULL, created_at REAL NOT NULL,
        error_code TEXT, http_status INTEGER,
        CHECK((state='resolving' AND owner IS NOT NULL AND lease_until IS NOT NULL)
            OR (state!='resolving' AND owner IS NULL AND lease_until IS NULL))
    )""")
    db.execute('CREATE INDEX ready_scheduled_calls ON scheduled_calls(state,available_at,seq)')
    db.execute("CREATE INDEX expired_scheduled_calls ON scheduled_calls(lease_until) WHERE state='resolving'")
    db.execute("""CREATE TRIGGER scheduled_call_immutable BEFORE UPDATE OF uuid,request_sha256,definition,definition_sha256,created_at
        ON scheduled_calls BEGIN SELECT RAISE(ABORT,'scheduled requests are immutable'); END""")


def specification(value):
    if (not isinstance(value, dict) or set(value) != {'service', 'mode', 'source'}
            or value['service'] not in SERVICES or value['mode'] not in ('new', 'top')
            or value['service'] != 'reddit' and value['mode'] != 'new'):
        raise InvalidData('Invalid subscription scheduling definition')
    source = definition(value['source'])
    if source['operation'] != 'download' or 'subscriptions' in source:
        raise InvalidData('Scheduling requires an unexpanded download definition')
    return {**value, 'source': source}


class ScheduledCalls:
    def __init__(self, box, *, max_calls=10000):
        if type(max_calls) is not int or not 1 <= max_calls <= 1000000:
            raise InvalidData('Invalid scheduling capacity')
        self.box, self.db, self.max_calls = box, box.db, max_calls

    def find(self, call):
        identifier(call)
        return self.db.execute('SELECT * FROM scheduled_calls WHERE uuid=?', (call,)).fetchone()

    @staticmethod
    def definition(row):
        if digest(row['definition']) != row['definition_sha256']:
            raise InvalidData('Scheduled definition differs from its recorded digest')
        return specification(decode(row['definition'], 16384))

    def record(self, call, request_sha256, prepare):
        identifier(call)
        if not sha256(request_sha256):
            raise InvalidData('Scheduling requires a stable request digest')
        previous = self.find(call)
        if previous is not None:
            if previous['request_sha256'] != request_sha256:
                raise Conflict('Caller UUID already identifies different options')
            self.definition(previous)
            return self.summary(call)
        value = specification(prepare())
        body = encode(value, 16384)
        with self.box.transaction():
            previous = self.find(call)
            if previous is not None:
                if previous['request_sha256'] != request_sha256:
                    raise Conflict('Caller UUID already identifies different options')
            else:
                if self.db.execute('SELECT count(*) FROM scheduled_calls').fetchone()[0] >= self.max_calls:
                    raise Capacity('Scheduling capacity exhausted; existing requests are retained')
                self.db.execute("""INSERT INTO scheduled_calls(uuid,request_sha256,definition,definition_sha256,
                    state,available_at,created_at) VALUES(?,?,?,?,'pending',?,?)""",
                    (call, request_sha256, body, digest(body), self.box.clock(), self.box.clock()))
        return self.summary(call)

    def summary(self, call=None):
        if call is None:
            counts = {state: 0 for state in ('pending', 'resolving', 'recorded', 'empty', 'review')}
            counts.update(dict(self.db.execute('SELECT state,count(*) FROM scheduled_calls GROUP BY state')))
            return {'counts': counts}
        row = self.find(call)
        if row is None:
            raise InvalidData('Scheduled request was not found')
        return {key: row[key] for key in ('uuid', 'state', 'attempts', 'available_at', 'error_code', 'http_status')}

    def claim(self, call=None):
        if call is not None:
            identifier(call)
        with self.box.transaction():
            now = self.box.clock()
            self.db.execute("""UPDATE scheduled_calls SET state='pending',owner=NULL,lease_until=NULL,
                error_code='schedule_interrupted' WHERE state='resolving' AND lease_until<=?""", (now,))
            row = self.db.execute("""SELECT * FROM scheduled_calls WHERE state='pending' AND available_at<=?
                AND (? IS NULL OR uuid=?) ORDER BY available_at,seq LIMIT 1""", (now, call, call)).fetchone()
            if row is None:
                return None
            delivery = dict(row)
            delivery.update(owner=str(uuid.uuid4()), fence=row['fence'] + 1, attempts=row['attempts'] + 1)
            self.db.execute("""UPDATE scheduled_calls SET state='resolving',owner=?,fence=?,attempts=?,lease_until=?
                WHERE uuid=?""", (delivery['owner'], delivery['fence'], delivery['attempts'], now + 900, row['uuid']))
            return delivery

    def owned(self, delivery):
        row = self.find(delivery['uuid'])
        if (row is None or row['state'] != 'resolving' or row['owner'] != delivery['owner']
                or row['fence'] != delivery['fence'] or row['lease_until'] <= self.box.clock()):
            raise LeaseLost('Scheduled request ownership expired; its intent remains retained')

    def finish(self, delivery, targets, bindings):
        spec = self.definition(delivery)
        with self.box.transaction():
            self.owned(delivery)
            if targets:
                SourceCalls(self.box).record_in_transaction(delivery['uuid'], delivery['request_sha256'],
                    {**spec['source'], 'targets': targets, 'subscriptions': bindings})
            self.db.execute("""UPDATE scheduled_calls SET state=?,owner=NULL,lease_until=NULL,error_code=NULL,http_status=NULL
                WHERE uuid=?""", ('recorded' if targets else 'empty', delivery['uuid']))
        return self.summary(delivery['uuid'])

    def fail(self, delivery, code, *, status=None, retry=False, retry_after=0):
        with self.box.transaction():
            self.owned(delivery)
            # Continue indefinitely for a recoverable outage, at most once per
            # five minutes once the backoff grows. Honor a longer Retry-After.
            delay = max(min(300, 15 * 2 ** min(delivery['attempts'] - 1, 5)), retry_after)
            self.db.execute("""UPDATE scheduled_calls SET state=?,owner=NULL,lease_until=NULL,
                available_at=?,error_code=?,http_status=? WHERE uuid=?""",
                ('pending' if retry else 'review', self.box.clock() + delay, code, status, delivery['uuid']))
        return self.summary(delivery['uuid'])

    def retry(self, call):
        identifier(call)
        with self.box.transaction():
            self.db.execute("""UPDATE scheduled_calls SET state='pending',available_at=?,error_code=NULL,http_status=NULL
                WHERE uuid=? AND state='review'""", (self.box.clock(), call))
        return self.summary(call)


def advance_schedule(calls, client, *, call=None):
    if (calls.box.endpoint, calls.box.producer) != (client.endpoint, client.producer):
        raise Conflict('Scheduler and client identify different Stash producers')
    delivery = calls.claim(call)
    if delivery is None:
        return calls.summary(call) if call else {'state': 'idle'}
    try:
        spec = calls.definition(delivery)
        targets, bindings = subscribed_targets(client, spec['source']['root_uuid'], spec['service'], mode=spec['mode'])
        return calls.finish(delivery, targets, bindings)
    except LeaseLost:
        return {'state': 'lease_lost', 'uuid': delivery['uuid']}
    except (Unavailable, InvalidData, Capacity) as error:
        code, status, retry, delay = 'invalid_scheduled_request', None, False, 0
        if isinstance(error, Unavailable):
            code, status, delay = error.code, error.status, error.retry_after
            retry = code == 'network_unavailable' or status in (408, 425, 429, 500, 502, 503, 504)
        elif isinstance(error, Capacity):
            code, retry = 'caller_submission_capacity', True
        try:
            return calls.fail(delivery, code, status=status, retry=retry, retry_after=delay)
        except LeaseLost:
            return {'state': 'lease_lost', 'uuid': delivery['uuid']}
