"""One bounded scheduling pass over local download and metadata profiles."""

from pathlib import Path
import re

from .backfill_calls import BackfillCalls, advance_once
from .client import Unavailable, drain_once
from .collection_dispatch import CollectionDispatcher
from .configuration import _json_file, _path
from .profile_dispatch import ProfileDispatcher
from .encoding import InvalidData, identifier
from .enrichment_configuration import EnrichmentConfiguration
from .enrichment_journal import EnrichmentJournal
from .enrichment_worker import execute as deliver_enrichment
from .outbox import Capacity, Conflict
from .dispatch_retention import DispatchCapacity
from .run_queue import RunQueue, submit_once
from .source_calls import SourceCalls, resolve_once
from .scheduled_calls import ScheduledCalls, advance_schedule

SCHEMA = "stash-gallery-dispatch-v1"


class Profiles:
    def __init__(self, filename):
        filename = Path(filename).resolve(strict=True)
        value = _json_file(filename)
        if (not isinstance(value, dict) or set(value) != {"schema", "uuid", "profiles"} or value["schema"] != SCHEMA
                or not isinstance(value["profiles"], list) or not 1 <= len(value["profiles"]) <= 32):
            raise InvalidData("Invalid worker dispatch configuration")
        self.uuid, self.entries = identifier(value["uuid"]), []
        seen = set()
        for entry in value["profiles"]:
            if (not isinstance(entry, dict) or not {"id", "operation", "profile"} <= set(entry)
                    or set(entry) - {"id", "operation", "profile", "priority", "background"}
                    or type(entry.get('priority', 0)) is not int or not 0 <= entry.get('priority', 0) <= 2
                    or type(entry.get('background', False)) is not bool
                    or (entry.get('background', False) and entry.get('operation') != 'post.enrich')
                    or not isinstance(entry["id"], str) or not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", entry["id"])
                    or entry["id"] in seen or entry["operation"] not in ("download", "post.enrich")):
                raise InvalidData("Invalid or repeated worker profile entry")
            seen.add(entry["id"])
            self.entries.append({**entry, "profile": _path(entry["profile"], filename.parent)})


class WorkerDispatcher:
    def __init__(self, box, transport, profiles):
        if (box.endpoint, box.producer) != (transport.endpoint, transport.producer):
            raise Conflict("Worker dispatcher and outbox identify different producers")
        self.box, self.transport, self.profiles = box, transport, profiles
        with box.transaction():
            if self.state() is None:
                if box.db.execute("SELECT count(*) FROM worker_dispatch").fetchone()[0] >= 10000:
                    raise Capacity("Worker dispatch capacity exhausted")
                box.db.execute("INSERT INTO worker_dispatch(uuid) VALUES(?)", (profiles.uuid,))

    def state(self):
        row = self.box.db.execute("SELECT * FROM worker_dispatch WHERE uuid=?", (self.profiles.uuid,)).fetchone()
        return dict(row) if row else None

    def save(self, state, **changes):
        value = {**state, **changes}
        with self.box.transaction():
            changed = self.box.db.execute("""UPDATE worker_dispatch SET after_entry=?,enrichment_delivery_after=?,revision=revision+1
                WHERE uuid=? AND revision=?""", (value["after_entry"], value["enrichment_delivery_after"],
                    self.profiles.uuid, state["revision"])).rowcount
        if changed:
            state.update(changes, revision=state["revision"] + 1)
        return bool(changed)

    def recover_delivery(self, state):
        # Delivery does not require a current website profile, active collection
        # definition or new metadata grant. The server authenticates the saved
        # job's historical scope when receiving its exact pending operation.
        cursor = "enrichment_delivery_after"
        if state[cursor]:
            identifier(state[cursor])
        # Finished native jobs can leave an unsent publication, failure or checkpoint.
        # The saved execution must observe that outcome even without a current lease.
        query = """SELECT job_uuid FROM enrichment_executions WHERE phase='active'
            AND json_type(state,'$.pending')='object' AND job_uuid>? ORDER BY job_uuid LIMIT 1"""
        row = self.box.db.execute(query, (state[cursor],)).fetchone()
        if row is None and state[cursor]:
            row = self.box.db.execute(query, ("",)).fetchone()
        if not self.save(state, **{cursor: row["job_uuid"] if row else ""}):
            return {"state": "contended"}
        return deliver_enrichment(self.box, self.transport, None, row["job_uuid"]) if row else {"state": "idle"}

    def once(self):
        state = self.state()
        deliveries = {}
        delivery = self.recover_delivery(state)
        deliveries["enrichment_delivery"] = delivery
        if delivery["state"] == "contended":
            return {"state": "contended", **deliveries}
        entries = self.profiles.entries
        names = [entry["id"] for entry in entries]
        start = names.index(state["after_entry"]) + 1 if state["after_entry"] in names else 0
        # Express metadata interest before selecting another download. Stash
        # grants its bounded turn or returns waiting; a busy download lane must
        # not prevent the metadata claim that registers this scheduling interest.
        # Rotate peers fairly and retain the ordinary download priority order.
        ordered = sorted(entries[start:] + entries[:start],
                         key=lambda entry: (entry['operation'] == 'download', entry.get('priority', 0)))
        skipped = []
        for entry in ordered:
            if not self.save(state, after_entry=entry["id"]):
                return {"state": "contended", **deliveries, "profiles": skipped}
            try:
                if entry["operation"] == "download":
                    result = ProfileDispatcher(self.box, self.transport, entry["profile"]).once()
                else:
                    # A background profile belongs in a metadata-only worker: it
                    # uses spare request capacity beside downloads other
                    # workers run, and never waits for a metadata turn.
                    result = CollectionDispatcher(self.box, self.transport, EnrichmentConfiguration(entry["profile"]),
                                                  background=entry.get("background", False)).once()
            except DispatchCapacity as error:
                result = error.result()
            except Capacity:
                result = {"state": "unavailable", "error_code": "worker_capacity_exhausted"}
            except (OSError, InvalidData, Unavailable):
                # An unavailable local profile cannot hold every other source.
                # Do not return paths, access values or raw exception strings.
                result = {"state": "unavailable", "error_code": "worker_profile_unavailable"}
            result = {**result, "profile_id": entry["id"]}
            if result["state"] not in ("waiting", "idle", "backoff", "unavailable", "profile_required", "contended"):
                return {**result, "profiles": skipped, **deliveries}
            skipped.append(result)
        idle = all(r["state"] == "idle" for r in [*skipped, *deliveries.values()])
        return {"state": "idle" if idle else "waiting", "profiles": skipped, **deliveries}


def dispatch_all(box, client, profiles):
    worker = WorkerDispatcher(box, client, profiles)
    requests, calls, backfills = RunQueue(box), SourceCalls(box), BackfillCalls(box)
    delivery = drain_once(box, client)
    schedules = ScheduledCalls(box)
    scheduling = advance_schedule(schedules, client)
    backfill = advance_once(backfills, client)
    resolution = resolve_once(calls, client)
    submission = submit_once(requests, client)
    result = worker.once()
    return {**result, "scheduling": scheduling, "scheduled_calls": schedules.summary(), "delivery": delivery, "backfill": backfill, "resolution": resolution, "submission": submission,
            "outbox": box.status(), "source_requests": requests.status(), "source_calls": calls.summary(),
            "backfill_calls": backfills.summary(), "enrichment": EnrichmentJournal(box).summary(),
            "intake_completion": "inspect_native_receipts"}


def dispatch_available(box, client, profiles, *, max_passes=1):
    """Continue after completed attempts; return promptly when no work can run.

    Each pass still discovers and claims work through the server's shared request
    budgets, priorities, leases and retry deadlines. The bound lets a service
    release its process periodically without a timer delay after every account.
    """
    if type(max_passes) is not int or not 1 <= max_passes <= 100:
        raise InvalidData("Worker dispatch requires one to one hundred passes")
    attempts = []
    for _ in range(max_passes):
        result = dispatch_all(box, client, profiles)
        attempts.append({key: result[key] for key in ('state', 'profile_id', 'run_uuid', 'job_uuid', 'error_code')
                         if key in result})
        if result['state'] not in ('source_succeeded', 'completed', 'page_delivered', 'yielded', 'retry', 'deferred'):
            break
        if result.get('delivery_error') or result.get('error_code') == 'worker_interrupted':
            break
    return {**result, 'dispatch_passes': len(attempts), 'dispatch_attempts': attempts}
