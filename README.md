# Stash integrations

Optional Python clients for the [Native Stash](https://github.com/notsafeforgit/stash).
The Stash application remains a Go server and TypeScript UI. These packages run
outside it and use its HTTP APIs; they do not write its live database directly.

| Package | Purpose |
| --- | --- |
| [stash-ingest](integrations/gallery-dl/) | gallery-dl capture, durable delivery, source workers and intake clients |
| [stash-native-archive](integrations/archive/) | Portable archive export, verification and restore |
| [stash-library](integrations/library/) | Manual library tagging, title and scan helpers |

## Deployment

A separate repository does **not** require another container. An operations image
such as stash-ops can install the released packages alongside its FileFlows adapter
and backup tools. Other users can install only the clients they need with uv.
An existing Stash container needs only its API endpoint and the relevant optional
capabilities enabled. Ordinary library use needs none of these packages.

Workers receive a scoped Stash token, logical media-root mapping and a persistent
outbox. Mount credentials and downloader configuration at runtime. Keep the outbox,
producer UUID and locks across image updates: delivery retries use the server's
idempotent receipts. FileFlows can stay in its own container and submit completed
files through its adapter. No Docker/Podman socket is needed inside Stash.

The API and data-model documentation remains in the
[Stash documentation](https://github.com/notsafeforgit/stash/tree/develop/docs).
The application repository retains real-server compatibility tests against a
pinned release of these clients. Python unit tests and package releases live here.

## Development and releases

Native n8n completion workflows can use `stash-ingest-n8n --inspect=TOKEN
--monitor` to retain typed retry/review diagnostics even when storage or API
access fails. The command never creates a missing outbox, changes the caller
token, or treats a failed check as scrape completion. Ordinary admission and
strict inspection retain their exit-code contracts. Worker stderr records
`source_request_blocked` with HTTP status and `network_security` or `http_429`,
without website response bodies, URLs or credentials.

Worker storage failures and background delivery exceptions emit one JSON stderr
record with `event: stash_worker_error`. The record includes the operation stage,
command or source-run UUID where available, exception type, extended SQLite code
and name or OS errno and name, and up to eight producer code locations. The service
journal supplies the time and process. No raw exception messages, source lines,
locals, private paths, SQL values or response bodies are logged. These diagnostics
preserve the exact storage cause without exposing private data. SQLite BUSY/LOCKED
from durable drain/dispatch commands returns pending (exit 2); other storage errors
still fail visibly. Background delivery retries contention with interruptible
backoff, and storage failures retain a retryable source attempt instead of being
misclassified as source/configuration review.

```sh
make setup test
make build
```

Use Python 3.12+ and uv. Tests use synthetic, versioned corpora and mocked website
responses; they do not scrape real accounts. `pkg/` contains the small shared
contract fixtures and frozen upgrade DDL copied at the revision recorded in
[extraction.json](extraction.json). Stash retains the canonical fixtures and tests
current APIs against these clients. No production records or credentials were
copied during extraction.

Push a `v<version>` tag after updating all package versions. CI runs the tests,
builds wheels/source archives and attaches them to a GitHub release. Downloader
pins establish a tested baseline; installations may independently update upstream
gallery-dl and yt-dlp under their own tested update policy.

### Worker compatibility review

Queue policies bind explicit execution/capture contracts, reviewed profile behavior
and helper assets. Compatible code repairs and comments retain those policies.
Diagnostics still identify every producer Python source (including entry points)
and the exact installed downloader commits.

For a producer source change, review its effect on admitted commands, coverage,
cursors and retained outputs. Update `policy-compatibility.json` with the resulting
source digest, decision and reason; `python scripts/check_policy_contract.py
--print-identity` reports the digest. A compatible change keeps the contracts. An
incompatible change increments `EXECUTION_CONTRACT` and requires an explicit queue
upgrade plan. CI checks both the source digest and the transition from the event's
base revision; the review file never participates in runtime policy hashing.

Version 0.5 introduces this contract identity. Existing source-hash policies still
need one explicit administrative upgrade during deployment. Keep producer UUIDs,
outboxes, windows, checkpoints and retry deadlines. That release retained producer schema 19;
there is no implicit policy alias or queue reset. Later compatible releases need
no policy handoff solely because their Python bytes changed.


Release v0.6.7 introduces producer schema 20 for durable subscription scheduling.
Requests are saved before API lookup and resumed by ordinary workers after Stash
maintenance. Deploy the matching archive reader and every shared-outbox client
before enabling the new workers; existing source policies and receipts are retained.

Release v0.6.10 advances the producer to schema 21 with an index from immutable
run requests to caller tickets. Admission capacity traverses only unfinished
requests and their indexed links, independent of retained receipt history. Current
queues validate their binding in a read snapshot and need no writer lock to open.
Upgrade shared-outbox command clients and archive readers before enabling the new
workers. The index migration is transactional; it changes no queued event, source
window, policy, receipt or producer identity.

Release v0.6.21 introduces execution contract v3 (producer schema stays 24).
Reddit post completion is stored as `stash:completed:reddit:<id>` in the configured
gallery-dl archive. The native adapter skips a completed post without another media
fetch. Ordinary file archive entries and file receipts still describe real files.
Focused missing-file jobs expire after 48 hours; their final claim writes the
original archive locally from retained source context, without contacting Reddit.
Retry-only state is retired after completion, and old missing-media caches expire.
Partial albums cannot become complete while an attachment is unresolved.

Deploy native schema 1000137 / attachment retry protocol 2 and all shared-outbox
clients and backup readers together, then explicitly upgrade reviewed v1/v2 queue
policies to v3. Preserve producer UUIDs, archives, original requests, pending ranges,
checkpoints, and actual receipt history. Stop writers during the coordinated
upgrade and wait for active backup captures to finish before changing captured
runtime configuration. Vanilla gallery-dl does not interpret the adapter's post
completion entries; keep using the native adapter for these managed jobs.

Release v0.6.23 introduces execution contract v4 (producer schema stays 24).
An explicit `User account deleted.` response from the claimed Redgifs account's
listing retires that source and cancels its remaining work without marking it
successfully downloaded. The native server preserves captures, media, archives,
unfinished ranges and attempt history. Generic missing/private profiles retain
their recovery checks. Deleted profiles cannot be relinked by old Reddit posts;
a different Redgifs uploader discovered later can still be linked and monitored.

Reddit's explicit suspension and X's structured unavailable-user result retain
separate `account_suspended`, `account_deactivated`, `account_unavailable` and
`account_not_found` codes where the provider supplies that distinction. These
remain eligible for monthly rechecks. A missing or renamed handle, generic 404,
or suspension does not establish permanent deletion.

Deploy native schema 1000138 advertising `source_run_account_status_protocol: 1`
and `source_run_redgifs_deletion_protocol: 1`,
then explicitly upgrade reviewed v3 policies to v4 while writers are stopped and
no backup is capturing their configuration. Keep the same producer identities,
outboxes, archive files and destinations.

Release v0.6.25 keeps execution contract v4 and introduces producer schema 25.
`stash-outbox-retention --compact-acknowledgements --apply` verifies old accepted
receipts against the native library and replaces their local JSON copies with
37-byte versioned SHA-256 proofs after the retention interval (one day by default).
This also covers unfinished crawls. Event identities, payload hashes, parent
references, pending payloads, and gallery-dl archives remain intact. Replays still
deduplicate locally; an old full receipt is fetched from Stash only when needed,
checked against its proof, and cached in bounded memory. No receipt is copied back
into the outbox. Completed runs remain eligible for verified whole-run retirement.

Deploy producer clients and archive readers together with shared writers stopped
and no active backup capture. Older workers reject schema 25. The migration itself
preserves every receipt; only the explicit verified retention pass compacts them.
Backup verification rejects compact proofs with missing or changed native receipts.
Compaction frees SQLite pages for reuse; reclaiming the physical file size requires
a separate coordinated SQLite compaction while writers are stopped.

Release v0.6.26 introduces execution contract v5. An explicitly reviewed Redgifs
profile can set `extractor.redgifs.user.native-history-limit` to `available`.
After an explicit page-limit response, that traversal tries separate image and
video feeds and the oldest-first end of feeds that exceed the ceiling. Saved
page checkpoints retain the remaining passes across interruptions. Finishing
means all history available through those feeds, with any inaccessible middle
range identified in `history_coverage` in the worker result. HTTP 429, temporary
errors, missing accounts, and malformed responses still require their normal
retry or retirement handling. The setting accepts unfiltered user profiles and
cannot be used for recovery replay.

Redgifs profiles with `scan_mode: routine` stop after a wholly downloaded post;
gallery members must all be accounted for before that boundary can end a scan.
Use a separate routine profile for future checks. Keep unfinished initial crawls
on their initial profile so an archived post cannot hide remaining history.
Deploy v5 through explicit policy upgrades while workers are quiesced and no
backup captures configuration; retain producer identities, source windows,
archives and saved page progress. Do not reinterpret retained v4 admissions
without that review. Producer schema 25 is unchanged.
