# Native Stash producer

For a fresh installation, start with the [gallery-dl quickstart](QUICKSTART.md).
`stash-gallery setup` generates the local configuration and scoped Stash
credentials; `download`, `resume`, `check`, `status`, `drain` and `worker` use the
existing producer contracts. No n8n, S3 or installation-specific wrapper is
required. The reference below covers advanced workers, protocol details and
historical migration tools.

This integration is optional. Enable **Settings → Services → Source tracking**
and restart Stash before registering producers or running workers. It defaults
to off; host/n8n schedules and website access still require their own setup.
See [optional integrations](https://github.com/notsafeforgit/stash/blob/develop/docs/optional-integrations.md). A disabled
server returns `source_tracking_disabled` without acknowledging queued events.

This package implements durable delivery and a gallery-dl download adapter.
Host and n8n workers scrape directly into the native archive through durable
source runs and delivery journals. See [scrape requests and checkpoints](https://github.com/notsafeforgit/stash/blob/develop/docs/native-source-runs.md)
for shared request budgets, routine/initial/recovery modes, pagination and the
metadata retained without separate per-post enrichment.

Python 3.12 or newer is required. Runtime delivery uses only the standard
library. Create an environment with `uv venv .venv`, then install the local
package with `uv pip install --python .venv/bin/python ./integrations/gallery-dl`
in the runner's environment when integrating it; do not replace a live wrapper
with this CLI. The CLI can queue source requests, drain captured events and
execute one claimed download attempt using an explicitly reviewed local profile.
The optional `[gallery]` extra pins the gallery-dl source revision and yt-dlp
version used to verify downloader integration. Run `make setup` from the stash-integrations root to install
that test runtime into `.venv`, independently of live workers.
The setup target also checks installed modules against current sources without
workspace imports. Wheel builds remove obsolete modules from reused `build/lib`
output; they must not alter a worker's policy fingerprint. Use Python `-I` for
package installation so a development `PYTHONPATH` cannot redirect pip's uninstall
lookup to the source tree.

Concurrent workers require an SQLite library with the
[WAL-reset corruption fix](https://sqlite.org/wal.html#walreset).
Check the producer interpreter itself with
`python -c 'import sqlite3; print(sqlite3.sqlite_version)'`; a newer SQLite CLI
does not upgrade Python's library. Production host and n8n producers use SQLite
3.53.4. The host's isolated environment loads its pinned library from
`~/.local/share/stash-ingest/sqlite-3.53.4/` through
`stash_sqlite_runtime.pth`; retain that library, its manifest and the bootstrap
file when restoring the producer environment.

## Authentication boundary

The package also provides `stash-intake-folders` for
[scheduled local-file discovery](https://github.com/notsafeforgit/stash/blob/develop/docs/native-manual-intake.md#scheduled-folder-discovery).
It uses application authentication and native collection policies, retains exact
admissions and directory cursors in one SQLite journal, and caps pending work
across invocations. Service/timer templates are packaged but inactive; production
folder coverage, policy activation and legacy scan retirement remain cutover work.
A terminal failure is kept against the file version it observed, so an unchanged
file is not retried. After a server-side fix, `stash-intake-folders --config ...
--requeue-failed ERROR_CODE` (for example `file_changed`) marks those failures for
resubmission; later passes submit them within the usual pending limit.
While Stash is restarting or unreachable (no connection, a dropped one, or a
502/503/504), `stash-intake-folders` and `stash-dedupe-host` print
`{"skipped": true, "reason": "stash_unavailable"}` (or `network_unavailable`) and
exit 0. Their journals keep pending requests for the next run. Other HTTP errors
still fail with their rejection codes.

`STASH_INGEST_TOKEN` is a token for Stash's ingestion API. The queue binds to a
Stash HTTP origin and stable producer UUID, independently of that token. Rotating
the token preserves queued event identity and receipts. The client reads the
environment reference for each request and sends the token only in the
Authorization header; it does not store the token, follow redirects, inherit
website proxies or load cookies. Website credentials remain with gallery-dl.

Stash can authorize named collection/root pairs or all registered collections at
an explicitly selected logical root. The latter also covers later additions,
without granting source registration or editing. Existing tokens do not acquire
root grants when the database upgrades; unbound metadata still requires a named
collection grant. Capabilities report `scopes` and `root_uuids` separately.

The endpoint must be an HTTP(S) origin without user information, path, query or
fragment. HTTPS uses normal certificate verification. A request timeout is at
most 30 seconds. Response bodies are bounded and do not become persisted error
messages. Queue errors contain controlled codes.

## Queue contract

Provision a private directory on a durable local filesystem that supports SQLite
WAL and filesystem flushes. Keep it on the worker's persistent volume and include
it in backups; an ephemeral container layer is unsuitable. The constructor does
not create missing parent directories, so a missing mount does not silently
become a replacement queue. It rejects a different producer/origin, foreign
database lineage and a symlink in place of the database.

Use `stash_ingest.retention.retain(metadata)` on a copy, construct a supported
event envelope, serialize it with `encoding.encode`, and call `Outbox.enqueue`
before reporting the evidence captured. Event fields are documented in
[native ingestion](https://github.com/notsafeforgit/stash/blob/develop/docs/native-ingestion.md). The outbox validates the
envelope and requires source data to already satisfy `gallery-dl-retained-v1`.
The envelope has no plugin-settings field. Retention removes known secret-bearing
fields and private runtime handles; unsupported public runtime objects are
rejected. Server source-identity and domain checks remain authoritative.

When the configured extractor already yields a publisher's profile/bio, the
native server can [append its links to the linked performer](https://github.com/notsafeforgit/stash/blob/develop/docs/native-ingestion.md#performer-links-from-captured-profiles).
The server remembers URLs removed in Stash so new captures do not restore them.
This requires native schema 1000097; it needs no producer setting or additional
gallery-dl profile fetching and applies equally to host and n8n delivery.

A `file.completed` event requires the actual final relative path, positive size,
SHA-256, and image/scene kind after transformations and filesystem flushes. Its
source capture must already be queued for the same run, collection revision and root.
Delivery waits for that capture's receipt. The queue never creates a filename or
infers an attachment from a directory name; those are producer integration tasks.

An `attachment.download` report retains the original source attempt, capture and
attachment, with a stable transfer sequence taken from the capture's retained
outbox row. Start, failure, exclusion and skip reports wait for the capture receipt;
a downloaded report waits for its exact `file.completed` receipt. Each report is
at most 16 KiB. Acknowledgements must match the original event and capture and
must not claim verified media intake. Queue schema 16 admits these reports while
preserving all older payloads, acknowledgements and request/job journals.

SQLite transactions use WAL and `synchronous=FULL`. Independent drainers claim
bounded batches with expiring, fenced delivery leases. Concurrent first opens
use a bounded retry for SQLite BUSY/LOCKED responses during WAL activation,
without replacing the database. Other setup failures propagate.
Interrupted work replays
the exact original bytes and event UUID. A mismatched receipt cannot discard the
payload. Valid receipt storage and payload removal commit together, retaining a
small acknowledgement row for replay and dependent file events. Outbox delivery
never modifies the download archive; the download adapter handles it as below.

Default capacity is 10,000 unacknowledged events or 512 MiB of queued payloads.
Pending, in-flight and review events all consume capacity. Exhaustion raises an
error and must pause the producer; no event is evicted. Acknowledgement rows are
retained, so capacity bounds queued payloads rather than total disk usage. A disk
write failure propagates to the caller and must stop new download work.

Transient failures use persistent exponential backoff, from five seconds to one
day. A rejected/expired Stash token leaves the evidence pending for rotation.
Scope, schema and conflict errors require explicit review. Retrying a reviewed
event preserves its bytes; correcting its contents requires a new event UUID.

## Download lifecycle

`RunQueue` durably records and submits typed source-run requests. `RunLease`
claims the work, renews ownership and sends checkpoints or a terminal outcome.
Lease deadlines use the response's server time and a conservative monotonic
budget, so a worker's wall-clock skew cannot extend ownership. Start the lease's
heartbeat before extraction and close it when finished. Failed renewal or
progress pauses new source work; it does not discard a current file's evidence.

Construct `Producer` with the matching outbox, lease and a `filesystem.Root`
whose device/inode identity was provisioned explicitly. `NativeDownloadJob`
requires that producer and an existing persistent lock directory shared by all
host/container writers. It checks the pinned collection URL and path prefix,
mount identity and live lease before extraction/download boundaries. Child jobs
inherit those objects, and asynchronous extraction is disabled. Destination
locks cover a filename stem and its transformed encodings.

Native jobs also hold a shared filesystem-publication lock through each file's
download, postprocessing, completion and archive update. Initialization,
directory/finalization work and callbacks outside a file are guarded too. A
current file can finish after lease loss; new file work still checks the lease.
The locks are reentrant within a worker thread, including nested callbacks.
A worker that times out waiting for the backup/dedupe boundary returns
`retry` / `worker_publication_busy`. The server preserves its window, saved
progress and source failure count, and waits at least 30 seconds before another
attempt. Child/finalizer contention cannot report a successful traversal;
completed files and their durable delivery events remain available on retry.
Deploy the corresponding server retry handling before updating producers.

Reddit can configure yt-dlp's browser transport separately from gallery-dl's
`browser` setting. The `gallery` extra includes yt-dlp's supported `curl-cffi`
extra. For Reddit downloads, `extractor.reddit.ytdl.cmdline-args` can contain
`["--impersonate", "chrome"]`; gallery-dl forwards its scoped cookies through the
normal download bridge. This applies to Reddit's yt-dlp downloads without
changing other extractor categories. The two projects use different HTTP stacks,
so Chrome presets are equivalent browser profiles rather than identical TLS
configurations. Retained work needs the usual reviewed configuration upgrade.

Native jobs can keep `extractor.reddit.api: "rest"` and select
`extractor.reddit.native-api-fallback: "oauth"`. This opt-in mode requires the
configured Reddit client ID and refresh token. It tries cookie-authenticated
JSON first. Only an HTTP 403 containing Reddit's recognized network-security
page retries the same public JSON request through gallery-dl's OAuth API.
Search, individual posts, profile details and public listings are tracked
separately within the extractor: a blocked search does not move working post
lookups to OAuth. A route tries REST again after five minutes, and a new crawl
starts with REST. There is no additional persistent history or fallback database.

Both attempts preserve the query, sort, time range and `after` cursor and pass
through the native request budget. A recovered REST transport rejection remains
a recorded HTTP 403 error with a redacted `oauth_fallback` diagnostic; it is not
classified as exhausted service-wide quota. Every `Retry-After` hint is retained.
HTTP 429, authentication failures, private/missing accounts, timeouts, unknown
responses and unrecognized/private feed routes keep their ordinary handling.
OAuth failures cannot become empty successful pages or trigger a mode-switching
loop. Metadata-only post enrichment supports the same option. Existing profiles
default to no fallback; enabling it requires a reviewed policy upgrade.

For accounts where browser search works but Reddit's JSON search is blocked,
native download jobs can opt into `extractor.reddit.native-search-api: "browser"`
with `extractor.reddit.api: "rest"`. This reads the classic search HTML through
the existing gallery-dl cookie session at `old.reddit.com`, then retrieves each
result's full post JSON through gallery-dl's individual-post API. Titles, details,
dates, galleries and linked media retain their normal extraction behavior. This
is a native worker adapter; stock gallery-dl does not recognize this option.

Only search listings change. Search terms, sort/time restrictions, subreddit
scope, date/ID filters and `after` checkpoints are preserved. Full post lookups
are lazy, so each post can finish and checkpoint before the next request. The
normal source budget, rate-limit handling and browser cookies still apply; this
does not bypass challenges or promise access to unavailable results. A blocked,
unrecognized or incomplete page, changed continuation scope, or unavailable
post metadata leaves the crawl unfinished. No result-page HTML is retained.

The default remains `"api"`. Enabling this setting changes the reviewed profile
policy: upgrade retained requests explicitly while preserving their windows,
checkpoints, producer identities, outboxes and download archives. REST search
and browser search use the same `t3_...` continuation token, without a new cursor
format. No completed requests need to be reopened.

The Twitter adapter also preserves `reply_user_id` from the raw API response,
alongside the extractor's `conversation_id` and `reply_id`. Native capture intake
uses these exact numeric account and post IDs for navigable threads and shared
self-reply galleries. Screen names and retweet/quote references do not establish
a reply relationship.

The host backup coordinator uses
`stash_ingest.publication_lock.PublicationBarrier(lock_directories)` to exclude
these mutations across every reviewed worker lock root. It acquires the barrier
**before** requesting Stash's native database writer guard. A gate blocks new
mutations while existing downloads finish; the default acquisition timeout is
five minutes. A timeout fails capture instead of claiming a filesystem boundary.
Call `barrier.release()` as soon as immutable filesystem views have been retained,
before the large database copy, packing or uploads. Context exit also releases
locks after an error or an earlier explicit release. Multiple roots are acquired
in stable physical-identity order.

The two `native-publication-*.lock` files are permanent coordination objects.
Host/container profiles must share the same physical lock directories; never
remove or replace their lock files while workers are active. This protocol does
not pause legacy gallery-dl processes or unrelated filesystem tools. The existing
backup/dedupe lock is still required, and the host snapshot orchestration remains
to be integrated before production uses this barrier.

The executor requires native `source_run_pacing_protocol: 1` and
`source_run_origins_protocol: 1`. It reserves the root
service and each supported linked service before extractor initialization, which
may log in. Explicit source HTTP requests also reserve their requested service,
including additional website and CDN hosts. The server retains a canonical HTTP
origin without paths, signed queries or fragments. A busy service stops the
attempt with `source_busy`; native attempt dependencies make an
exact-window retry wait before repeating source work. Reservations are released
by completion, cancellation or expiry, and expired retries retain dependencies.

The executor also requires `source_run_fairness_protocol: 1`. Its server-derived
`turn_until` is five minutes after the attempt starts and cannot be extended by
heartbeats. The worker finishes its current file and checkpoint before yielding
at a source boundary. A resumed traversal gets one five-minute slice for new work
after reaching its saved cursor, shared by all child extractors. Checkpoints and
heartbeats cannot renew that slice. Replay remains bounded by the saved item count;
lease ownership, cancellation and source cooldowns still apply throughout. This is a
cooperative budget, not a hard wall-clock limit. An acknowledged yield returns
`state: "yielded"` and the incomplete CLI exit code. Native history records
`retry` / `source_turn_complete` while preserving progress and the normal target
cooldown, without consuming failure attempts or imposing failure backoff.

Source rate limits and request timeouts stop gallery-dl's immediate HTTP retries.
Controlled extraction, authentication and access errors identify the service
that failed, including linked extractors. Applicable cooldowns affect that service.
An individual media download failure does not become a website-wide outage, and
intentional archive/window stops retain their normal semantics. Failed ownership
checks take precedence over source error reporting. A lost finish acknowledgement
is recovered only from the exact attempt with the same outcome, code and service.

The adapter enforces the claimed half-open source-post window (`since` inclusive,
`until` exclusive) before directory, file or postprocessor handling. Reddit's
original `created_utc` and Twitter's Snowflake timestamp retain milliseconds
that gallery-dl's formatted dates discard. Raw and transformed Twitter payloads
use the same boundaries. Missing or invalid source dates stop file processing.
An older pinned post is skipped without ending traversal, so later in-range
posts are still visited. Linked child work uses its accepted parent post's date.
Postprocessor initialization waits for an accepted post, and its proposed
destination is checked before initialization or post callbacks can run.

Twitter profile URLs dispatch to the configured tweets, media, replies, reposts,
highlights or likes collection. That routing message has no publication date;
the child applies the original window to each actual tweet. This exception only
accepts the pinned profile dispatcher's post-collection routes, not undated
tweets, profile pictures or unrelated child extractors. gallery-dl 1.32.20
removed the combined `timeline` route; an `include` that still names it routes
to the timeline selected by `timeline.strategy` (normally `media`). A profile
whose `include` selects no route fails as `source_rejected` instead of
completing without reading anything.

### Twitter timelines and the history search

`stash_ingest.twitter_listing` reads the media route as two timelines, photos
then videos, and the posts route as the originals timeline. Its page
checkpoints (`twitter-listing-v1:`) record the phase, any search bound and the
remote cursor, so a resumed run continues the right timeline. A first-page
checkpoint is still the empty cursor saved by earlier releases. A checkpoint
from the removed timeline endpoints restarts its listing from the first page,
without treating archived posts as the end of a routine check.

Routine checks stop each timeline separately at its archived boundary. A post
that also has a photo is not a boundary in the video timeline, since the photo
timeline may have already downloaded it in this run.

Runs with no lower date bound (`initial` and `recovery` scans: new-account
imports and explicit recovery) then search `from:<account>` for older posts.
The search starts where the earliest-ending timeline stopped, which is the
last post it listed. Each search is cursor-paged and the next begins below the
last post the previous one reached (`max_id`), until one finds nothing. Like
upstream's removed traversal, the search is restricted to posts with links
unless `text-tweets` is enabled, with an unfiltered search only when that finds
nothing. X search can still omit posts, so this is the available history, not
proof of completeness. A protected account this viewer does not follow fails
with `access_denied`/`protected` even if X returns an empty timeline. Stash
rechecks it every two days without exhausting its retries, so its import runs
in full once access is granted.

Date limits apply to each extractor instance. They replace inherited
`date-min`, `date-max`, `date-after` and `date-before` without changing the shared
configuration file. Child extractors do not apply unrelated upload-date limits
to an accepted parent's content. Other configured predicates and skip policies
remain in effect and must be included in the worker's reviewed configuration.
Extractor keywords cannot replace the source identity or publication date.

Before downloading, the adapter queues a retained source capture and derives the
attachment from source evidence. Reddit galleries use their media IDs; ordinary
Standalone Redgifs profiles and watch URLs can use `stash-gallery download` with
the ordinary manual gallery-dl profile. Deploy the Stash reader advertising
`native:redgifs` before enabling this producer support. Each API item retains its
case-normalized watch ID and the same attachment ID used by Reddit embeds, so
existing archive entries still skip downloads. The original `createDate` owns
publication windows; a gallery member cannot inherit another member's date.
Only captured HD, SD or GIF renditions are accepted, never posters. Gallery
labels and downloader counts do not establish a complete ordered album.

The actual API `userName` stays attached to each item instead of being replaced
by the requested feed owner's name. It is handle evidence, not an immutable
account ID or proof that a Reddit publisher owns the Redgifs account. Associate
the profile with a performer explicitly; standalone fetching does not silently
expand every Reddit subscription or change existing run windows and cursors.

Reddit images and videos use direct service URLs. Linked Redgifs clips, including
legacy `v3.redgifs.com/ifr/` links, use the
captured watch/iframe ID; direct external files use their full source URL.
Redgifs image permalinks retain that direct URL identity while their captured
API item ID and rendition URLs connect the actual CDN download to it.
Imgur album responses retain their original ordered media IDs/kinds in
`album.imgur_media` version 1 before gallery-dl splits the list into files. The
album permalink and API identity bind those members to the Reddit post; each
file uses its own `native:imgur` ID. Metadata-only fetches retain the same list.
Direct Imgur GIFV/animated-image links keep their URL identity when the captured
single-item API response resolves the corresponding MP4 rendition.
A retained Reddit fallback preview can resolve to that same linked attachment.
Unrelated child downloads and Redgifs posters cannot substitute for the clip,
and one watch URL does not establish an external gallery's membership or count.
Twitter captures preserve the
original media list before gallery-dl's transformation and keep each output's
attachment ID. Output numbers and filenames do not establish attachment identity.
Contradictory identities still stop before download. A single attachment can be
associated without creating a gallery. Exact linked Imgur album/gallery API
404/410 responses use three bounded attempts, then record a durable post-level
skip and continue later posts. These decisions share the existing producer
outbox database, distinguish album IDs from image IDs and survive interruption.
A missing album does not invent an attachment, download receipt or successful
archive entry. Authorization, throttling and transport failures remain retryable.

Schema 24 adds explicit retention of completed delivery history. Upgrade every
writer sharing an outbox and all archive readers before opening it with this
version. `stash-outbox-retention --outbox PATH --library NATIVE_SQLITE --endpoint
ORIGIN` previews retirement; add `--apply` after inspecting its report. The library
is opened read-only. Run this on the host against its matching native library,
under the deployment's backup/maintenance exclusion. The default `--keep-days 1`
keeps a day of recently completed history. It also keeps every event of queued,
running, deferred, unknown, or unresolved runs, including review decisions.

Retirement requires a succeeded or cancelled native source run and an exact
match of all acknowledged receipts, their counts, and their canonical digest.
A compact immutable proof remains for each retired run. Short transactions
delete child events before parents, and a persistent sequence high-water mark
prevents transfer-sequence reuse after the queue is emptied or compacted. An
interrupted deletion resumes from its verified proof. Re-enqueuing an old event
uses its native receipt and exact payload digest; it cannot recreate retired
history or add new events to the terminal run. These exceptional receipt lookups
require the producer's normal API credential and run outside write transactions.
Gallery-dl download archives are unaffected. SQLite compaction is a separate
maintenance operation; stop shared writers and respect the host disk reserve.

After synchronous postprocessing, the adapter flushes and hashes the actual
final file, then queues its dependent file event and downloaded report before updating gallery-dl's
archive. Metadata writes use atomic replacement. Failed processors, queue
capacity or persistence failures leave the archive unacknowledged. Existing
unarchived files retry metadata/exec processing; archived files can repair queued
delivery without downloading again. Unresolved archive skips retain the source
evidence and an explicit skipped report without asserting that bytes are present.
The existing GIF-to-MKV converter is recognized explicitly.
Filename budgeting preserves source IDs and handles UTF-8 and downloader
temporary suffixes without truncating the source metadata.

A start report is durable before the downloader writes bytes. Fallback URLs use
the same transfer, with failure recorded only when they are exhausted. Failed
postprocessing, unsupported mirror files and unresolved skips retain distinct
outcomes. A process interruption may leave only a start report; the server derives
interruption from the source attempt rather than fabricating a completion. A
current file can report its original attempt after lease loss. New worker claims
require the backend's attachment-download capability before source access.

Checkpoints retain the last completed source cursor during bounded replay. A
missing saved cursor cannot report successful traversal. The worker entry point
below supplies configuration, heartbeat, event delivery and attempt outcomes;
production launcher conversion remains separate work.
Attachment adapters cover Reddit, Twitter, Instagram, Coomer, Kemono, Bluesky,
TikTok, Tumblr, the Chevereto sites JPGfish/Imglike/Putmega, LeakGallery, Erome, and
resolved yt-dlp videos. Publication windows require a usable source timestamp;
configured traversal profiles also handle undated sources and collection routing.
The direct host `gallery-dl` command still uses legacy hooks. Its native command
handoff, permitted cross-host source requests, and final host/n8n profiles remain
required cutover work.

The pinned yt-dlp bridge now checks native ownership and source reservations for
its own HTTP requests, which bypass gallery-dl's `extractor.request`. Rate limits,
transport failures, lease loss and cooperative turn limits cannot be swallowed
by `ignoreerrors` or the bridge's error handling. Fatal extraction errors retain
upstream logging/recovery actions and cannot report a partial playlist as a
successful run. Optional HTTP probes can still fall back. An already prepared
media transfer can finish after ownership loss; the next source request cannot.

Direct `url`/`url_transparent` results are resolved with `download=False` before
the first file message. This includes ThisVid's delegation to Generic, where the
final video ID is otherwise unavailable until download. Playlist traversal keeps
each child's identity. The actual leaf's `extractor_key` scopes named extractor
IDs under `ytdl:<extractor>`. Generic results use `ytdl:<webpage-host>` and a
SHA-256 key over the normalized source webpage plus video ID. This prevents
same-basename collisions across pages and keeps rotating CDN URLs, filenames,
collection labels and playlist owners out of video identity. Different video
leaves remain separate posts; playlist membership does not declare an album.

The bridge marks resolved leaves with `ytdl_media` version 1, carrying only the
resolved type. Producer and backend independently validate the source identity
and single attachment. The marker does not duplicate the full metadata and
cannot certify an unresolved URL or playlist. Existing publisher extraction
still records only explicit channel/uploader IDs and does not assign depicted
performers or infer collection ownership.

Source descriptions, titles and actual publication timestamps are retained.
An `upload_date` without a timestamp stays a day-only date; an undated video
stays undated. Neither can prove a publication-time window. A profile with
`source_mode: "traversal"` supports these downloads as configured scans instead;
the default `published` mode still rejects missing precise timestamps. Supported
cross-host origins are retained separately from their shared provider cooldowns.
The manual command below selects traversal profiles; deployment profiles and
the live command handoff still require the coordinated cutover.

## Worker profiles and execution

`stash-gallery-worker-v2` profiles name a logical media-root UUID and a local
storage registry. The registry owns mount paths, directory verification and the
shared publication-lock directory. Its verification tokens are opaque to scrape
policies. New `stash-gallery setup` installations create `storage.json` once and
share it between their profiles. New bindings use an application-owned UUID in
`.stash-storage-id` in each media and lock directory. They do not persist device,
inode or filesystem numbers, which can vary after reboot or between libc implementations.
Back up the registry and both marker files alongside the profiles.

Create a registry for an existing deployment with:

```sh
stash-ingest-storage --root ROOT_UUID /media/porn \
  --locks /persistent/download-locks --output /private/storage.json
```

The command checks existing directories, atomically provisions missing marker
files and creates a private registry. Markers contain no secrets. It reuses valid
markers, rejects corrupt ones, and never overwrites a registry, creates a media
directory or changes a queue. Interrupted setup can be retried with the same
directories; workers only verify markers and never provision replacements. It
prints the registry UUID to use in profile references. Multiple `--root UUID PATH`
arguments share one publication-lock directory. Host/container bindings can have
different local paths but must identify the same roots and physical locks.

Changing a binding requires an explicit deployment review. Keep the registry UUID,
stop every writer sharing its locks, validate the replacement directories and
switch the registry atomically. `--uuid` can prepare the reviewed replacement in
a new file. Moving a worker mount does not move files or update Stash's own local
binding. Registry UUIDs and root UUIDs are checked separately; a missing registry,
unknown root or changed directory stops work without falling back to old paths.
Preserve the marker with its directory when moving or restoring storage. A copied
marker identifies the same logical storage; do not use cloned lock directories
for concurrently running workers. Low-level descriptor checks remain confined to
detecting file or directory changes within an operation. Existing inline v1 and
0.6.0 filesystem-token bindings retain their strict historical checks until an
explicit registry review replaces them.

The v2 profile's required keys are `schema`, `root`, `storage`, `gallery` and
`bindings`. Its registry reference contains a path and UUID, and `root` contains
only the existing Stash root UUID. Optional
`source_category` restricts the root extractor; a mismatch stops before extraction.
`source_adapter` selects `gallery-dl` (default) or the explicit `yt-dlp` bridge
and participates in the policy digest. The latter preserves gallery-dl's
`ytdl:` selection semantics while Stash stores and leases the actual HTTP URL.
`stash-ingest-config --source-adapter yt-dlp` prepares that profile. For example:

```json
{
  "schema": "stash-gallery-worker-v2",
  "source_category": "reddit",
  "root": {"uuid": "REVIEWED_ROOT_UUID"},
  "storage": {"path": "/private/storage.json", "uuid": "STORAGE_REGISTRY_UUID"},
  "gallery": {
    "extractor": {
      "base-directory": "${stash:media_root}",
      "directory": ["{author}, reddit"],
      "filename": "{id}_{filename}.{extension}",
      "archive": "${stash:archive}",
      "skip": true,
      "reddit": {"cookies": "${stash:reddit_login}"}
    }
  },
  "bindings": {
    "archive": {"kind": "path", "path": "/persistent/archives/reddit.sqlite"},
    "reddit_login": {
      "kind": "private",
      "file": "/private/gallery-dl/config.json",
      "pointer": "/extractor/reddit/cookies"
    }
  }
}
```

This template requires the existing Stash root UUID and the created registry UUID.
Use the actual shared lock directory and existing archive/path rules when
converting a deployment. The directory produced by gallery-dl must fit the
claimed collection's path prefix. Root and lock identities are checked again
at source boundaries inside the storage adapter; a replacement mount stops further work.

Retained v1 profiles remain readable in `retained_storage.py`. With an explicitly
selected v2 profile, the dispatcher resolves their logical root UUID through the
current registry without modifying frozen documents or policy hashes. Existing
requests, coverage, cursors, producer schema 19 and receipts remain unchanged.
A broken new gallery layer can still run retained instructions with current
storage. An unreadable profile or unavailable registry pauses execution. A v1
installation without a registry keeps its historical strict inline checks,
including any previously reviewed `filesystem_id`; it is never silently upgraded.

The registry can also bind archive directories for both current and retained jobs:

```json
"archive_roots": {
  "/old/archive-directory": {"path": "/persistent/archives", "token": "marker-v1:REVIEWED_UUID"}
}
```

Use `stash-ingest-storage --archive-root OLD_DIRECTORY DIRECTORY` during explicit
storage setup to provision the directory marker. Aliases must be disjoint absolute
directories. Only `archive` settings relocate; their relative filenames, archive
keys, prefixes and tables stay unchanged. Markers are checked before execution and
at source boundaries. A missing mount or unreadable configured archive stops new
downloads instead of silently starting an empty archive. The dispatcher applies
the current registry even to saved v1 policies, without rewriting their documents,
receipts, cursors or policy identities. Back up the archive directory and marker.

`path` bindings relocate archives and other local files without changing policy
identity. `asset` bindings add `path` and a reviewed `sha256` for local helper
code. Python postprocessor `function` settings must refer to such an asset,
for example `${stash:converter}:prepare`. Asset digests are checked when loading
the profile and their file identities are checked during execution. Put local
conversion helpers used by exec processors in asset bindings too. This does not
sandbox trusted postprocessors or fingerprint arbitrary system executables.
Legacy `gallery_catalog_hook` writers are rejected, including named definitions.

`private` bindings read a JSON Pointer from an existing local JSON file, or use
`{"kind":"private","env":"WEBSITE_ACCESS"}`. A complete reference can supply
structured cookies or headers. Layered access settings use
`{"kind":"private","sources":[{"file":"…","pointer":"…"},…]}` and merge
objects in order, replacing scalar/list values as gallery-dl does. Private
references are accepted only in website access settings and the values of known
yt-dlp access arguments such as `--cookies` or `--password`. They cannot be
interpolated into filenames or shell commands. These values
stay in worker memory and gallery-dl; they are not sent to Stash or copied into
the outbox. The Stash API token still uses the separate `--token-env` option.

The policy SHA-256 includes the portable gallery configuration, reviewed asset
digests and the versioned execution/capture contracts. It
excludes private values, local binding paths, directory identities and the
requested run window. Equivalent host/container profiles therefore share a
policy; rotating cookies does not create a new policy. Changing extraction or
conversion settings/code does. Upstream downloader patches do not change the
policy; runtime diagnostics separately report their actual versions and commits.
Conditional map order is retained in the digest
and saved profile: rearranging first-match filename rules changes the policy.
Relative binding paths resolve against the
profile file. Keep one worker in each process: gallery-dl configuration is
process-global, and simultaneous activation is rejected.

`worker-policy --profile FILE` validates the local profile and prints its policy
digest, root UUID and source mode. `queue-run --profile FILE` uses that digest in place of
`--policy`. `execute-run RUN_UUID --profile FILE` first checks server download
capability, root/operation/policy agreement and live ownership. It starts the
heartbeat and a drainer with its own SQLite connection, then runs gallery-dl.
Temporary failures request retry with exponential backoff capped at 24 hours;
download runs have no automatic eight-failure hold. Missing accounts and access-denied responses
request daily automatic rechecks without increasing the technical failure backoff;
other retrieval passes for the account remain eligible. Authentication,
challenge, source-validation and configuration failures defer, and loss of
ownership pauses further source work. Current-file events remain
durable. A lost finish response is accepted only after finding the exact
producer, owner, fence and outcome in the server's attempt history.

The execution command prints JSON on stdout; downloader and child-process logs
go to stderr. Exit 0 and `source_succeeded` mean this source **attempt** finished.
Other source windows and media jobs can remain pending. `run_state` reports the
acknowledged run state when available; `intake_completion` explicitly requires
checking native receipts. Waiting, retry, deferred, paused and unconfirmed
completion exit 2. Invalid configuration or unavailable admission exits 1.
Continue draining the durable outbox after execution; stopping the download
process does not certify that its file events were delivered.

## Automatic source dispatch

### Shared gallery-dl configuration

Use `stash-ingest-config --layered` to reference existing JSON files instead of
copying their settings into every worker profile. Supply `--config` in the same
order as gallery-dl: the shared config first, then environment/site overrides.
Objects merge recursively; lists and scalar values replace earlier values.
Conditional filename/directory maps retain their order.

For example, a `stash-gallery-worker-layers-v2` profile contains the normal
logical `root` and `storage` reference plus:

```json
{
  "config_files": [
    "/home/node/.n8n-files/gallery-dl/config.json",
    "/home/node/.n8n/workflowConfigs/n8n-gdl-overrides.json"
  ],
  "working_directory": "/home/node",
  "source_category": "reddit",
  "scan_mode": "initial",
  "full_history": true
}
```

Host routine profiles reference the same shared file at its host path, use
`scan_mode: "routine"` and omit `full_history`. The latter supplies the existing
global `skip: true` override for initial/recovery scans. Small site-specific
overrides, such as Twitter recovery traversal, belong in another JSON layer.
No new include syntax is required inside the shared gallery-dl files.

New caller executions resolve these files automatically. Before admission, the
producer saves one normalized configuration per profile/root/policy in its
outbox (`worker_configurations`, producer schema 17). Queued callers, repeated
n8n executions, interrupted jobs and retries keep their original policy. The
dispatcher rotates across current and saved policies, so a config edit does not
strand queued work. An idle older policy is checked at most every five minutes.
These snapshots contain credential references, never resolved tokens/cookies;
credential rotation remains possible. Referenced helpers remain hash-checked.
A producer/helper code upgrade still requires the existing explicit policy
upgrade procedure; changing config does not authorize replacing executable code
for an already admitted run.

When retiring enrichment policies after a worker upgrade, stop their dispatchers
and preview `stash-ingest --outbox FILE --endpoint STASH_ORIGIN --producer UUID
retire-enrichment-progress --retired-policy SHA256` (repeat `--retired-policy` for
each reviewed, no-longer-configured policy). Add `--apply` to remove those progress
cursors. Include all workers sharing the outbox in the review. Policies with
active or review jobs are protected; jobs, saved metadata, queued deliveries and
server records are never removed. This is a local operation with no website or
Stash API requests. Repeat after retained jobs finish if protected cursors remain.

At the 10,000-cursor limit, metadata dispatch also reclaims up to 1,000 expired,
empty cursors from other policies. Partial pages, backoff, the selected policy
and collections with unfinished local jobs remain protected. Recreated cursors
reject stale updates from an earlier dispatcher. If reclamation cannot free a
slot, the worker reports `dispatch_capacity_exhausted` with the table, count and
limit, and backs off; this does not mean posts or media were discarded.

Automated deployments can run `python -m stash_ingest.dispatch_reconcile` with
`--outbox`, `--library`, `--endpoint`, and one `--profile-directory` for every
directory shared by that outbox. Preview is the default; `--apply` retires only
obsolete enrichment dispatch bookmarks. Hold deployment/backup exclusion while
reading profiles and applying the result. Inventory includes direct metadata
profiles, disabled worker profiles and metadata references in dispatcher files.
It validates public settings and assets without loading website credentials,
including profiles mounted at different paths in containers. Missing, empty,
unknown or changing inventories prevent retirement. A deliberately empty
deployment still requires the explicit retirement command above.

The reconciler also reads native metadata jobs and approved policy upgrades.
Unfinished original-policy jobs remain protected unless a direct approved
replacement is configured. Pending delivery bodies, reserved extraction space,
review jobs, running attempts and missing/mismatched native jobs remain protected.
An upgraded queued or ended job without pending local evidence can release its
original bookmark; its immutable journal, checkpoint and native history remain.
Policies with no remaining work are retired on a later pass. The delivery-only
cursor is never retired. A busy metadata execution defers cleanup to the next
pass. No jobs, media, receipts or website state are deleted or fetched, and no
policy upgrades are approved by this command. `--report` atomically replaces one
current report instead of accumulating another receipt history.

Keep the producer outbox, profiles, shared files and override files in backups.
The worker inventory resolves their dependencies, including those still needed
by saved configurations. When upgrading an existing deployment, register its
old effective profiles in the outbox before replacing them with layered profiles
and preserve every pending run's original request and execution progress.

`dispatch --profile FILE` performs one bounded scheduling cycle: deliver one
ready event batch, resolve up to 50 pending caller URLs, submit one queued source
request, then discover up to 50
eligible runs and execute at most one claimed attempt. It uses the scoped
`POST /runs/ready` API, filtered by the reviewed root and policy. The server
returns only run UUIDs and pagination sequences. A discovery result grants no
ownership; the existing claim still checks collection definitions, destination
overlap, cooldowns and fencing before any source work starts.

```sh
stash-ingest --outbox /persistent/producer.sqlite --endpoint STASH_ORIGIN \
  --producer PRODUCER_UUID dispatch --profile /persistent/profiles/reddit.json
```

The producer retains its pagination cursor and discovery backoff in the outbox.
A page of busy candidates cannot permanently hide later work, including after
process restart. Cursor changes use revisions so competing dispatchers cannot
both advance the same page state. End-of-list wraps the cursor for a later pass;
this does not acknowledge any scrape as completed. Expired attempts go through
the server's recovery/backoff path. Deferred work requires explicit review.

Delivery and request admission continue while discovery is backing off. A cycle
can report `idle`, `waiting`, `backoff`, `contended`, `unavailable`, or a worker
attempt outcome. Exit 2 denotes unfinished local resolution/delivery/admission or an
unsuccessful attempt; exit 0 only describes this cycle. Neither certifies the
whole source queue or media intake. Preserve the result's native run identity
and inspect its state and file receipts when a workflow needs completion.

The [host worker units](#host-worker-service-and-timer) run `dispatch-all` across
the reviewed local profiles. Host/n8n wrapper activation and migration of old
operational receipts remain required. These commands do not update installed
launchers.

### Host worker service and timer

The templates in [`systemd/`](systemd/) provide independent user service instances.
Use one per source site so a long traversal does not occupy every site's worker.
The `host` instance reads
`~/.config/stash-ingest/host.env`; its profile list uses the
[`stash-gallery-dispatch-v1` format](#dispatch-across-local-profiles). Configure
the installed executable, durable outbox, Stash origin, registered producer
UUID, reviewed profiles and scoped ingestion token with absolute paths. The
example contains placeholders. Keep the completed environment file private
(mode `0600`) and include it, the profile dependency closure and the outbox in
the deployment's backup inventory.

At the reviewed native cutover, after registering the producer and validating
the installed runtime and its profile policies, install and start the units:

```sh
install -d -m 0700 ~/.config/stash-ingest
install -d -m 0755 ~/.config/systemd/user
install -m 0644 integrations/gallery-dl/systemd/stash-ingest-worker@.service \
  integrations/gallery-dl/systemd/stash-ingest-worker@.timer \
  ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now stash-ingest-worker@host.timer
```

Provision the completed `host.env` before starting the timer; copying the
placeholder file is not configuration. These are user units, so unattended
operation requires the user's service manager to remain running after logout.
Use a separate producer identity, outbox and profile list for each runtime.
An n8n container needs its worker in the container's reviewed runtime with its
own path mappings; a host unit cannot substitute for that worker just by
pointing at container paths.

The timer starts its first cycle one minute after the user manager starts
(immediately if enabled later) and schedules subsequent cycles 30 seconds after
the service becomes inactive. Timer coalescing and random delay are configured
at five seconds each; system load can postpone execution further. A cycle can
contain a long source attempt. The
service has no start timeout, and an already-running instance is not launched
again by either its timer or another `systemctl start` for that same instance.
This prevents timer overlap for that instance; native source leases still
coordinate other producers and manual/n8n requests.

Exit `2` is accepted because a cycle may leave pending, deferred or review work;
exit `0` also describes only that cycle. Inspect its JSON outcome and native
run/file receipts for completion. Configuration and execution failures remain
visible in the journal, and the timer can retry on its next cycle. The service
stops its whole process group; unfinished durable work resumes through outbox
delivery and fenced lease recovery.

```sh
systemctl --user status stash-ingest-worker@host.timer stash-ingest-worker@host.service
journalctl --user -u stash-ingest-worker@host.service -n 50
# Stop the timer and any active cycle before replacing its runtime/configuration.
systemctl --user stop stash-ingest-worker@host.timer stash-ingest-worker@host.service
```

## n8n worker image

`Containerfile.n8n` adds the pinned producer and gallery-dl/yt-dlp dependencies
to `/opt/stash-ingest`, an isolated Python environment. Build on the existing
customized n8n image, pinned by digest or immutable local image ID; it must
already supply Python 3.12+, uv, git, ffmpeg and the retained conversion tools.

```sh
podman build --pull=never \
  --build-arg N8N_BASE_IMAGE=PINNED_CUSTOM_N8N_IMAGE \
  --build-arg STASH_REVISION=SOURCE_COMMIT \
  --tag localhost/n8n-native:SOURCE_COMMIT \
  --file integrations/gallery-dl/Containerfile.n8n integrations/gallery-dl
```

The build installs baseline downloader commits with uv: gallery-dl from Codeberg
and yt-dlp from GitHub. Deployments can independently install newer commits and
test them against the adapter before activation. The optional operations image
does this on startup in a separate environment, with the baked baseline and last
working installation as fallbacks. Runtime diagnostics report actual versions and
Git commits because development branches can retain an older version label.
Downloader updates preserve queue policies; changes to the adapter or reviewed
configuration still require a policy handoff. Never modify a running worker's
environment in place.

Metadata job `extractor_version` now identifies the native capture adapter
(`stash-gallery-v1`), whose checkpoint format is independent of downloader
patches. Download capture events still report the actual gallery-dl version.
Upgrade-only handling finishes previously admitted metadata jobs using their
original version header and captured prefix; new jobs use the adapter contract.
It does not relabel retained observations or refetch completed checkpoint parts.

`dispatch-all --max-passes 16` keeps claiming ready work after an ended attempt
instead of waiting for an external timer after every account. Every pass still
uses the same server priorities, request budgets, retry deadlines and leases.
It returns when idle, blocked, unavailable or interrupted, and after at most
sixteen passes. The default remains one pass for callers that want a single
bounded scheduling step. Results include `dispatch_passes` and a bounded
`dispatch_attempts` summary without repeating outbox contents.

Native launchers use `/opt/stash-ingest/bin/stash-ingest` and its sibling Python
interpreter. n8n's existing PATH, entry point and system Python environment stay
as supplied by the base image. Private website config, Stash token references,
profiles, outboxes, download archives and media are deployment mounts, never
image build inputs. Select the new image only at the verified cutover after
converting workflow/host entry points and their receipts.

A local image ID pins the running container but cannot be downloaded on a new
host. Retain an OCI image export as an explicit backup component when an image
has no published registry digest. Include the task-runner image if it is also
local-only. Keep exports immutable by image ID so incremental backups reuse
their contents. Restore with `podman load --input <image.oci.tar>` and verify the
loaded ID before starting its Quadlet. A Containerfile referring to an
unretained intermediate local image is insufficient recovery coverage.

An existing deployment can update only the pure-Python producer package through
a versioned, read-only mount of `stash_ingest` over the environment's installed
package directory. Build a wheel from the committed source and verify every
module against that source. This option requires unchanged dependencies,
entry-point definitions and `stash_ingest_bootstrap.py`; otherwise rebuild the
runtime through its release pipeline. Verify the overlay in a disposable
container using the same image and Python environment before activation.

For a package update, briefly stop producer timers/services and preserve their
outboxes. Deliver retained source requests with their original bytes and runtime
before installing the same verified wheel for host callers. Retain all
profile definitions; review the changed execution fingerprints and apply the
bounded policy upgrades to existing pending work before resuming the original
timer cadences. Keep the image identity separate from the overlay's source
revision. Declare the wheel, module manifest, mounted package directory and
Quadlet changes in the coordinated backup configuration, so recovery restores
the exact package rather than relying on mutable workspace files.

Use the `stash-ingest` console command for download workers. Its bootstrap
initializes gallery-dl logging once, including the lowercase levels used by
configured actions such as `error:network security`. Logs go to stderr while
the command's JSON result stays on stdout.

## Shared source backoff

Workers require native `source_request_backoff_protocol: 1` (schema 1000127).
HTTP 429 responses and Reddit's recognized HTML network-security block stop
extraction and report `rate_limited` for the affected service. The HTTP status is
preserved; the Reddit block additionally reports `block_reason: network_security`.
Block detection examines the complete buffered HTML response, including messages
after large inline styles; it never stores the response body in the request report.
Ordinary private/missing accounts and individual media failures keep their own
handling. No response bodies or website credentials enter the pacing records.

Stash persists the cooldown and recovery probe in its existing source request
budget, shared by downloads, metadata enrichment and every producer. An explicit
Retry-After/reset hint controls the delay (bounded to one day by the protocol);
otherwise retries wait 1, 2, 4, 8, then at most 15 minutes. Only one ordinary queued
request probes recovery. A normal response restores ordinary pacing; another
block or an inconclusive probe renews the delay. Probe leases expire after two
minutes, and late responses or repeated acknowledgements cannot clear or extend
a newer outage. Successful responses with exhausted-rate headers still defer
subsequent requests until the reported reset.

A reported service outage retains pending windows, pagination, captured evidence,
receipts and metadata checkpoints. It neither increments the account/job failure
counter nor adds the older five-minute/exponential account delay. No work is
marked complete or skipped because of throttling. Deploy the native server before
these workers; the capability check prevents starting extraction on older servers.
Saved worker policies and producer schema 19 remain unchanged.

## Converting existing gallery-dl settings

`stash-ingest-config` reads ordered JSON config layers and writes a **new,
inactive** worker profile. It requires the reviewed root UUID and local storage
registry. It does not register a root, contact a website,
start downloading, rewrite the input files or activate a launcher.

```sh
stash-ingest-config \
  --config /private/gallery-dl/config.json \
  --config /private/gallery-dl/worker-overrides.json \
  --category reddit \
  --root ROOT_UUID --storage /private/storage.json \
  --working-directory /media/porn \
  --output /persistent/profiles/reddit.json
```

The command recursively merges objects and replaces arrays/scalars exactly as
gallery-dl does. It retains insertion order, filenames, archive formats/IDs,
original-quality flags, skip rules, pacing and remaining processors. Recognized
legacy `prepare`/`complete` catalog hooks are removed from named, typed and inline
definitions; unknown catalog callbacks require review. Archive segment lists
remain lists so formatted archive names still use gallery-dl's path handling.
Local Python/shell helpers in argument-array processors become asset bindings;
`--asset PATH` can identify additional helpers. Shell-string processors and
ambiguous path forms require explicit review rather than silent rewriting.

Access values reference their original input file and JSON Pointer, including
partial merges of structured headers/cookies and credential argument values.
Other yt-dlp arguments remain visible in the policy; changing format selection
must not be hidden as a credential change. Input files remain the private access
source, so the eventual cutover must keep those references valid when retiring
legacy settings. No credentials are printed in the conversion report.

`--category twitter`, `instagram`, `coomer`, `kemono`, `bluesky` and `tiktok` remove unrelated extractor
settings and unused named processors. Reddit can do the same when its existing whitelist limits children
to Reddit, Imgur, Redgifs and direct links; their base and parent-specific
settings remain. Unrecognized dependency graphs retain all configured sites.
This prevents unrelated ThisVid recovery settings from splitting otherwise
equivalent Reddit/Twitter worker policies. The root-category constraint also
prevents using the resulting profile for another service.

Create a separate profile with `--full-history` for the host wrappers' historical
`-o skip=true` override. This sets the global gallery-dl `skip` option, which
overrides extractor and child-specific `abort:4` settings without removing them.
The normal and full-history profiles have different policy digests. This option
does not clear date limits or bypass the download archive; already downloaded
files can still be skipped while traversal continues past them.

The report lists removed/excluded setting paths and binding counts, plus the
validated policy/root identity. Publication uses a flushed temporary file and
an exclusive atomic link with private permissions. An existing output is never
replaced. Missing mounts, helpers or pinned runtime prevent publication. Deploying
the new profiles and converting host/n8n/recovery launchers remain separate steps.

## Offline source requests

Profiles may set `source_mode` to `published` (default) or `traversal`.
`stash-ingest-config --source-mode traversal` selects the latter when
preparing an inactive profile. The mode is part of the reviewed policy digest.
Date-based host launchers and n8n backfills require `published` profiles.
`queue-run` and `queue-sources` derive the mode from `--profile`; a low-level
caller using `--policy` can explicitly pass `--source-mode traversal`.

For a configured scan, omit `--since` and `--lookback-seconds`. `--until` records
the frozen request time, not a source date. The resulting window contains
`basis: "traversal"`, `since: null` and that timestamp. It preserves the profile's
filters, date settings, skip rules and stopping behavior. Completion certifies
only that configured scan, never a complete historical backfill. No publication
timestamp is invented. Pending requests coalesce to the latest timestamp, but a
newer request remains pending while an older scan runs. Published requests remain
separate, and neither kind supplies coverage for the other. Submission and
execution require server capability `source_run_traversal_protocol: 1`.

`run_queue.RunQueue` shares the event outbox database and producer/origin binding.
It accepts only a collection UUID/revision, operation, configuration fingerprint,
cooldown and an absolute half-open published-time window. It stores no commands
or website credentials. `since: null` means all earlier history; `until` is always
explicit. Window timestamps normalize to UTC with millisecond precision using
the same fixture corpus as the server.

Equivalent pending requests merge overlapping or adjacent windows. Disjoint
windows keep their gaps. Before HTTP submission, one window is frozen with a
stable request UUID, canonical bytes and digest. New requests preserve that
in-flight window and queue only additional coverage. Backoff and review block
further submissions for that configuration; a repeated timer does not reset them.
Ready download work precedes enrichment, and newer windows precede old history.
Concurrent senders use expiring fenced leases without holding a database
transaction during HTTP calls.

`submit_once` checks source-run capabilities, sends the frozen bytes, and requires
an admission response identifying that exact request and collection definition.
It atomically retains the response/run UUID and releases the request body.
Lost responses replay the original request. The state is **admitted**, including
when the response says the server run is queued or deferred. It never means the
scrape or media ingestion completed. Inspect the native run's current status;
claim a live source lease before starting extraction. An outage only queues work.

Admission capacity bounds configurations and caller tickets still waiting for
native admission: pending windows, pending/in-flight/review requests, and tickets
with unassigned windows or unadmitted assignments. Retained configurations,
tickets and admission receipts do not consume those slots. Reusing an idle
configuration needs a free slot; coalescing into an active one and replaying an
existing ticket remain possible at capacity. Receipt history remains durable, so
these limits do not bound total disk usage. Status reports active usage and limits
separately from retained totals.

If URL resolution reaches admission capacity partway through a page, committed
bindings remain intact and remaining targets retry with backoff. The dispatcher
continues submitting and executing queued work; admission pressure must not stop
the work that frees capacity.

Callers that can retry a command after losing its response should supply a stable
`ticket_uuid` to `enqueue` (CLI `--ticket`). A ticket replay returns its existing
intent even after server admission. Changed request contents under that ticket
are rejected. Persist the absolute cutoff with the caller's execution identity;
recomputing a relative window on retry would change the request. A new scheduled
execution uses a new ticket. Ticket inspection includes its requested window and
the first applicable request sequence for paginated history; the history is not
a completion receipt for the whole requested range.

`ticket-status UUID` checks that range against the native runs originally
assigned to the ticket. Assignments are saved when a request is frozen, in the
same transaction as its pending-window removal. A caller joining an existing
frozen request shares its overlapping portion; any additional ranges retain
their own assignments. Neither retries nor later rescans replace those links.
A cancelled request stays cancelled even if an unrelated later run succeeds.

The inspector reads each assigned run's current completed windows, validates its
admitted collection/root/policy, and reports any remaining ranges. It reports
`source_succeeded` only when every assigned range is covered, even if a wider
shared run still has other work. Missing status, deferral, review and incomplete
coverage remain visible. An API outage returns `unavailable`, not a cached
success. Exit code 0 certifies this ticket's source traversal only; file intake
still requires the native event receipts. This is the completion check intended
for callers that record permanent backfill decisions.

Capacity defaults are 10,000 retained configuration groups, 64 disjoint pending
windows per group and 100,000 caller tickets. Exhaustion stops admission without
evicting work. Request acknowledgements and tickets remain as history, so these
limits are not a cap on total disk usage. Queue status reports pending windows,
submission states and age; a fresh schedule resets the age of an emptied group.

Producer schema 2 introduced request/ticket tables; schema 3 added dispatch
cursors and discovery backoff. Schema 4 added durable ticket-to-submission links
and unassigned ranges. Schema 5 adds caller snapshots and source bindings;
schema 6 adds durable backfill calls, history checks and completion proof.
Schema 7 adds retained n8n receipt history; schema 8 adds the enrichment execution
journal; schema 9 adds enrichment discovery cursors and backoff; schema 10 adds
rotation across worker profiles and permitted metadata collections; schema 11
adds durable discovery page delivery; schema 12 adds its dispatch cursors and
backoff; schema 13 adds discovery collection rotation and separates discovery
and enrichment delivery cursors in the shared worker. Schema 14 adds candidate
detail execution, collection dispatch and an independent delivery cursor. Schema
15 adds the cursor for automatic candidate inspection. Schema 16 admits durable
attachment-download reports and their capture/file dependencies.
Opening an outbox from schemas 1–15 promotes it
in one SQLite transaction, preserving event bytes, receipts, dependencies,
active delivery/submission leases, frozen requests and caller tickets. Old tickets
are linked to the first covering submissions from their original request sequence;
pending portions stay unassigned. Both tickets and requests are read in bounded
pages. A failed migration rolls back. Older producer code refuses the new schema when opening
it; preserve the queue in backups rather than recreating it during rollback.
This does not change the Stash database schema.

Producer outbox schema 26 (this release) drops the discovery execution and
dispatch journal tables (`discovery_executions`, `discovery_detail_executions`,
`discovery_dispatch`, `discovery_collection_dispatch`, `discovery_detail_dispatch`
and `discovery_detail_collection_dispatch`) and the discovery delivery columns of
`worker_dispatch`. The migration is transactional. It refuses to run, leaving the
outbox unchanged with a clear error, while any retained discovery execution is
active or in review, or holds an unacknowledged body or reserved bytes. Completed
and failed discovery rows without bodies are dropped. Deploy this release together
with the Stash server containing native schema 1000142. That server deletes the
discovery archive jobs, so an outbox still at schema 25 with discovery history must
be upgraded first; its completed and failed discovery rows reference jobs the
server no longer has.

## Manual downloads

`stash-gallery-dl` accepts one to fifty HTTP URLs, including `ytdl:`-prefixed
URLs, and `-i FILE` lists containing one full URL per line. Blank lines and
whole-line comments are ignored; extra command flags or inline comments in a
list are rejected. Downloader settings come from reviewed profiles. Unsupported
gallery-dl command-line flags produce an error rather than being ignored.

```sh
stash-gallery-dl https://www.reddit.com/user/Example/submitted/
stash-gallery-dl ytdl:https://thisvid.com/videos/example/
stash-gallery-dl -i /persistent/targets.txt --queue-only
stash-gallery-dl --call SAVED_INVOCATION_UUID
```

Set the usual `STASH_INGEST_OUTBOX`, `STASH_INGEST_ENDPOINT`,
`STASH_INGEST_PRODUCER` and `STASH_INGEST_TOKEN` values, plus
`STASH_INGEST_MANUAL_CONFIG` pointing to this runtime document:

```json
{
  "schema": "stash-manual-gallery-v1",
  "state": "/persistent/manual-requests",
  "profiles": {
    "gallery-dl": "/persistent/profiles/manual-gallery.json",
    "yt-dlp": "/persistent/profiles/manual-ytdlp.json"
  },
  "new_source_policy": {
    "enabled": true,
    "apply_to_scans": false,
    "rules": {
      "scene": {
        "on_create": true, "on_existing": false,
        "skip_organized_on_create": true, "mark_organized": false,
        "filename_title_fallback": true, "mappings": {}
      },
      "image": {
        "on_create": true, "on_existing": false,
        "skip_organized_on_create": true, "mark_organized": false,
        "filename_title_fallback": true, "mappings": {}
      }
    }
  }
}
```

Create the canonical state directory first. Both profiles must use
`source_mode: "traversal"`, omit `source_category`, select their respective
`source_adapter`, and share the same verified media root and lock directory.
Convert the existing complete gallery-dl configuration so its service-specific
directories, filenames, download archives, filters and originals preference
remain in effect. Prefer layered profiles referencing that same shared config,
with `scan_mode: "initial"` and `full_history: true`, so one-off downloads pick up
config edits without maintaining another copy. For example, a Kemono profile URL
passed to `stash-gallery-dl` uses the same durable intake as scheduled scrapes;
the invocation itself does not create a daily monitoring subscription.
Configure `new_source_policy.rules` using the native metadata
policy schema; this example only supplies filename titles on creation, with no
source-field mappings. Source registration
and policy initialization use `STASH_API_KEY`, or the environment variable named
by `--api-key-env`. This is separate from the scoped ingestion token and local
website-access references.

Before queuing, the command saves its input snapshot and guarded registration
plans. Existing unique active sources retain their labels, namespace, ownership,
directory and metadata policy. Disabled, retired or ambiguous sources require
review. A new source is a `manual_batch` collection without inferred account or
performer ownership, with its configured media root and broad `.` path prefix.
That prefix serializes overlapping root work until it is explicitly narrowed.
Only newly created collections receive `new_source_policy`; each downloaded
leaf establishes its own captured source identity, including links to other sites.

Foreground execution runs only this invocation's admitted downloads and waits
through pending source work. `--queue-only` reports `recorded`, leaving execution
to the background dispatcher. Include both profiles in that dispatcher's reviewed
profile list. `--max-wait SECONDS` stops waiting between worker steps; it does not
cancel durable requests or interrupt a current download. `--dry-run` validates
targets and local profiles without creating state or making API calls.

Resume with the printed invocation UUID even if the original list has changed
or disappeared. The original targets, request time and profile policies stay
fixed; retain those profiles while work remains pending. Registration and
completion recover their original receipts after lost responses. Exit 0 in
foreground mode means the source traversal succeeded; native file intake and
metadata jobs still have their own receipts. Review, cancellation or deferral
exit 2; a wait limit or unavailable completion status exits 3. Ctrl-C exits 130
and leaves saved work resumable.

Back up the complete manual state directory as operating state under the same
publication barrier as its producer outbox. It holds registration plans that
may exist before any runnable source call. Retain the runtime, both profiles and
their local bindings in the worker inventory. Installing the entry point alone
does not replace the host's current `gallery-dl` executable. The staged
`bin/gallery-dl` convenience launcher reads the owner's private
`~/.config/stash-ingest/host.env` (or `STASH_INGEST_ENV_FILE`), uses the Python
beside `STASH_INGEST_EXECUTABLE`, and selects `STASH_INGEST_API_KEY` for source
management. It runs that installed runtime with Python import isolation.

## Durable caller lists

`queue-sources` records one execution with a caller-provided UUID, an ordered
URL list, reviewed policy, logical root, operation and frozen absolute window.
It works before Stash is reachable or the sources have collection bindings.
The UTF-8 file uses the first token of each nonempty, non-comment line;
duplicates retain their first position. Entries must already be exact HTTP(S)
source URLs. The host launchers below expand Twitter handles and Reddit modes
before recording the snapshot; this low-level command does not reinterpret those
lists. The n8n adapter below adds its permanent-history and workflow-result contract.

```sh
stash-ingest --outbox /persistent/producer.sqlite --endpoint STASH_ORIGIN \
  --producer PRODUCER_UUID queue-sources --call CALL_UUID \
  --targets-file /persistent/source-urls.txt \
  --profile /persistent/profiles/reddit.json --lookback-seconds 604800
```

When `--until` is omitted, the first committed snapshot fixes the current UTC
cutoff. `--lookback-seconds` subtracts from that cutoff; `--since` supplies an
absolute lower bound instead. Omitting both requests all earlier history.
The time window does not change the reviewed profile's archive/skip behavior.
Full-history host launchers require the separate reviewed global `skip=true`
profile described above.

Repeat the same caller UUID and options after a lost command response. The
original URL list, policy and window are retained even if the files changed or
disappeared; preparation is not repeated. Changed options under that UUID are
rejected. A fresh scheduled execution uses a fresh UUID. Concurrent first
requests retain whichever snapshot commits first. Local acknowledgement is not
source or media completion.

`resolve-sources` and `dispatch` resolve at most 50 targets per cycle. Resolution
prioritizes downloads over enrichment and newer cutoffs within each operation,
matching the subsequent submission queue. Each unique
active collection match and its deterministic source ticket commit in one local
transaction. Interruption cannot leave a queued ticket without its retained
binding. Existing source-request coalescing still combines equivalent work from
different callers. Bound targets are never looked up again or silently moved to
another collection/revision. Unresolved, ambiguous, disabled and retired matches
remain in review. After fixing the source definitions or access grants,
`retry-call UUID` retries only the reviewed targets; already queued work remains
bound. Transient lookup failures retain fenced leases and persistent backoff.

`calls-status --call UUID --after N` pages through 50 local targets at a time.
`call-status UUID` checks every target's original source ticket and requested
window, sharing a bounded cache only within that inspection. It reports success
only when all targets have confirmed source coverage. Later rescans cannot
complete a cancelled earlier call. Outages and mismatched bindings remain
visible; diagnostics include at most 20 affected URLs with an explicit truncation
flag. Media intake still requires the native event receipts.

A snapshot holds at most 10,000 distinct URLs and 8 MiB of URL data. The default
queue limits are 10,000 retained calls and 100,000 retained targets. Capacity
failure preserves old records and rolls back the new snapshot. These records,
bindings and ticket relationships are part of the persistent producer outbox
and its backup/restore boundary; schema migration invents no past caller runs.

## Staged host launchers

The package entry points `stash-ingest-twitter`, `stash-ingest-reddit`,
`stash-ingest-instagram`, `stash-ingest-coomer`, `stash-ingest-kemono`,
`stash-ingest-bluesky` and `stash-ingest-tiktok` record source calls through this queue. The first two
are also available as `bin/update-twitter-media` and `bin/update-reddit-media`.
Run the scripts with the installed producer's
Python. They are staged replacements; the existing host scripts and n8n workflow
commands have not switched.

| Input | Retained behavior |
|---|---|
| Twitter `--username`, `--user-id`/`--gid` | Handles, profile URLs and numeric account IDs use the existing normalized X URLs |
| Instagram `--username` | Handles become profile URLs; explicit post/reel/story/highlight URLs keep their path and query |
| Coomer/Kemono `--url` | Explicit account, post or posts-listing URLs retain their underlying service, account ID, path and query; other container types require separate adapters |
| Bluesky/TikTok `--url` | Keeps exact supported post/profile/collection URLs and TikTok shortlinks; standalone artwork and following traversal require separate adapters |
| `--config-file` | Twitter retains first-token/comment handling and input order; Reddit sorts profile/community names before expansion |
| Instagram/Coomer/Kemono/Bluesky/TikTok `--config-file` | Preserves URL order and removes exact duplicates; blank/comment-only lines are ignored and invalid lines stop the request |
| Reddit `--mode new\|top`, `--subreddit`, `--saved` | Retains profile/search URLs, all/year top variants, communities and explicit saved-post targets |
| Reddit `--date-min`, `--date-min-relative`, `--date-min-days` | Freezes the first request's lower bound; the absolute option takes precedence |
| `--full-history`, Reddit top mode | Requires the reviewed profile with global `skip=true`; retains any date minimum |
| `--dry-run` | Prints URL/window expansion without a profile, API request or outbox creation |

Saved-list defaults remain `~/.config/gallery-dl/twitter-list.conf`,
`reddit-list.conf`, `instagram-list.conf`, `coomer-list.conf`, `kemono-list.conf`,
`bluesky-list.conf` and `tiktok-list.conf`,
overridden by the corresponding `<SERVICE>_LIST_CONFIG` variable, such as
`COOMER_LIST_CONFIG`. A missing list requires an explicit target or list path;
installing a launcher does not create a list or schedule.
Ignored input lines are reported by line number. Empty lists and invalid date
filters fail instead of claiming completed work. Reddit `me` is accepted only
through the saved-post option; use Twitter's ID option for `/i/user/ID` URLs.
Raw gallery-dl flags and `--gallery-dl-bin` are replaced by the reviewed profile.

Set `STASH_INGEST_OUTBOX`, `STASH_INGEST_ENDPOINT`, `STASH_INGEST_PRODUCER`, and
the Stash API token environment reference. `--profile` chooses an explicit
profile; otherwise the launcher selects `STASH_INGEST_<SERVICE>_PROFILE`, where
`<SERVICE>` is `TWITTER`, `REDDIT`, `INSTAGRAM`, `COOMER`, `KEMONO`, `BLUESKY` or
`TIKTOK`. Full-history/top requests select
the corresponding `STASH_INGEST_<SERVICE>_FULL_HISTORY_PROFILE`.
These profiles must have the matching
source category. Website access remains in their local gallery-dl references.

```sh
stash-ingest-reddit --username Example --mode top --call CALL_UUID \
  --profile /persistent/profiles/reddit-full-history.json
stash-ingest --outbox /persistent/producer.sqlite --endpoint STASH_ORIGIN \
  --producer PRODUCER_UUID call-status CALL_UUID
```

`--call` must stay fixed across retries of one execution. Under systemd, omitting
it derives a UUID from the producer, `INVOCATION_ID`, service and mode. Each new
invocation gets new work; interactive calls without an explicit UUID create a new
execution. The response includes the call UUID. Repeating it with the same
options retains the original lists, policy and dates even if input files vanish.

By default exit 0 means **recorded locally**, with JSON `state: recorded`.
Dispatchers/workers execute the call separately. `--strict-errors` also inspects
completion and exits 0 only for `source_succeeded`, 2 for unfinished/review work,
and 1 when inputs, storage or API access fail. It does not wait for completion.
Source success still requires separate file-intake receipts. An existing n8n
completion branch must never treat local recording as completed backfill.

The old relative date options formatted local wall time without an offset,
which gallery-dl interpreted as UTC. This conversion preserves that exact
boundary and freezes it once. Use an explicit timezone in `--date-min` when a
new caller needs an unambiguous cutoff. ISO string bounds retain gallery-dl's
second precision; numeric Unix bounds retain milliseconds. Native windows reject
empty/future lower bounds and sub-millisecond numeric values.

## Inspection and delivery

The CLI takes `--outbox PATH --endpoint ORIGIN --producer UUID`, followed by one
of these commands. `--token-env NAME` changes the environment reference; there
is no command-line token argument.

| Command | Result |
|---|---|
| `status` | Event counts/bytes/age plus source-request and caller backlogs |
| `lookup-collections --target URL [--target URL ...] [--root UUID]` | Current scoped collection candidates; exits 0 only when each URL resolves to one active collection, otherwise 2 |
| `queue-sources --call UUID --targets-file FILE --profile FILE` | Freezes one caller execution and its source list locally; optional absolute or relative time window |
| `resolve-sources` | Resolves up to 50 caller URLs and queues bound tickets; exits 2 while unresolved or reviewed targets remain |
| `calls-status [--call UUID] [--after N]` | Local caller counts and an optional page of retained source bindings |
| `call-status UUID` | Checks all original source tickets; exits 0 only for `source_succeeded`, otherwise 2 |
| `retry-call UUID` | Retries reviewed targets without changing bound sources or the frozen window |
| `drain` | Attempts one ready batch of at most eight events; exits 2 while local event work remains |
| `retry EVENT_UUID` | Requeues one explicitly reviewed event with its original contents |
| `retire-profile EVENT_UUID --sha256 HASH --superseding-revision N --reason TEXT [--apply]` | Previews or records an explicit local retirement of an obsolete, HTTP-409-rejected profile report; retains the original event and does not acknowledge it |
| `receipt-status EVENT_UUID` | Reads actual server ingestion/worker status |
| `queue-run --collection UUID --revision N --profile FILE --until TIME [--retrieval-url URL]` | Records/coalesces a download request using the profile digest; low-level callers may use `--policy SHA256` instead |
| `submit-runs` | Submits one ready request; exits 2 while requests remain pending, in flight or in review |
| `dispatch --profile FILE` | Resolves/delivers/submits queued work and discovers at most one source attempt; retains pagination and backoff |
| `dispatch-all --profiles FILE` | Delivers saved work and rotates across local download and enrichment profiles and permitted collections |
| `runs-status [--intent UUID \| --ticket UUID] [--after N]` | Local request counts and up to 50 relevant historical submissions |
| `ticket-status UUID` | Checks the ticket's original source windows; exits 0 only for `source_succeeded`, otherwise 2 |
| `retry-run-request UUID` | Retries a reviewed submission with the original UUID and bytes |
| `worker-policy --profile FILE` | Validates the local worker profile and prints its portable policy/root identity |
| `execute-run RUN_UUID --profile FILE` | Executes one claimed source attempt; does not assert media intake completion |

`acknowledged` means Stash accepted the event transaction. For a file, that means
verification was queued; it does not assert a successful media import. A disabled
file processor leaves new file events pending while previously committed
receipts remain recoverable. `queue-run` succeeding means the request was recorded
locally; `submit-runs` succeeding means requests were admitted by Stash. Neither
command claims a completed scrape or starts a downloader.

`retire-profile` is for an operator-reviewed collection revision that cannot
accept a retained `account.profile` report. Verify the newer collection definition
before using it; the command is local and does not infer that review from a 409.
It requires the exact original event digest, a newer revision and a review reason.
Only held profile reports qualify: pending/in-flight work, captures, files,
attachment reports and events with dependents are rejected. Preview is the default;
`--apply` commits the immutable decision and repeat application is idempotent.
No receipt is fabricated, and the original bytes, revision, attempt and error
remain unchanged. Status separates `retired` and `retired_bytes` from actionable
review/queued counts. Retained reports still count against storage capacity, and
`retry` cannot reactivate them. No download, source cursor or coverage changes.

This adds producer schema 23 through a small ledger, without copying the events
table. Upgrade archive verification and all writers sharing the outbox before
opening it with the new producer. Older producers reject schema 23. Archive
verification checks the original payload and retirement decision and includes
both in its receipt-boundary digest; retirement is distinct from acceptance.

Reddit subscriptions use one canonical profile URL, not one collection per
new/top/search pass. The n8n source-management flow registers this profile once.
Profile caller inputs expand into internal retrievals; lookup binds each one to
the same collection and returns `retrieval_url`. The producer retains that URL in
the run ticket and completion proof, so completion of one pass cannot complete
another. This is advertised as `profile_source_protocol: 1`.

Collection lookup uses the native API's `collection_lookup` capability and
`POST /collections/lookup`. A request contains up to 50 distinct exact source
URLs and one logical root; omit the root only for unbound metadata collections.
The server applies the token's collection/root grants to current revisions,
then returns candidate UUIDs, revisions and states grouped by URL. Each URL has
at most 128 candidates and a `has_more` flag. Truncation always requires review;
the client validates the flag and allows up to 4 MiB for the entire response.
It does not send collection labels, account associations, directory names or
local mounts.

An absent visible match is `unresolved`; it does not prove the URL is absent
from the whole library. Multiple matches are `ambiguous`, including when one is
inactive. One disabled or retired match retains that state. Callers must review
those cases rather than select a candidate automatically. Historical targets,
changed media roots and different query/sort parameters never silently match.
The Python client rejects inconsistent roots, URLs, duplicate IDs and malformed
revisions before producing a usable binding. Lookup neither creates collections
nor enqueues work. The durable caller queue freezes the original URLs and time
window, then commits selected bindings with their source tickets before native
submission. Installed launcher conversion remains unfinished.

## Retaining the scan journal

`stash-import-scan-journal` prepares a frozen journal database through one
read-only SQLite snapshot. It inventories every table/view, rejects unknown
families or changed columns, retains all recognized scan-history rows and reports
the account-backfill tables handled by `stash-import-backfills` separately.
Preparation is local and prints counts/digests without raw commands or payloads.
The server performs semantic validation when applying the snapshot.

```sh
stash-import-scan-journal --journal /migration/run-journal.sqlite \
  --root ROOT_UUID --source ORIGINAL_DATABASE_UUID --snapshot SNAPSHOT_UUID \
  --captured-at 2026-10-01T12:00:00Z
stash-import-scan-journal --journal /migration/run-journal.sqlite \
  --root ROOT_UUID --source ORIGINAL_DATABASE_UUID --snapshot SNAPSHOT_UUID \
  --captured-at 2026-10-01T12:00:00Z --expected-sha256 PREPARED_INPUT_SHA256 \
  --endpoint https://native-stash.example --apply
```

Use the same database UUID as the account-backfill importer, and a distinct
snapshot UUID and fixed capture time for this backup boundary. Apply requires
an explicit endpoint, the reviewed preparation digest and `STASH_API_KEY`
(or `--api-key-env`). It makes one atomic application API request. Repeat the
same frozen inputs after a lost response; an existing snapshot identity cannot
be repurposed for changed bytes, another root or another source database.
No proxy, redirect, website login or producer token participates in this import.

The native receipt inventories `scan_jobs`, `extractor_jobs`, `scan_deferrals`,
`backfill_scan_completion`, `collection_backfill_completion`,
`backfill_policy_migrations` and `legacy_handoffs`, including empty tables.
Older extractor tables can omit their later checkpoint columns. Valid unknown
historical provenance inside JSON strings is preserved; unsupported command
options must be reviewed in the private source snapshot before importing them.
Limits are 10,000 retained records and 8 MiB per atomic document. Oversized
snapshots fail rather than silently dropping rows or splitting the boundary.

The result is `retained`, with `jobs_activated: 0`. This is an evidence-import
step, not completion of the operational migration. Original retry delays,
deferrals, date minima and cursor positions remain available for the next
binding/activation step described below. Manual/orphan extractor scopes and historical
process handoffs remain review records. Historical collection and hashed per-scan
completions never become native source-window proof. No old command or systemd
unit is executed, and no PID is resumed.

Retained rows and their manifest now belong to Stash's database and its normal
backup boundary. Keep the frozen original and preparation manifest for semantic
reconciliation and rollback. The original journal and active old workers are
untouched by this development importer.

## Activating retained scans

`stash-activate-scan-journal` previews a binding through the application API;
`--apply` requires its reviewed plan digest. Prepare a JSON object with a new
activation `uuid`, the selected `scan_record_uuid`, native `collection_uuid`,
`collection_revision`, `root_revision`, reviewed `policy_sha256`,
`cooldown_seconds` and fixed RFC3339 `cutoff` at millisecond precision. The source
URL and logical root must match exactly. The cutoff must cover the frozen
snapshot. The native profile replaces the old command; verify its destination,
archive and skip policy as part of the source binding.

```sh
stash-activate-scan-journal --binding recovery-binding.json \
  --endpoint https://native-stash.example > recovery-preview.json
stash-activate-scan-journal --binding recovery-preview.json \
  --endpoint https://native-stash.example --expected-sha256 REVIEWED_PLAN_SHA256 --apply
```

Both commands use `STASH_API_KEY` (or `--api-key-env`), with no proxy or redirect.
Keep the returned normalized binding and plan digest for retries. A lost response
is retried with exactly those inputs; activation never resets an existing native
run. The result is one run for all equivalent pending requests at this exact
snapshot/context/URL. Differing old command policies require review. Existing
deferrals and retry delays survive; normal source-run review controls retry or
cancellation. No worker starts merely because the snapshot was retained.

Optionally set `checkpoint_record_uuid` to a retained extractor checkpoint whose
scope covers the consolidated lower bound. The native worker reproduces that
legacy archive-key hash during replay and then records native cursors. A missing
checkpoint leaves the run unfinished. Full-history/no-skip profiles without an
archive stop rule keep their old behavior of ignoring legacy stop checkpoints.
Omit the checkpoint to replay the window without archive stopping. An expanded
window also replays without archive stopping so unfinished older work is covered.
No existing files are downloaded again solely because archive stopping is disabled;
the reviewed profile still controls archive skips and forced downloads.

The final production binding requires the common quiesced migration boundary.
Development rehearsals use isolated roots and do not authorize production worker
activation. Manual/orphan checkpoints, historical handoffs and completion hashes
remain evidence; this command creates no historical lease or completion proof.

## Historical post-to-media links

`stash-backfill-post-media` connects retained post/file appearances to existing
scenes and images even when an old NFO capture has no attachment manifest. It
uses native file-generation, path/content and current ownership checks. It
preserves explicit choices, leaves conflicting matches in review and creates
no attachment order or album. Source-album matching below remains a separate
operation.

```sh
stash-backfill-post-media prepare --endpoint STASH_ORIGIN --output /migration/post-media
stash-backfill-post-media show --plan /migration/post-media --expected-sha256 PLAN_SHA256 --post POST_UUID
stash-backfill-post-media apply --endpoint STASH_ORIGIN --plan /migration/post-media --expected-sha256 PLAN_SHA256
stash-backfill-post-media status --endpoint STASH_ORIGIN --plan /migration/post-media --expected-sha256 PLAN_SHA256
```

Preparation only reads the native API and saves a new private plan in bounded
parts. Use its printed digest after inspecting the proposals. `--posts-file`
can restrict preparation to a JSON array of UUIDs; otherwise the client pages
all retained post-level evidence, up to one million posts. A per-post review
limit remains an explicit item and does not hide other eligible posts. The plan
is bound to its original endpoint and never contains `STASH_API_KEY` or the value
of `--api-key-env`.

Explicit post UUIDs resolve to their current identity during preparation. Several
old IDs for one merged post produce one reviewed entry. An existing plan keeps
its original post/request IDs; a later merge requires a new preview for work
that has not committed, while completed receipts remain recoverable.

Resume Apply with the same plan and digest after a network error or restart.
The client reads durable receipts before submitting missing requests. A later
unlink does not cause an earlier request to recreate the link. Stale previews
require a new plan; completed request identities and history remain intact.
Every part is validated before submission and checked again before use. Changed
plan bytes, unexpected target identities and mismatched receipt counts fail.

Exit codes are 0 for preparation/show or processed work without review, 1 for
input/transport failures, 2 for review items/conflicts, and 3 for unsubmitted
requests. `processed` concerns the saved matching pass; `needs_review` and the
outcome counts retain unresolved candidates. Matching does not apply metadata
policies or enable scrapers. See the [native contract](https://github.com/notsafeforgit/stash/blob/develop/docs/native-ingestion.md#historical-post-to-media-matching).

## Fclones-backed file deduplication

Fclones JSON paths use STFU-8 inside JSON strings. The client decodes that
format once before checking literal paths, containment, symlinks, filesystem,
scope and size. Malformed escapes and unsafe reports still fail before any API
request. Valid local filenames outside the native API's portable path contract
(backslashes, control characters or non-UTF-8 bytes) put their whole duplicate
group into `unsupported_media_path` review. Other groups continue normally;
the client never rewrites names or silently chooses a different survivor.

Discovery reviews retain the original lossless fclones paths in the version-2
run manifest inside `dedupe.sqlite3`. The existing SQLite schema and backup
role are unchanged. Version-1 pending runs, request UUIDs and uncertain receipts
remain recoverable. Summary `pairs`/`review` counts include these groups' redundant
copies, `discovery_review_groups` counts affected groups, and `all_removed` is
false when any need review. Completed review passes use the ordinary daily
cooldown. A held backup lock skips new discovery before starting fclones;
discovery still releases the lock so a later backup can begin.

`stash-dedupe` uses bounded `fclones group` discovery and the native Stash
preview/apply API. It preserves the oldest copy, saves requests before sending
them, and recovers committed receipts after interruption. Read-only discovery
allows downloads and backups to continue; each candidate's preview/recovery/apply
holds the existing backup lock plus every inventoried native worker publication
barrier, releasing them between pairs. Stash
verifies full bytes and preserves provenance. Without an explicit policy,
different media owners remain review cases. See the [client guide](https://github.com/notsafeforgit/stash/blob/develop/docs/native-file-deduplication.md#native-host-client)
for arguments, persistent state and exit codes.
`stash-dedupe-host` supplies the scheduled/pre-backup configuration,
completion cooldown and private key-file handling. Its compact SQLite journal
is inventoried as an operating database for coordinated backup. The installed
host launcher selects this native caller.

Set `media_policy` to `earliest-fill-blanks-v1` only with a supporting native
server. Exact-byte duplicates with separate single-file media records then keep
the earliest record, fill eligible blank dates/descriptions and preserve source
URLs, galleries and explicit clears. Multi-file owners remain review cases.
An explicit `intake_collection_uuid` additionally registers unindexed equal-byte
files through native saved-metadata intake. The native preview must report saved
posts for that exact location; missing evidence stays in review. No website fetch
is involved. At most eight durable registration requests wait at once, and no
pair is removed before both registrations finish. Schedule `--resume-only` for
bounded continuation without another full-library scan. Journal schema 2 adds
registration receipts while preserving schema-1 deletion requests unchanged.

## Historical source albums

`stash-backfill-source-albums` uses the native application API to match imported
media to source post attachments and construct the corresponding galleries.
It runs after the relevant source evidence, attachment manifests and media
associations have been imported. It requires the existing application API key
in `STASH_API_KEY` (or a named `--api-key-env`), not a scoped scraper token.
No website login or cookie is involved, and plans contain no API key.

First prepare a new private plan directory. This fetches read-only previews and
saves the exact post, matching policy, preview signature and request UUID for
every operation. Choose `source-identifiers-v1` for retained qualified source
media IDs. Use `legacy-reddit-filename-v1` only when explicitly accepting the
historical Reddit `post-id_media-id` filename convention as matching evidence.

`legacy-twitter-filename-v1` also recovers missing partial attachment lists from
the archive's known original `<tweet-id>_<number>` filenames. It follows retained
file matches to deduplicated survivors, preserves numbering gaps, and never
infers a complete list or total count. Existing source selections remain intact.
With this policy, `--all-selected` discovers historical filename candidates,
including posts that have no selection yet. Use `--posts-file` for a bounded
explicit JSON array of post UUIDs. No new captures or duplicate payloads are
created. See [recovery semantics](https://github.com/notsafeforgit/stash/blob/develop/docs/native-source-albums.md#recovering-historical-twitter-albums).

```sh
stash-backfill-source-albums prepare --endpoint STASH_ORIGIN \
  --policy legacy-reddit-filename-v1 --all-selected \
  --output /migration/source-albums

stash-backfill-source-albums show --plan /migration/source-albums \
  --expected-sha256 PLAN_SHA256 --post POST_UUID

stash-backfill-source-albums apply --endpoint STASH_ORIGIN \
  --plan /migration/source-albums --expected-sha256 PLAN_SHA256

stash-backfill-source-albums status --endpoint STASH_ORIGIN \
  --plan /migration/source-albums --expected-sha256 PLAN_SHA256
```

`PLAN_SHA256` is the manifest digest printed by `prepare`; retain it with the
reviewed plan. `show` displays one complete saved preview without contacting
Stash. `apply` validates every saved record before making changes. The endpoint
must match the saved origin; redirects and ambient proxies are not used. Existing
plan directories are never overwritten. Instead of `--all-selected`, use repeated
`--post POST_UUID` arguments or `--posts-file FILE` containing a JSON array.
New preparation resolves those explicit UUIDs and includes each current post
once, including when several supplied IDs have merged. Saved plans and retries
keep their original post UUIDs. A merge before gallery publication requires
fresh review; notification retries for published changes keep their original
event and do not repeat the gallery changes.

Discovery includes only posts with selected attachment lists, including disabled
and forgotten posts. Forgotten entries remain in the report without a submission.
Previews requiring review are retained without application. Disabled selections
and single-media posts can complete as no-ops. Bounded UUID pagination is not a
global snapshot while writers run: migration uses a quiesced boundary or an
explicit reviewed post list. Plans allow at most 100,000 posts and 64 MiB per
preview; exceeding a limit fails explicitly rather than dropping records.

After interruption, rerun `apply` or `status` with the same directory and digest.
The client looks up each original request before submitting it, including when a
response was lost. It never refreshes a stale preview or silently replaces a
failed/cancelled job. `publication_committed` means the database changes exist;
`hooks_finished` additionally means the worker finished the applicable plugin
notifications. Gaps without usable media remain visible in publication counts.

Exit 0 means the selected command succeeded; for `apply` and `status`, every
submitted operation in that plan has finished and no review outcome remains.
Exit 2 means review is required (including stale, failed or cancelled work),
exit 3 means queued/running/unsubmitted work remains, and exit 1 means an error
or unavailable response. An error does not prove that a preceding request failed
to commit. Plan completion does not mean all source media was downloaded, all
the full migration is complete.

To cancel, inspect the job revision, then use `cancel` with the plan arguments,
`--post POST_UUID` and `--expected-revision REVISION`. Cancellation cannot undo
already-committed gallery changes or notifications. Explicit retry creates a
separate saved plan before submitting anything:

```sh
stash-backfill-source-albums prepare-retry --endpoint STASH_ORIGIN \
  --plan /migration/source-albums --expected-sha256 PLAN_SHA256 \
  --post POST_UUID --output /migration/source-album-retry

stash-backfill-source-albums apply --endpoint STASH_ORIGIN \
  --plan /migration/source-album-retry --expected-sha256 RETRY_PLAN_SHA256
```

The retry pins its parent job and revision. If that job already published,
retry resumes notification delivery with the original event identity, preserving
later library edits. If unpublished evidence has changed, prepare and review a
fresh plan instead. Old terminal job history remains available.

## Native n8n backfills

`stash-ingest-n8n` replaces the account backfill runner's record/inspect contract.
It supports the existing eight Reddit/Twitter modes and their exact URL order.
New calls require a reviewed matching service profile with global `skip=true`.
Configure `STASH_INGEST_OUTBOX`, `STASH_INGEST_ENDPOINT`, `STASH_INGEST_PRODUCER`
and `STASH_INGEST_TOKEN` in the execution environment. Set
`STASH_INGEST_REDDIT_FULL_HISTORY_PROFILE` and
`STASH_INGEST_TWITTER_FULL_HISTORY_PROFILE`, or pass `--profile` explicitly.

```sh
stash-ingest-n8n --mode reddit-new --identity Example \
  --workflow WORKFLOW_ID --execution EXECUTION_NUMBER --node NODE_UUID --item 0 \
  --profile /persistent/profiles/reddit-full-history.json
stash-ingest-n8n --inspect RESULT_TOKEN
```

The first command records a bounded local snapshot and returns a 32-character
result token. Its UUID derives from the producer, workflow, execution, node,
item, mode and account; retrying the same node cannot create a new caller after
a lost response. `--call UUID` can supply an explicit identity instead. The
first call freezes its profile policy, exact URLs and absolute full-history
cutoff. Replays do not reopen a removed/changed profile or recalculate the window.

Inspection advances that same call; the normal dispatcher also advances queued
backfills. A root-authorized native history lookup must succeed before any
source call is created. Prior completion or deliberate skip produces a retained
result with no source tickets. A `needed` decision and its child source snapshot
commit together. API outages retain the pending check with backoff. Concurrent
inspectors use fenced leases, and no network request holds a SQLite transaction.

Once source work exists, only its original tickets can establish completion.
Later account history cannot turn an unfinished/cancelled call into a success.
After every target's original windows complete, the adapter saves the exact
native proof before sending it. Lost responses replay the same proof and decision
UUID. Native completion and file-intake completion remain separate.

Known-token inspection retains the old `command_failed`, `exit_code`,
`network_blocked`, `stdout_tail` and `stderr_tail` result fields, adding `token`,
`backfill_pending`, the native state, retained decisions and source-call identity.
These fields contain bounded status messages, not downloader logs or settings.
While pending, `exit_code` is 2 and `backfill_pending` is true. Normal command
exit 0 means the JSON result was produced; it does not certify scraping success.
`--strict` exits 2 while pending, 1 for review/failure, and 0 for native completion
or a retained historical acceptance/skip. `backfill_cached` identifies the latter;
`account_backfill_complete` does not claim exhaustive website availability.
Finished results remain available locally after restart, independently of token
rotation or later API outages. `--inspect TOKEN --retry-reviewed` rechecks the
same original work; it does not replace a cancelled ticket with a fresh scrape.
If a source lookup or native job needs review, resolve and retry that underlying
work first using its existing caller/job controls, then recheck this token.

Producer schema 7 stores these calls and receipts in the existing outbox. Its
default limit is 10,000 retained calls; reaching capacity stops admission without
evicting history. Include this outbox in the migration/backup boundary. Stash's
database schema does not change in this adapter increment.

### Retaining old n8n result tokens

`stash-import-n8n-receipts` imports a frozen copy of the old `n8n-receipts`
directory into the native producer outbox. Preflight reads every file, reports
each token/digest/outcome and performs no writes. Apply requires that reviewed
snapshot digest and an explicit outbox binding. Use a stable receipt-source UUID
from the migration manifest, distinct from the run-journal database UUID.

```sh
stash-import-n8n-receipts --receipts /migration/n8n-receipts --source RECEIPT_SOURCE_UUID
stash-import-n8n-receipts --receipts /migration/n8n-receipts --source RECEIPT_SOURCE_UUID \
  --expected-sha256 REVIEWED_INPUT_SHA256 --apply \
  --outbox /persistent/native-outbox.sqlite --endpoint https://stash.example \
  --producer PRODUCER_UUID
```

The entire import commits atomically, including its manifest. Repeat the same
command after an interruption or lost command response. Original JSON bytes,
tokens, input hashes and import provenance are immutable; conflicting bytes,
source identities or native caller tokens roll back the whole import. Unknown
directory entries, nonregular files, changing files and malformed JSON block
import. Valid JSON with unsupported result semantics is retained for review.
The retained limits are 10,000 receipts/64 MiB, plus 1,000 manifests/16 MiB;
capacity failure never evicts earlier results.

`stash-ingest-n8n --inspect OLD_TOKEN` then works locally, even without the old
directory, an API token or network access. It returns `state: legacy_*`,
`backfill_pending: false` and a `legacy_receipt` provenance object. Successful
and deliberately skipped command results retain their old result fields. Failed
results remain failures; a recorded network block sets `command_failed: true`
and a nonzero exit code even if its original child exit code was zero. The exact
original is still retained in `legacy_n8n_receipts.body`. Unsupported results
return a review error, and `--strict` exits 1 for failure/review, 0 otherwise.
Historical receipts cannot be retried as native work. General producer status
lists their counts separately from active work.

Old receipts do not record an account or execution identity. The importer never
guesses those from log text, queues a scrape, or creates native completion proof.
Even an old `account_backfill_complete` flag is only a retained historical claim;
permanent account history uses the separate journal importer below. File intake
remains unverified. Include the outbox and its import manifests in backups.
This preserves token inspection during cutover; saved n8n execution graphs still
need explicit drain/resume handling before their old command paths are removed.

Schema 6 → 7 preserves existing delivery events, source calls, tickets and
backfill results, adding two initially empty receipt/history tables. It does not
read any old receipt directory automatically. Older producer binaries reject
schema 7; restore the pre-migration outbox copy when rehearsing a rollback.

### Staging the existing n8n graphs

`stash-ingest-n8n-config --input workflow-export.json --output staged.json`
accepts a reviewed workflow export array and atomically creates a private file.
It refuses unknown command/connection/result shapes and never overwrites an
existing output. It does not access, publish or activate the n8n database.
Existing workflow IDs, node IDs, credential references, unrelated parameters,
inputs and success/error branches remain. The converted command records its
execution context, and the inspection parser retains the pending flag and token.
Pending results enter a 90-second Wait and inspect the same token again. Other
results continue through the existing error/success handling.

The interval uses n8n's persisted Wait path; waits shorter than 65 seconds keep
the execution in the running process. See the [n8n Wait documentation](https://docs.n8n.io/integrations/builtin/core-nodes/n8n-nodes-base.wait/).
The `tests/n8n_workflow_runtime.cjs` rehearsal evaluates real staged commands and
result expressions with the installed n8n evaluator, verifies retained IDs and
credential references, and calls the actual Wait implementation with an isolated
checkpoint context. It runs no scraper commands and needs no network.

Before activation, preserve/import the old account decisions, per-scan state and
result receipts, finish or reconcile old executions, register the exact source
collections/root grants, and provision the profiles, outbox and dispatcher.
Compare both current and published graphs at cutover; the staged converter must
not overwrite later workflow edits. Old result-file tokens are a separate input
format, not native outbox tokens. The installed workflows remain on the old
runner until that conversion and deployment boundary is complete.

The converter also recognizes the reviewed parent-execution heartbeat command
and relocates it to `stash-n8n-parent-alive` in the isolated worker environment.
Its existing Redis coordination, connections and input expressions are preserved.
The helper opens the local n8n SQLite database read-only; `new`, `running` and
`waiting` executions remain alive until stopped. An unavailable database fails
the command instead of claiming the parent finished. `N8N_DATABASE_PATH` or
`--database` supplies a relocated database. This helper neither reads a creator
catalog nor establishes scrape completion.

## Registering and disabling native sources

`stash-manage-sources` prepares and resumes bounded source-definition changes
through the existing native API. It requires the application's `STASH_API_KEY`
for administration and the producer's `STASH_INGEST_TOKEN` with a grant for the
whole selected logical root. A collection-limited token cannot prove a URL is
absent outside its own grants. Website login credentials are not involved.

An input contains `operation` (`ensure` or `disable`), `root_uuid`, `reason` and
one to fifty complete `targets` definitions. For example:

```json
{
  "operation": "ensure",
  "root_uuid": "00000000-0000-4000-8000-000000000001",
  "reason": "Add the requested scrape source",
  "targets": [{
    "label": "Example Reddit source",
    "kind": "account",
    "namespace": "native:reddit",
    "state": "active",
    "target_url": "https://www.reddit.com/user/example/submitted/?sort=new",
    "root_uuid": "00000000-0000-4000-8000-000000000001",
    "account_uuid": null,
    "path_prefix": "."
  }]
}
```

Use the real root UUID and exact URLs expanded by the launcher, including each
Reddit traversal. Preparation uses indexed current-target lookups and records
existing identities or new UUIDs in a private immutable plan. It performs no
server mutation. Existing collections keep their labels, accounts and path
scopes. Missing sources can be registered; ambiguous, disabled or retired
sources require review before `ensure` can proceed. `disable` changes only the
state of matched active collections, preserves their metadata and evidence, and
does not create missing collections or delete media.

```sh
stash-manage-sources prepare --input sources.json --plan source-plan.json --locks /shared/native-locks
stash-manage-sources apply --plan source-plan.json --expected-sha256 PLAN_DIGEST --locks /shared/native-locks
stash-manage-sources status --plan source-plan.json --expected-sha256 PLAN_DIGEST
```

Configure `STASH_INGEST_ENDPOINT` and `STASH_INGEST_PRODUCER`, or pass the
corresponding `--endpoint` and `--producer` arguments. Save the digest returned
by preparation; retries reuse that plan rather than preparing new identities.
Commands return 2 for changed targets needing review, 3 for unapplied work and
1 for invalid input or unavailable service/storage. A successful apply confirms
source definitions, not a completed scrape or media intake. A lost response is
recovered from the original definition revision; later owner edits are retained
and prevent the old plan from becoming ready again.

Apply preflights all selected targets. Concurrent changes can still leave a
partial batch, which the retained plan resumes safely. The shared producer lock
directory coordinates host/container mutations and backup capture. Include the
input/plan directory in the worker's backup inventory before activation.
Registration does not infer account ownership, create performers, set depicted
performers or install metadata policies. Those choices use the native account
review and policy APIs. The actual parent/add/remove n8n graphs still require
that integration and runtime verification before deployment.

### Native n8n source subscriptions

`stash-n8n-sources` connects the reviewed parent/removal workflows to native
source definitions, metadata policies and the lists used by scheduled host
launchers. Set `STASH_SOURCE_MANAGEMENT_CONFIG` to a private JSON runtime file
with these fields:

- `version`: `1`.
- `root_uuid`: the native media root covered by the producer's complete root grant.
- `locks`: the existing shared native worker lock directory.
- `state`: an existing private directory for durable source-operation plans and receipts.
- `lists`: separate canonical absolute filenames keyed by `reddit` and `twitter`.
- `new_source_policy`: a complete reviewed native metadata policy definition,
  with `enabled: true`, `apply_to_scans: false` and explicit scene/image rules.
  Source mappings, completeness requirements and filename fallback use the same
  schema as the collection policy editor. Policies are captured in each request;
  changing this runtime default does not reinterpret an interrupted operation.

The command also uses `STASH_INGEST_ENDPOINT`, `STASH_INGEST_PRODUCER`,
`STASH_INGEST_TOKEN` and the separate application `STASH_API_KEY`. Credentials
are environment references; workflow command arguments contain no keys.

```sh
stash-n8n-sources --action add --platform reddit --identity Example \
  --workflow WORKFLOW_ID --execution 123 --node NODE_UUID --item 0
stash-n8n-sources --action remove --platform twitter --identity 123456789 \
  --workflow WORKFLOW_ID --execution 124 --node NODE_UUID --item 0
stash-n8n-sources --action remove-performer --platform reddit --identity 40497b95-1304-469c-8d29-79649e655a77 \
  --workflow WORKFLOW_ID --execution 125 --node NODE_UUID --item 0
```

`remove-performer` takes the Stash performer's UUID, available in its page URL
or GraphQL `id`. Site account handles and IDs keep their service-specific format.

The stable workflow/execution/node/item tuple identifies one operation. Changed
arguments conflict with its saved request. Registration initializes metadata
policies only on newly created sources; existing policies and collection scopes
stay selected. Missing or disabled policies on an existing source require review.
An ambiguous account identifier also requires review before any mutation.
Performer removal resolves current native account ownership, follows canonical
identities and pages all identifiers. Names and aliases cannot authorize removal.
Sources, media, accounts and metadata history are retained when tracking stops.

The operation saves complete source/policy plans and before/after list bytes
before changing the server. Retries recover original revisions and never rewrite
a later list or policy edit. Completed receipts return the original result after
later changes. List comments, unrelated accounts, communities and line endings
are retained. Exit 0 means this source operation completed; exit 2 requires
review, and exit 1 reports unavailable/invalid input. It does not certify a
scrape: the parent must still await its native backfill children and their durable
completion checks. Tracking starts once registration completes, allowing scheduled
runs and the initial backfill to share native source-window coordination.

The converted parents use native ingestion and metadata policies in place of
their old scan/autotag/name-based performer creation sequence. Adding a source
does not assert that its publisher is depicted in every file. Account ownership
and depicted-performer defaults remain explicit native choices.

Capture the runtime file, both lists and the entire operation-state directory
under the same shared publication barrier as producer outboxes. These are
required backup components before activation. Freeze/drain old n8n executions
before replacing their graphs or retiring legacy helper paths. Staging workflow
exports or testing this command does not install a live runtime or activate jobs.

## Backfill journal import

`stash-import-backfills` migrates permanent account completion/skip decisions
through the native application API. It handles `backfill_completion` and
`legacy_backfill_skip`; per-scan completions and the remaining journal families still
require the broader migration. This is a maintenance command,
separate from normal worker ingestion and the staged native n8n adapter.

```sh
stash-import-backfills --journal /migration/run-journal-snapshot.sqlite \
  --root ROOT_UUID --source INPUT_DATABASE_UUID
# After reviewing the preflight, target the intended native database instance:
stash-import-backfills --journal /migration/run-journal-snapshot.sqlite \
  --root ROOT_UUID --source INPUT_DATABASE_UUID \
  --endpoint STASH_ORIGIN --apply
```

The first form only validates/counts records and needs no API key or connection.
Use a consistent journal snapshot from the migration backup boundary. The source
database UUID belongs in that migration's manifest and must stay fixed across
retries and relocated copies; the root must already exist in the native database.
The importer opens SQLite read-only, rejects unknown columns/unsupported row
shapes, and validates all records before its first network write. Both passes
use the same SQLite read transaction, so concurrent changes cannot alter a batch
halfway through import.

`--apply` reads the existing Stash **application** API key from `STASH_API_KEY`
(or `--api-key-env NAME`) and sends it only in the `ApiKey` header to the selected
origin. Producer tokens cannot import historical acceptances. Redirects, website
cookies and proxy inheritance are disabled. Batches contain at most 50 records
and 4 MiB; acknowledgements must match the stable decision UUID, component,
outcome and historical basis for every record.

Rerun the same command after interruption or a lost response. Already committed
records replay unchanged; a different payload under an imported primary key is
a conflict, not an overwrite. Errors report confirmed acknowledgement counts
without printing record bodies, historical logs or keys. A lost response can
leave more rows committed than that count indicates, which replay resolves.

The native database retains the original `result_json` string and all provenance.
An old user acceptance remains an acceptance even when it states that exhaustive
history was unverified. Imported skips remain distinguishable from completion;
neither kind creates native source-run coverage or successful media receipts.

## Instagram downloads and source albums

The pinned adapter adds `instagram_media` version 1 before gallery-dl filters
unavailable media or reverses its output order. It contains the enclosing post ID,
an explicit album flag and ordered media IDs/kinds; null entries retain missing
source slots. Per-file `media_id` must match that evidence. Carousels can create a
source gallery as files arrive; one ordinary image/reel does not become an album.

Stories and highlights are containers of individual posts. The adapter preserves
the container ID/type as evidence but keys each post by its original media ID and
filters on that item's publication time. Finding the same story in a highlight
therefore does not create another post. A profile's posts, photos, reels, tagged,
stories and highlights children each apply the original run window. Container
timestamps, download order and output numbering cannot supply post identity/order.

Extra audio, previews and highlight covers must be disabled for native download
intake. Music stickers still retain their metadata while their disabled audio
outputs are ignored. Gallery-dl owns static-video detection and output selection. The adapter
retains the original attachment IDs, order and missing slots, then records the
media kinds selected by gallery-dl; it does not reimplement upstream selection
rules. Offline compatibility checks protect this Stash contract and resumable
pagination, while upstream token and query-parameter discovery remain upstream's
responsibility. New captures share post fields and keep per-file dates, sizes, URLs and attribution in patches;
older captures without the versioned evidence retain their original partition
for migration, proof verification and replay.

The installed `stash-ingest-instagram` command records work for a separate native
dispatcher. An exit code of zero does not confirm that a timer has finished
scraping. The host timer, durable outbox, dispatcher/recovery schedule and API
registration must be converted together at reviewed cutover.

## Kemono/Coomer downloads and source albums

The pinned user, post and posts-listing extractors require `original=true`.
The adapter records `mirror_media` version 1 before configured file selection,
download ordering or the extractor's mutation of file dictionaries. Its qualified
post identity includes the mirror, underlying service, account ID and post ID;
mirror IDs do not establish an association with a native OnlyFans/Patreon account.

Membership preserves original attachment-array order, including unavailable slots
and repeated positions. A primary file precedes that list unless it aliases an
existing attachment. Inline source files follow in text order with existing paths
deduplicated. Selecting only attachments or changing download order cannot shrink
or reorder this source evidence. A singleton primary-file alias does not create an
album; multiple evidenced source slots can create a gallery as visual files arrive.

The backend revalidates the manifest against the retained original file,
attachment and inline-content fields. Full source paths identify attachments;
their hash-like filenames are not verified local content checksums. New captures
share the post body while retaining selected-file paths, URLs, types and exclusions
in per-capture patches. Unmarked historical payloads keep their original partition.

Audio, archives and other unsupported file extensions retain source captures with
`native_file_exclusion: "unsupported_image_or_video_extension"` and an excluded
download report. They produce no downloaded bytes, file receipt or download-archive acknowledgement. Supported GIF conversion
still uses the reviewed host processor. Scan windows use the original `published`
timestamp, preserving fractional precision; missing publication time fails rather
than substituting the mirror's import timestamp.

`stash-ingest-coomer` and `stash-ingest-kemono` record durable source requests for
the separate dispatcher. Their reviewed profiles retain existing filenames,
archive/skip rules and helper bindings. Native root/producer registration, outbox,
dispatcher/recovery and actual host/n8n caller activation remain deployment work.
Discord, favorites and artist-dispatch containers are explicitly unsupported by
this adapter and require their own identity/traversal contracts.

## Bluesky/TikTok downloads and source albums

`bluesky_media` and `tiktok_media` version 1 retain the qualified post reference
and original ordered media entries before selection. Producer and backend derive
and validate membership against the same original source fields. Null entries
retain unavailable positions, and repeated source positions remain distinct.
One source item is attributable without becoming a gallery; multiple evidenced
slots can create an album before every image/video is available.

Bluesky posts use author DID plus record key, while attachments use original blob
CIDs. Image alt text remains per-file metadata. Embedded mixed media and quoted
posts retain their own membership; a quoted post has its own identity and source
date. Disabling videos does not remove them from the original attachment list.
Windows use full `createdAt` precision instead of the extractor's truncated date.

TikTok photo keys derive from the original CDN filename before rendition suffixes;
changing a URL signature or CDN host does not create another attachment. Video
keys are scoped to the source post. Unavailable photos are omitted from download
iteration while their original list and source numbering remain in captures.
Photo numbers and the chosen image cannot replace the post ID. Windows use the
original `createTime`. Extractor errors remain failed work, including errors the
pinned upstream loop catches internally.

TikTok requires `audio`, `covers` and `subtitles` disabled for native media intake.
The original music, cover and subtitle references remain source metadata. Native
profile dispatch defaults to posts; explicit includes may select posts, reposts,
stories, likes and saved posts. An explicit unsupported include fails before
source access. Bluesky supports media/posts/replies/video/likes profile routes and
post-producing feed/list/search/hashtag/bookmark targets. TikTok shortlinks route
only to the supported post extractor. Child routes enforce the original window.
Standalone avatar/banner/info targets and following traversal require separate
account/artwork adapters, and are not silently treated as posts.

Only versioned new captures opt into shared post-body partitioning. Per-file
descriptions, dimensions, selected images and generated titles remain patches;
historical capture partitions stay replayable. The packaged
`stash-ingest-bluesky --url ...` and `stash-ingest-tiktok --url ...` launchers
record durable work for the dispatcher. Native registration, reviewed effective
profiles and observed file receipts are still required before replacing callers.

## Metadata-only extraction for enrichment

`stash_ingest.metadata_fetch.fetch(url, settings, resume=None, timeout=180,
check=...)` runs one metadata lookup in an isolated instance of the pinned
gallery-dl runtime. This is the extraction component for the native enrichment
worker. Native target/job binding, producer-owned checkpoint storage, verified
capture publication, scoped HTTP routes and a transport/lease client are
implemented. The selected-job executor below persists returned metadata before
delivery. Scoped producer dispatch and shared source scheduling are implemented
below; conversion of the existing scheduled service is still pending. Calling this helper alone
does not create a capture, complete a native job or import media.

The initial root extractors are Reddit submissions, Twitter tweets, Bluesky and
TikTok posts, Instagram posts/reels, Kemono/Coomer posts, Patreon posts and Fansly
posts. Kemono/Coomer retain their mirror category and service-specific metadata;
a mirror's `onlyfans` label does not identify a native OnlyFans account. Admission
checks the extractor class, since gallery-dl replaces the Kemono/Coomer instance
subcategory with the service name for both posts and whole creator feeds. Profile
URLs and unsupported root extractors return explicit failures. Linked Redgifs
images and Imgur images/albums/galleries can be resolved to two child levels.
Other links remain unresolved references without expanding into another feed.

The Python producer and Go server share post-identity fixtures for these root
services. Bluesky uses a DID plus record key; Kemono/Coomer use mirror, service,
account and post ID together. Instagram regular posts use the enclosing post ID,
never a selected carousel image ID. Story downloads use each original story's
media ID, retaining the story/highlight container separately. Linked media hosts inherit their
enclosing post; a social account/feed parent does not replace its post. These
adapters allow verified metadata capture/publication, while file selection and
source-window adapters for services beyond Reddit, Twitter, Instagram, Coomer,
Kemono, Bluesky and TikTok remain unfinished.

Metadata projection preserves source post captions and dates rather than image
alt text or per-file dates. Kemono/Coomer only use `published` for publication
time because their transformed `date` may instead be the mirror's import time.
Offline fixtures run the pinned extractors' real transformations as well as the
shared Go/Python contract, including multi-image posts and exact large IDs.

Only source access and pacing settings survive configuration projection. There
are no download jobs, file paths, archive updates, user filters, custom actions
or postprocessors. Cookies can be read but are not updated, the extractor cache
is in memory, and originals are selected for Kemono/Coomer metadata. The child
process disables downloader construction and stops at the first HTTP 429. The
caller-supplied `check` callback can cancel a running lookup; the parent kills
the process group on cancellation, timeout or oversized output. Website access
values enter through a private pipe and are excluded from results and logs.

Successful extraction returns a `stash-metadata-fetch-v1` checkpoint containing
the exact requested URL, runtime/retention versions, compact records, pending
children and unresolved references. Each record retains its original observation
time. Records share unchanged post fields through backward base references;
patches and explicit removals reconstruct each retained metadata object. Child
records refer to their parent context, preserving Reddit attribution without
copying the whole parent onto every attachment. These references only compact
the producer transcript; they do not create native post/performer associations.

`metadata_bundle.Bundle(url, extractor_version, checkpoint).metadata(index,
with_parent=True)` reconstructs a record for subsequent identity validation.
The same source retention policy used by the download adapter reduces Reddit
previews and incidental profile fields before checkpoint serialization. Input
metadata stays unchanged. Checkpoints are bounded to 32 MiB, 1,024 records and
256 entries in each child-reference list. Each reconstructed record, including
parent context, must fit the 4 MiB source payload limit, and all reconstructed
records together must fit 128 MiB. These limits also account for native JSON's
Unicode separator escaping. Invalid references, duplicate records, invalid
observation times, policy/request changes, oversized results and unretained data
are rejected before a record is appended.

The server's `archive.ParseEnrichmentTranscript` independently validates the
same compact representation and reconstructs records without losing large
identifiers or original observation times. Checkpoint and subprocess decoding
retain original number tokens, including decimal precision, exponent spelling
and negative zero, so resuming cannot silently rewrite earlier record hashes.
`Extends` checks that a resumed
checkpoint retains all earlier records and unresolved references. A pending
child must remain explicit or have newly returned records for that URL and
parent; retry cannot silently erase it. A shared Go/Python fixture verifies
these semantics. The native coordinator separately checks immutable job bindings,
producer-owned attempts and current source eligibility. Its checkpoint store
keeps one current body plus small acknowledgements and original per-record
producer provenance across resumed attempts. Publication separately consumes the
saved revision/digest, verifies every source identity against the existing target
post and commits native captures, target completion and job outcome together.
Only supported post adapters can publish; retained unresolved references remain
explicit limitations. Equal observations within a job can share captures without
losing their record associations. Successful publication now releases verified
staging atomically while preserving native captures, original acknowledgements,
producer attribution and unresolved references. A release receipt distinguishes
completed cleanup from a job without a checkpoint. Older publications retain
staging through migration until verified cleanup. The scoped producer API accepts
these checkpoints through a separate enrichment contract. Shared native source
reservations and bounded download preference now coordinate the worker with
downloads. Dispatch across local profiles is described below; legacy scheduling
import and service conversion remain required before activation.

Temporary child failures retain the parent and discard that child's partial
records. Persist the entire returned checkpoint before retrying its pending
children; the resumed fetch skips the parent request and retains its observation
times. Missing children remain explicit source limitations. A root error or
empty result cannot appear successful. The caller must distinguish a retained
checkpoint with pending children from a finished lookup, and must still verify
the result against the intended existing post before native publication. The
helper neither applies field mappings nor mutates selected scene/image metadata.

### Enrichment transport and ownership

`enrichment_client.EnrichmentClient(Client(...))` exposes capability checks,
permitted collection discovery, unadmitted target discovery, revision-pinned admission,
job/target inspection, claim/renew, source reservations, checkpoint save/read, verified publication
and controlled failure acknowledgements. See the
[HTTP contract](https://github.com/notsafeforgit/stash/blob/develop/docs/native-ingestion.md#producer-enrichment-api).
Checkpoint bodies remain JSON objects. Requests bind the owner/fence; server
credentials supply the producer identity. Checkpoint reads are bounded to
32 MiB plus 4 KiB and verify the body digest, counts, URL and extractor version.
An unchanged checkpoint may replay an older receipt without changing provenance.

`job_lease.JobLease.claim(client, job, owner=..., seconds=180)`
returns an owned lease or `None` when the selected job is currently unavailable.
`start()` starts heartbeats; pass `check` to the isolated extractor and call
`close()` when the attempt ends. Deadlines use the server date and monotonic
request start, with a safety margin. Failed renewal stops further source work.
Retain an owner UUID across a lost claim response to recover the same attempt.

Pass `reserve_source=lease.reserve_source` to `metadata_fetch.fetch`. The executor
does this automatically and requires native `enrichment_source_pacing_protocol: 1`.
Its bounded subprocess protocol requests a native reservation before initializing
each root or linked extractor. A busy child becomes a pending `source_busy`
reference with the parent transcript intact; no child request is made. Resuming
fetches only pending children, whose services are reserved by the new native claim.
Reservations end with their fenced attempt, including cancellation or expiry.
Website access values stay in the worker and never enter scheduling records.

Only publication certifies success. Failure calls accept controlled codes;
temporary errors receive server backoff and the eighth actual failure becomes terminal.
Expired leases retain progress without consuming that failure allowance.
Exact failure replay cannot affect a newer attempt. Checkpoint/publication
acknowledgements remain recoverable after staging release. Transport tests lose
committed responses, exercise large Unicode checkpoints, and preserve original
number tokens against the real native HTTP server and SQLite.

Use the executor below to persist fresh extraction results before network
delivery. Ready-target discovery lists unadmitted targets; the separate ready-job
route finds admitted retries. The native server now maintains expired/stale
enrichment jobs independently of other workers. Shared service cooldowns and
download/enrichment exclusion and typed download service failures are implemented.
Legacy queue mapping and production host/n8n launchers remain transition work.

### Durable selected-job execution

The current client requires native `enrichment_protocol: 2`.

Saved outbox deliveries take priority over extraction after restart. Admission
does not write a fake initial checkpoint.

`enrichment_worker.execute(outbox, client, profile, job_uuid)` executes or recovers
one admitted native enrichment job. `profile=None` only recovers persisted
delivery; it never claims an attempt or contacts a source website. The CLI accepts
the usual `--outbox`, `--endpoint`, `--producer` and `--token-env` arguments:

| Command | Effect |
| --- | --- |
| `enrichment-policy --profile PATH` | Validate a metadata-only profile and print its portable policy hash and pinned extractor version |
| `execute-enrichment JOB_UUID --profile PATH` | Recover saved deliveries, then claim/fetch only if the native job and reviewed profile still match |
| `deliver-enrichment JOB_UUID` | Recover saved checkpoint/publication/failure intents without loading website credentials or a profile |
| `enrichment-status [--job JOB_UUID]` | Inspect local phases, pending operation, retained bytes and publication proof |

Execution/delivery exit 0 only for a confirmed completed job, 2 for pending,
retry, capacity, review or failed outcomes, and 1 for invalid input or an
exception. A completed metadata lookup does not mean media has downloaded.
Native source evidence and target completion remain separate from selected
scene/image metadata.

Metadata profiles use `stash-gallery-enrichment-v1` and require no media root,
destination lock directory or download archive:

```json
{
  "schema": "stash-gallery-enrichment-v1",
  "source_category": "reddit",
  "gallery": {
    "extractor": {
      "sleep-request": 5,
      "reddit": {"cookies": "${stash:reddit_cookies}"}
    }
  },
  "bindings": {
    "reddit_cookies": {"kind": "private", "env": "REDDIT_COOKIES_FILE"}
  }
}
```

Private references and reviewed local assets follow the download profile
contract. Access values stay local and do not enter the policy digest, native
job or execution journal. The digest binds the projected access/pacing template,
source category, reviewed assets and pinned runtime. Changes to ignored filename,
archive or postprocessor settings do not change metadata policy. Website
credential rotation also preserves that identity. Feed/profile URLs are rejected
before a claim; the category must identify the job's supported post extractor.

Producer schema 8 stores stable claim requests, lease acknowledgements, exact
returned checkpoint bytes and pending delivery intents in the existing outbox.
A nonblocking process lock allows one enrichment executor per outbox; ordinary
download event delivery uses independent short database transactions. A process
crash releases the lock without discarding the journal. Before fetching, the
executor reserves the full 32 MiB checkpoint allowance against the shared outbox
byte limit. Other events cannot consume that reservation. Failure to reserve
records a retryable native attempt without contacting the website.

Returned checkpoints are committed locally before checking for a late ownership
failure. A matching native acknowledgement releases those bytes and atomically
records the next publication or child-failure intent. Lost replies therefore
replay the original requests after restart. Partial child lookups receive native
backoff and resume the accepted head without refetching the original parent.
If an expired attempt's undelivered body conflicts with a successor checkpoint,
it remains in local review with its original bytes; it is never rewritten to fit
the successor. Rejected evidence also stays inspectable. Review resolution is
still transition work.

The journal permits at most 10,000 active/review jobs by default. Registration
prunes finished convenience records beyond the newest 1,000; native publication
receipts remain authoritative. Those limits bound local execution history, not
total SQLite file size. Include the outbox in backups: an unacknowledged body
can exist only there. Schema 7 → 8 preserves all prior tables and receipt bytes;
older producer binaries refuse schema 8. The native Stash schema is unchanged.

The selected-job executor requires a known job UUID. The dispatcher below adds
admission. Shared native pacing is described above; replacement of the scheduled
enrichment service remains transition work.

### Enrichment dispatch and native maintenance

```sh
stash-ingest --outbox /persistent/producer.sqlite --endpoint STASH_ORIGIN \
  --producer PRODUCER_UUID dispatch-enrichment --collection COLLECTION_UUID \
  --profile /private/reddit-metadata.json
```

Each invocation makes one bounded dispatch step for the explicitly selected
collection. It first replays pending local delivery, including evidence produced
with an older profile, then resumes compatible local attempts. It discovers
eligible already-admitted jobs before admitting a fresh target. A lost admission
reply therefore remains recoverable after restart even if no local job record
was created. Admission and claim still validate the exact target/source revisions
and producer grants. No producer is allowed to release a held target.

Omit `--profile` to process only saved delivery for that collection, without
loading website credentials or claiming a new attempt. If expired delivery needs
new ownership, the result is `ownership_required`. With a matching profile,
dispatch can reclaim the job and deliver its original bytes without refetching.
A different profile produces `profile_required`, preserving the local body.
Rejected or divergent evidence remains in review.

Producer schema 9 stores separate delivery, local-job, native-job and target
cursors, plus discovery failure/idle delays. Pages contain at most 20 candidates;
target traversal retains priority, deadline and UUID order, including native
nanosecond precision. Unsupported URLs do not stall every subsequent page.
Selection is persisted before network work, and competing cursor updates cannot
execute a stale selection. Native ownership remains authoritative. Cursors wrap
to revisit temporarily unavailable jobs; they never certify completion.

Native/API failures back off from five seconds, honoring a longer `Retry-After`
for failed discovery/admission. An idle pass waits 30 seconds before rediscovery.
The local queue retains unacknowledged bodies throughout backoff. Exit 0 means
one job completed or no eligible work was found in that pass; `idle` is not a
completed backfill or evidence that the whole collection has been enriched.
Waiting, backoff, review, unavailable and profile/ownership requirements exit 2.
Native publication receipts continue to prove individual job completion.

The application runs trusted enrichment maintenance every 30 seconds while its
HTTP server is active. It uses the partial active-job index, bounded to the 64
permitted active enrichment jobs. It cancels work whose source/target changed
and recovers expired ownership with native source pacing/failure limits. It leaves
checkpoints, publication receipts and source evidence intact, never performs
website requests, and stops with the server before database shutdown. Producer
discovery itself is read-only and does not recover jobs or confer a lease.

Schema 8 → 9 preserves existing events, requests, receipts and staged enrichment
bytes, adding only discovery state. A table-name collision rolls back promotion.
Older producer binaries refuse schema 9. Include the outbox in backups. Blocked
ready-job claims cause a later retry pass, rather than repeated fresh admissions.
Actual blocked claims register 90 seconds of interest; polling refreshes it.
After four download starts or two minutes of live waiting, an eligible
enrichment requester that is actively polling gets its due turn after current
downloads drain. An older waiter's hint cannot reserve the service while its
worker is visiting other collections. Abandoned requests expire, and a metadata
start restores download preference. Held/stale
targets and cooling dependencies cannot reserve unrelated services. This server
policy coordinates collections even when different workers submit claims.
Historical metadata gets up to 60 seconds per claimed attempt before queued
downloads regain priority. Heartbeats cannot extend that turn; shared request
spacing and provider cooldowns still apply. Without queued downloads, metadata
may continue using idle capacity.
Legacy enrichment queue mapping, review resolution and production launcher
conversion still precede service activation.

### Dispatch across local profiles

`dispatch-all` rotates across reviewed download and enrichment profiles without
requiring callers to enumerate every collection. Keep a stable UUID for the local
worker list and stable entry IDs across restarts:

```json
{
  "schema": "stash-gallery-dispatch-v1",
  "uuid": "3425f421-119e-41dd-86cb-b2f71337afca",
  "profiles": [
    {"id": "reddit-download", "operation": "download", "profile": "reddit-download.json"},
    {"id": "reddit-metadata", "operation": "post.enrich", "profile": "reddit-metadata.json"},
    {"id": "twitter-metadata", "operation": "post.enrich", "profile": "twitter-metadata.json"}
  ]
}
```

```sh
stash-ingest --outbox /persistent/producer.sqlite --endpoint STASH_ORIGIN \
  --producer PRODUCER_UUID dispatch-all --profiles /private/worker-profiles.json
```

The list accepts 1–32 distinct entries. Relative profile paths resolve beside
the list. Each entry's `operation` is `download` or `post.enrich`, and it uses
that operation's profile contract; website access stays local, and the list does
not confer native source permissions or enable unsupported extractors.

A `post.enrich` entry may set `"background": true`. Its claims never wait for a
metadata turn: Stash grants a free turn when the service is idle, or a background
attempt beside a download already running there, whose requests yield to
downloads (see Stash's background metadata contract; the server must report
`enrichment_background_protocol: 1`). Use it only in a separate metadata-only
worker, because a worker that also downloads would spend its own download time
on it. An outbox executes one metadata job at a time, so choose an outbox whose
other workers do not run metadata, or they will wait for the background job.

One invocation delivers an ordinary event batch, advances a backfill call,
resolves a caller page and submits one source request. It then tries one saved
enrichment delivery before loading any website profile, and rotates through the
list until one profile performs work or all profiles have been checked. Saved
metadata can therefore finish delivery even if its original website profile was
removed or its access binding is unavailable. Native historical scope and
ownership checks still apply; conflicting evidence remains retained for review.

Each process executes one profile at a time because gallery-dl's configuration is
process-global. To run different websites concurrently, generate separate dispatch
lists with independent rotation identities:

```sh
python3 scripts/split_worker_profiles.py /private/worker-profiles.json
```

Run separate service instances with the generated `worker-dispatch-reddit.json`,
`worker-dispatch-twitter.json`, etc. By default each list includes that site's
download, full-history and metadata profiles. They share the original producer UUID, outbox,
reviewed profile files and physical filesystem lock directory. Do not copy or
replace the outbox. Generic download profiles use the `manual` list. Host units
can use `stash-ingest-worker@host-reddit.timer` with `host-reddit.env`, and an
equivalent instance for each other site. Container workers likewise need separate
processes and distinct container names selecting their site's dispatch list.
Include the generated lists, unit definitions and environment files in backups.

For concurrent incremental and initial scrapes, give the host and n8n separate
producer identities and dispatch lists. Select the full-history entry IDs
explicitly; an unbounded publication window does not distinguish an initial
scrape from an incremental Twitter traversal that stops at archived files:

```sh
python3 scripts/split_worker_profiles.py /host/profiles/worker-dispatch.json \
  --lane incremental --initial-profile host-reddit-full-history --initial-profile host-twitter-full-history
python3 scripts/split_worker_profiles.py /n8n/profiles/worker-dispatch.json \
  --lane initial --initial-profile n8n-reddit-full-history --initial-profile n8n-twitter-full-history
```

These commands replace the generated per-site lists, preserving profile files,
policies, outboxes and queued requests. The initial list contains full-history
downloads only; the incremental list retains ordinary downloads. Both retain
their metadata handlers so admitted jobs and pending deliveries keep draining.
Run one instance per site in each producer. This keeps initial n8n scrapes
serial per site while allowing a different profile's incremental scrape in
parallel. n8n's existing queue still controls profile submission and completion.

Schema 1000110 permits sharing a media root. Overlapping destinations serialize
within each producer/service; independent producers and sites can share the
root. The same source/target remains fenced across all producers, and shared
per-output locks protect actual filenames, including GIF/MKV siblings. A busy
file lock produces a retry without exhausting source failures. Backup barriers
still wait for all active filesystem mutations. Site cooldowns and metadata
fairness remain shared by both queues; parallel workers do not bypass them.

Download entries use existing scoped run discovery. Enrichment entries require
`enrichment_collections_protocol: 1`: Stash lists permitted active collections
with eligible unadmitted targets or due jobs for that runtime/policy. Current
collection/root grants apply before pagination. A root grant includes subsequent
collections at that root; an unbound collection grant stays specific to that
collection. The worker follows pages of at most 20 collections and uses the
existing collection dispatcher for admission and execution. Unsupported URLs
are skipped during target traversal; the server still validates every claim.

Producer schema 15 stores profile rotation, independent saved-delivery cursors
for discovery, enrichment and candidate details, and each operation's per-policy collection
cursors/backoff. Selection commits before execution, so a restart or a
continually busy first profile/collection does not reset traversal.
Revision checks reject competing stale selections. Idle or blocked entries give
peers a turn; an unreadable profile reports `worker_profile_unavailable` without
printing its path, exception or access values. Metadata discovery honors native
retry delays and retains its own failure/idle backoff. Native service fairness
and leases remain authoritative across different workers.

Metadata entries are polled before download entries so busy download profiles
cannot prevent them from expressing scheduling interest. A blocked metadata
claim falls through to downloads in the same pass; Stash decides when the next
bounded metadata turn is due. If admitting a fresh metadata target reports
`queue_full`, collection traversal continues through its bounded 20-collection
page to find already queued jobs. This neither raises the queue limit nor clears
the per-collection admission retry deadline.

Exit 0 means a successful or idle dispatch cycle with no reported local pending
or review work. Exit 2 means work, backoff or review remains, or an inspected
profile was unavailable. Neither `idle` nor exit 0 certifies that the library or
a backfill is complete: callers must check their native receipts and source
window completion. The dispatcher performs a bounded cycle; host/n8n scheduling
and legacy queue/cooldown/history conversion remain activation work.

Schema 12 → 13 adds discovery collection cursors and splits the worker's delivery
cursor, retaining its original value for enrichment. Existing outbox rows,
staged metadata/page bodies, claims and receipt bytes remain unchanged in one
transaction. Unknown table/column collisions roll back promotion, including
any preceding column rename. Schema 13 → 14 adds the detail journal and its
dispatch tables/cursor while preserving every existing value. Schema 14 → 15
adds the candidate-inspection cursor without replacing pending work or resetting
backoff. Older producer binaries refuse schema 15;
include the outbox in backups and retain its pending evidence during rollback.
These outbox cursor tables do not require a separate native database migration.

## Validation

After `make setup`, `make test-producer` runs delivery and actual
gallery-dl runtime tests, including the same retention corpus used by Go.
The Makefile uses the isolated `.venv` environment with the pinned dependencies.
Tests cover concurrent delivery, subprocess death,
receipt replay, token rotation, dependency ordering, capacity, partial batches,
redirect rejection, disabled file processing, download/skip/postprocessor paths,
Twitter transformations, filename budgets, lease loss, bounded resume, offline
window coalescing, caller tickets, split completion ranges, cancellation followed
by later rescans, schema promotion, shared window semantics,
portable configuration, changed assets, worker failure outcomes and log isolation.
Caller tests cover a 500-source list, lost command responses, frozen relative
windows, concurrent admission, expired resolution leases, atomic binding/ticket
rollback, reviewed matches, later rescans and schema-4 preservation.
The backend test
`TestPythonProducerDurableDeliveryAgainstNativeHTTP` runs this actual client
against the native HTTP router and SQLite, loses committed event and run-submission
responses, and checks replay after reopening, independent rejection, caller-ticket
deduplication, exact ticket completion and the submit/claim/renew/checkpoint/finish lease
cycle. `TestPythonDownloadWorkerRecoversFinishAndDeliversFiles` runs the worker
against the real API/SQLite from a persisted URL-list caller, drains a capture during downloading, recovers a lost
attempt-completion response and a committed download-report response, and admits
dependent file/report events after reopening the outbox. It checks the ticket-status
and call-status CLIs after restart and verifies that source success still leaves
actual media intake queued. It exports/restores the library and real producer
outbox together, compares receipt-boundary proofs and reopens the relocated native
database to recover the original report receipt.
The same real download fixture exercises the n8n adapter's stable record/inspect
token, pending result, lost permanent-completion response and exact-proof replay
after reopening the outbox. Source completion still leaves file intake queued.
`TestPythonBackfillImporterReplaysAfterLostNativeResponse` executes the maintenance
importer against the real application router/SQLite, loses a committed batch's
response and verifies replay without duplicate decisions or source-file changes.
`TestPythonScanJournalImporterRetainsAtomicSnapshotAfterLostResponse` does the
same for a whole journal, then checks paginated summaries and every original
record through the real HTTP inspection routes. Migration fixtures preserve
the seven families, old extractor columns, timestamps and embedded JSON strings;
unknown inputs and conflicting snapshot identities cannot partially publish.
The Python tests run in this repository's independent release workflow. The
Go/HTTP checks remain in the Stash repository's explicit `make test-clients`
suite, pinned by `integration-clients.json`; core CI selects them for contract
changes. No production endpoint or source website is contacted.

## Tumblr and gallery file sources

The pinned Tumblr adapter retains a post's numeric ID and its complete ordered
photos list before gallery-dl consumes it. Each downloaded photo refers to that
shared membership and post body; per-file photo fields remain capture details.
Only an explicit multi-photo list declares an album. Inline media and other
selected outputs retain partial membership without claiming an exhaustive list.
Avatar and external-link traversal need separate adapters and are rejected by
this adapter; ordinary post photos, inline media, videos and audio exclusions
are covered.

Chevereto sources (`jpgfish`, `imglike`, `putmega`) use the actual file-page ID
and its one observed attachment. An album label in a directory does not become
an invented post or performer. Parent album/user traversal uses a configured
scan; each file page keeps its own source identity.

LeakGallery uses the creator-page path plus the post ID to avoid combining
unrelated pages. All media URLs observed on one HTML post share one retained
manifest, but an HTML scan does not establish complete membership or an album.
API pagination failures now fail the native attempt even when the pinned
extractor catches and logs the underlying error. Its earlier completed files
and receipts remain durable for retry.

Erome album URLs use the explicit `album_id`, album title and observed photo/video
order. The adapter copies each file's metadata before the extractor mutates its
shared dictionary, then captures one bounded, shared manifest. HTML membership
stays partial, and its media-URL timestamp is not a publication date; use a
traversal profile. Uploader names do not assign performers. Native archive keys
use `native-v1_{album_id}_{web_media_id}` where `web_media_id` is the canonical
source URL digest. Reordering cannot archive a different file by position.
Native output filenames use `{album_id}_{web_media_id}.{extension}` as well, so
an existing file at an old position cannot be linked to a different attachment.
Custom filename formats must contain `{web_media_id}` in every rule; archive
formats must contain both `{album_id}` and `{web_media_id}`.
Existing position-based
entries are preserved but cannot prove completion for this native adapter.
The existing durable manual request and outbox support interrupted retries;
`stash-gallery download https://www.erome.com/a/<album-id>` needs no separate
scraper or database migration.

These adapters use version-1 `web_media` evidence and URL-qualified attachment
references, independently of local filenames and gallery-dl's output counters.
Only explicit source identity is used; folder names and scrape targets do not
establish depicted performers. Audio and other unsupported files retain an
excluded outcome without a download. Missing dates and the extractor's year-one
placeholder are not promoted to publication dates. Historical payloads without
the new marker keep their original capture partitioning.

### Stash restart recovery

Stash can restart during a scrape. The worker keeps downloaded files and its
local SQLite outbox, stops starting source work if its lease becomes unavailable,
and resumes the same native run after service recovery. Listing replay may be
necessary to find the saved cursor; download archives skip completed files.
Saved metadata/checkpoint bodies are replayed with their original receipts.

Actual job failures are counted separately from lease ownership generations.
Repeated Stash/worker restarts therefore do not increase the failure backoff.
Download runs keep retrying recoverable errors without a terminal failure count;
metadata jobs retain their separate retry policy. Expired metadata jobs are eligible once their old lease and
existing source pacing permit; source runs retain their configured cooldown
with a 30-second minimum recovery delay. Transport polling also retains its
normal backoff. Genuine source errors still increase the retry delay.

An ordinary Stash restart needs no worker drain or n8n shutdown. Compatible
producer repairs also preserve queued policies. An incompatible execution/capture
contract or reviewed profile change requires the explicit policy handoff while
retaining all queued data. Exact implementation bytes remain diagnostic provenance;
see the [release compatibility review](../../README.md#worker-compatibility-review).

## Missing linked media

Reddit posts linking to individual Imgur or Redgifs items no longer hold a full
account scrape when the exact item API repeatedly returns 404 or 410. The adapter
makes at most three item requests, respecting the existing shared request budget.
It saves temporary observations in the producer outbox. Once all attachments in
the post are fetched or terminally unavailable, execution v3 records post completion
in the configured gallery-dl archive and removes the temporary observations.
A success before the limit clears the failed observations. Missing-media caches
older than 48 hours are pruned when a Reddit worker starts.

Execution v3 does not create a permanent missing-file ledger or new
`native_media_status` capture for these decisions. The post completion entry means
processing finished; it never creates a completed file event or a successful file
archive entry for absent bytes. Available Reddit previews can still be downloaded.
Account/listing requests, unrelated API endpoints, authentication,
rate limits, network errors and server errors do not qualify for this skip.

Historically, producer schema 19 stored these decisions in the same database as jobs and
receipts. Its atomic migration imports v0.3's adjacent `.missing-media.sqlite`
file once, checking the original producer binding and preserving every observation.
The upgrade-only reader leaves that file unchanged; schema-19 workers never read
or update it again. Keep it as rollout evidence until the imported rows are
verified, then retire it. A corrupt or foreign file stops migration without
altering the old outbox. Restarting retries the same migration transaction.

Upgrade all readers and writers of a shared outbox together, including the n8n
command environment and archive/backup tooling, before restarting workers.
Versions through 18 remain readable by the new archive verifier with the same
historical receipt report. Version 19 adds verified missing-media counts and a
digest; future versions and downgraded headers that hide the table are rejected.
The ordinary producer snapshot now includes skip decisions and partial attempts.
Restoring it preserves both without a separate cache or inventory entry. The
Stash library schema and portable archive container format are unchanged.

The schema-19 release changed the old source-based policy fingerprint. Update the worker and n8n's
mounted command environment together so new admissions use the same policy. Keep
producer IDs, database paths, file locks and queued receipts. Approve compatible
policy upgrades for existing unfinished runs through Stash's administrative API;
keep their UUIDs, windows, checkpoints and retry deadlines. A deferred configuration
error still needs its own repair and review. Website downloader revisions remain
unchanged by this release.


### Subscription scheduling across Stash outages

Subscription launchers persist their intent in producer schema 20 before fetching
subscriptions. The intent freezes the caller UUID, reviewed policy and publication
window. A temporary network error or HTTP 408/425/429/500/502/503/504 leaves it pending;
`dispatch-all` retries it with exponential delays from 15 seconds to five minutes,
honoring a longer `Retry-After`. Retries continue until recovery and survive process
exit or reboot. Actual retry timing also depends on the worker timer and current
worker activity. Each pass handles at most one due scheduling intent before normal
queue delivery and execution, so an outage does not monopolize the worker.

The first successful subscription read freezes the enabled targets and revisions.
Recording those targets and marking the intent recorded happen in one transaction.
Repeated `--call` invocations retain the first window and policy; lost responses and
expired leases cannot duplicate that handoff. An empty subscription list completes
as `empty`. Pending scheduling is accepted work, not confirmation of a scrape;
`--strict-errors` still reports incomplete work. Local configuration/storage errors
fail immediately. Authentication, scope and invalid server responses are retained
as `review`, with a sanitized error code and HTTP status.

Use `stash-ingest ... schedules-status [--call UUID]` to inspect scheduling and
`stash-ingest ... retry-schedule UUID` after correcting a reviewed failure. The
normal worker dispatcher resumes it. No website requests are made by this stage.

Upgrade all users of a shared producer database and the archive reader to v0.6.7
before opening it with schema-20 workers. Older binaries deliberately reject the
new schema. This includes n8n command bridges and host maintenance clients, not
only background workers. Queued source windows, policies, receipts and website
backoff contracts are unchanged. Historical schema-19 archives remain supported.

### Queue contention and retained history

Producer schema 21 adds `request_run_tickets(request_uuid,ticket_uuid)`. The capacity
query starts with the partial index of unfinished submissions and probes these
links, deduplicating callers that still have unassigned coverage. Historical
admissions remain durable without adding work to each new admission. Opening an
up-to-date queue validates the binding in a read transaction; initialization and
upgrades still take the writer lock and commit atomically.

The background event drainer retries SQLite BUSY/LOCKED with interruptible delays
from 0.1 to 2 seconds after the existing SQLite busy timeout. It reclaims durable
work using the existing leases and receipt fences; it never replays arbitrary
transaction bodies. Storage errors crossing gallery-dl's exception handling retain
their cause and make the source attempt retryable, not a source/configuration hold.
The `drain`, `dispatch`, `dispatch-all`, `submit-runs`, and `resolve-sources` commands
report `retry/outbox_busy` (exit 2) on contention. New admissions still fail visibly
if persistence fails, and disk, permission and corruption errors remain diagnostic
failures rather than successful service exits.

Deploy v0.6.10 archive readers and all shared-outbox clients before schema-21 workers.
The migration only adds an index; producer UUIDs, policies, windows, event bytes and
receipts are unchanged. Earlier schema archives retain their verification rules.


### Discovering Redgifs profiles from Reddit

A resolved Redgifs child capture keeps its API uploader and the directory
actually selected by the Reddit job in `native_redgifs_directory`. Stash accepts
this destination only within the current collection's root and path prefix,
registers one monitored Redgifs profile per root, and retains its discovery
capture in the source definition history. Profile discovery applies to the
Reddit author's own profile passes (including backlog), not subreddit or saved
feeds, crossposts, or comments. A missing uploader or destination cannot create
a download subscription. Existing source destinations and subscription decisions
are preserved. Related profile URLs do not establish ownership of another
publisher or assign depicted performers.

`stash-ingest-redgifs --subscriptions --full-history --profile PROFILE` records
archive-aware passes for all enabled `native:redgifs` subscriptions. It uses the
same durable subscription scheduling, retry, scope and pause checks as other
launchers. Hosts choose the cadence; new subscriptions are picked up at the
next invocation. Direct `--username HANDLE_OR_PROFILE_URL` and explicit
`--config-file` profile lists are also supported.

For a dedicated Redgifs worker, use the existing archive and storage bindings,
`source_category: redgifs`, and a full-history profile with global `skip: true`.
Add `native-profile-directory: true` under `extractor.redgifs.user`. This native
adapter option uses the claimed collection's non-root path prefix as the exact
download directory; arbitrary website keywords cannot replace it. The default
gallery-dl directory remains the fallback for whole-root manual sources. Put
filename, fallback directory and postprocessors under that same `user` section,
so Reddit child extractors retain their current inherited directory. Share the
Redgifs archive with the Reddit worker to avoid downloading the same clip twice.

### Reddit profile refresh

Reddit profile and submitted-post runs now emit an independent `account.profile`
event before iterating posts. This refreshes performer URLs even when a run has
no posts, every post is outside its window, or every file is already archived.
The API bio and the current Reddit page's Social Links are retained only when
both identify the same stable Reddit account. Post/comment body links are not
profile evidence. Captured profile fields use the same retention rules in Go
and Python.

The worker resolves `myredlink.com` (including its Continue/JavaScript landing
page), `t.co`, `bit.ly` and `tinyurl.com` with a five-hop limit and a separate
session without Reddit credentials. It records the published URL and resolved
destination as evidence. It does not fetch destination profiles or recursively
crawl unknown link hubs. Unavailable social pages or shorteners retain the bio
or original link and warn; subsequent profile runs retry. Requests share the
existing source budget and rate-limit handling.

Deploy the Stash server with schema 1000134 and `account_profile_protocol: 1`
before releasing this worker. The Reddit worker checks this capability before
execution, and the outbox keeps profile deliveries pending on an older server.
Outbox schema 22 preserves existing queued bytes and receipts; older binaries
must not reopen it. Keep the outbox when upgrading. No media cursor, publication
window, archive key, performer ownership or configured destination changes.
Existing owner links, normalized URL deduplication, and explicit URL removals
continue to govern which performer receives new links. No additional timer or
configuration is needed for profile refresh during scheduled Reddit runs.
