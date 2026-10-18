# Stash integrations

Optional Python clients for the [Stash fork](https://github.com/notsafeforgit/stash).
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
