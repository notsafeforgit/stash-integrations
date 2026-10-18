# Reusable Stash integrations

This repository owns optional Python API clients, their packaging and tests.
Stash owns the schemas, API contracts, database and application. Installation
schedules, website credentials, destinations, FileFlows and backup orchestration
belong to deployment repositories such as stash-ops. Never add personal config.

Use uv, run make setup test before publishing, and preserve installed command
names, durable outboxes, producer identities and acknowledgement semantics.
Adapter module bytes contribute to saved policy fingerprints: moving or releasing
a package must not incidentally rewrite these modules. Change behavior only with
contract tests and an explicit deployment plan. pkg/ contains versioned fixtures,
not server implementation. Keep their provenance in extraction.json.

Release the three distributions together with a v-prefixed tag. The CI workflow
builds wheels and source archives. Consumers pin a release commit or artifact;
website downloader updates remain a separate tested deployment policy.
