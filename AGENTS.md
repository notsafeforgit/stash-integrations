# Reusable Stash integrations

This repository owns optional Python API clients, their packaging and tests.
Stash owns the schemas, API contracts, database and application. Installation
schedules, website credentials, destinations, FileFlows and backup orchestration
belong to deployment repositories such as stash-ops. Never add personal config.

Use uv, run make setup test before publishing, and preserve installed command
names, durable outboxes, producer identities and acknowledgement semantics.
Queued policies bind the explicit execution/capture contract and reviewed profile,
not Python source bytes. Exact source and downloader identities remain runtime
provenance. Every producer source change requires a reviewed entry in
policy-compatibility.json (including a reason and the exact implementation digest).
Use scripts/check_policy_contract.py and the contract tests: compatible repairs
retain the contract; changes to admitted command meaning, coverage or cursor
semantics must bump it and use an explicit deployment/upgrade plan. Never bypass
the gate by excluding a changed module from provenance. pkg/ contains versioned fixtures,
not server implementation. Keep their provenance in extraction.json.

Release the three distributions together with a v-prefixed tag. The CI workflow
builds wheels and source archives. Consumers pin a release commit or artifact;
website downloader updates remain a separate tested deployment policy.
