# Publishing qualified audit findings to Jira

Deep-audit findings become durable work only after independent qualification. Jira publication is therefore a separate, explicitly authorized step after `ai-audit-challenge`.

## Eligibility

Only `CONFIRMED` findings are Jira candidates by default. `LIKELY`, `QUESTION`, and `REJECTED` are never published by this command. A human may handle them separately.

Publication does not implement a fix and does not create a pull request. The resulting Jira bug is the durable handoff into the normal engineering workflow.

## Stable identity and deduplication

Every ticket contains a machine-readable marker:

`AI-AUDIT:<finding-id>`

Before creation, the publisher searches the configured Jira project for that exact marker. If a matching issue already exists, it reports the existing key and does not create a duplicate.

Jira text search is word-based, so the search is only a prefilter. The publisher paginates through every result, requests each description, and accepts an issue as an existing duplicate only when one of its description lines is exactly the marker, which is how the description writes it. The match is confined to the description: a returned issue that carries no marker, carries the marker only in its summary, carries another finding's marker, or carries a longer marker sharing this one's prefix is not a duplicate, and publication proceeds to creation.

Because the marker becomes part of that search, the publisher re-validates finding ids instead of trusting its input: a challenge result is rejected unless every finding id is unique and has the canonical `<audit-id>/<short-kebab-case-name>` form described in `ai-audit.md`. An id carrying quotes or query operators can never reach the Jira search.

The marker identifies the logical finding across repeated audits and HEADs. The audited HEAD remains recorded separately as evidence provenance.

## Jira mapping

Default project: `EN`.
Default issue type: `Bug`.
Default component: `Ledger`.

Use `--component <name>` when the target Jira project requires a different
component. The publisher sends creation data through ACLI's structured JSON
input because Jira Components are not exposed by ACLI's simple create flags.
The description is encoded as Atlassian Document Format in that same request.

The ticket description records:

- stable audit finding id and audited HEAD;
- severity and qualification status;
- challenge summary;
- invariant and reachability assessments;
- evidence for and against the finding;
- existing relevant tests;
- reproduction plan;
- recommended next action;
- the deduplication marker.

Severity remains the audit severity; this first publisher deliberately does not map it to Jira Priority because priority is a product/engineering scheduling decision.

## Authorization boundary

`ai-audit-jira` is dry-run by default. Jira writes require an explicit `--publish` flag. It never edits existing issues, transitions issues, assigns people, comments, commits, pushes, or modifies GitHub.

The command uses the Atlassian CLI (`acli`) provided by the repository's Nix development environment. Authentication is an operator prerequisite and credentials are never read or copied by repository scripts.

## Engineering handoff

Jira is the durable backlog and assignment mechanism for confirmed findings.
Before starting work, check the Jira assignment and existing branches or pull
requests, then use the ordinary Git branch/worktree/PR workflow. Do not create a
parallel coordination record.

## Input binding

The challenge result is caller-owned and may live outside the worktree, so it can be edited or replaced while Jira writes are in flight. A run snapshots it once, before validation, into a private temporary directory and reads every later value from that snapshot. Replacing the external file mid-run cannot retarget publication or introduce unvalidated findings.

## Failure behavior

Publishing is fail-closed per finding. Search failure, malformed challenge input, unsupported qualification status, or creation failure stops the command. Successfully created issues are reported so a retry can deduplicate them by stable marker.
