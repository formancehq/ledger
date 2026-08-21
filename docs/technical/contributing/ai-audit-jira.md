# Publishing qualified audit findings to Jira

Deep-audit findings become durable work only after independent qualification. Jira publication is therefore a separate, explicitly authorized step after `ai-audit-challenge`.

## Eligibility

Only `CONFIRMED` findings are Jira candidates by default. `LIKELY`, `QUESTION`, and `REJECTED` are never published by this command. A human may handle them separately.

Publication does not implement a fix and does not create a pull request. The resulting Jira bug is the durable handoff into the normal engineering workflow.

## Stable identity and deduplication

Every ticket contains a machine-readable marker:

`AI-AUDIT:<finding-id>`

Before creation, the publisher searches the configured Jira project for that exact marker. If a matching issue already exists, it reports the existing key and does not create a duplicate.

The marker identifies the logical finding across repeated audits and HEADs. The audited HEAD remains recorded separately as evidence provenance.

## Jira mapping

Default project: `EN`.
Default issue type: `Bug`.

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

The command uses the authenticated Atlassian CLI (`acli`). Authentication is an operator prerequisite and credentials are never read or copied by repository scripts.

## Failure behavior

Publishing is fail-closed per finding. Search failure, malformed challenge input, unsupported qualification status, or creation failure stops the command. Successfully created issues are reported so a retry can deduplicate them by stable marker.
