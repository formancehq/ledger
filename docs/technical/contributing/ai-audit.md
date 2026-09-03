# AI deep-audit contract

This contract defines repository-wide audits for reusable correctness domains. It is intentionally separate from the PR review loop:

- PR review asks whether a bounded change is safe to integrate;
- deep audit asks which latent defects may already exist in an immutable repository state.

Routine diagnosis of a known failure, a single bug fix, and performance or tooling investigation remain task-specific unless intentionally promoted into a reusable audit domain.

An audit is read-only. It must never fix code, create commits, push branches, create issues, resolve review threads, or change GitHub metadata.

## Required contract

The launcher accepts one checked-in manifest, requires an exact clean `HEAD`,
and runs the provider with a read-only repository sandbox. It accepts a report
only when the output schema, audit id, audited SHA, finding-id format, and
finding-id uniqueness all validate. Finding ids remain stable across repeated
audits of the same logical defect.

## Audit target

Every result is bound to:

- the exact repository `HEAD` SHA;
- one named audit domain manifest under `docs/technical/audits/`;
- the complete scope and invariants declared by that manifest.

The auditor must inspect the current code, tests, and relevant technical documentation. It must not infer that green CI or existing tests prove an invariant.

## Evidence standard

Audit findings are held to a higher evidence standard than ordinary review suggestions. Every finding must include:

- a stable id in the form `<audit-id>/<short-kebab-case-name>`, unique within the report;
- severity `P0`, `P1`, `P2`, or `P3`;
- an exact location when possible;
- the violated invariant or expected property;
- a concrete failure path;
- impact;
- evidence from the repository;
- a reproduction plan or executable test idea;
- the existing test gap that allowed the defect to survive, when identifiable;
- confidence `HIGH`, `MEDIUM`, or `LOW`.

`P0`, `P1`, and `P2` findings must not be speculative. They need a concrete code path, violated invariant, or reproducible state transition. If the intended behavior cannot be determined from repository evidence, report an audit question rather than inventing a defect.

## Audit questions

Questions are explicit uncertainties that need human/product/architecture input. They are not findings and must not be counted as defects.

## Challenge and downstream publication

Every finding emitted by the audit is a hypothesis. Qualify the report with `bash scripts/ai-audit-challenge <audit-result>` according to `docs/technical/contributing/ai-audit-challenge.md`. The challenge pass independently tries to disprove every finding, including by searching for unreachable states, duplicates, and existing protections. A finding is not a confirmed engineering defect merely because the first audit emitted it.

A `CONFIRMED` challenge result is eligible to become a backlog/Jira candidate after human or policy approval. Jira handling is an explicit downstream action through `bash scripts/ai-audit-jira <challenge-result>`; the command previews candidates unless separately invoked with `--publish`. Neither the audit nor challenge pass fixes code, creates issues, or publishes findings automatically.

The operational sequence is:

1. run the manifest-scoped audit at an exact clean `HEAD`;
2. independently challenge the raw report at that same `HEAD`;
3. inspect the qualified report;
4. preview eligible Jira candidates, then publish only through an explicit
   `ai-audit-jira --publish` invocation;
5. begin ordinary engineering work after a confirmed finding has been published
   or otherwise assigned.

Jira is the durable backlog and assignment mechanism. Before beginning
engineering work, check the Jira assignment and existing branches or pull
requests; no parallel coordination record is required.

## Output

`bash scripts/ai-audit <domain>` writes a structured JSON report under
`build/ai-audit/`. The report records the audited HEAD, manifest id, findings,
questions, inspected areas, and residual risk.

The launcher writes provider output to a temporary file in that directory,
validates it, and atomically renames it to the final result name. The ignored
`build/` artifact is launcher-owned state; tracked repository content remains
read-only.

The audit command exits non-zero when the provider or output contract fails. The presence of findings itself is not an execution failure.
