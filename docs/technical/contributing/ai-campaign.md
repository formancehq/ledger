# Audit campaign coordination and inspection

`scripts/ai-campaign` is a deterministic coordination and projection layer over
qualified native audit results. It preserves immutable audit provenance, caches
external observations, and reports the mechanically next valid workflow
transition. It does not perform engineering reasoning, choose product priority,
fix findings, publish issues, dispatch agents, mutate pull-request bindings, or
merge pull requests.

## Motivation and boundary

Qualified audit findings currently survive as native JSON artifacts and,
optionally, independently published Jira issues. That does not give an
interrupted agent or operator one durable, machine-readable view of every
qualification and its current external bindings.

The campaign artifact provides that local coordination view. It is
operational/informational state, not Ledger product state or a source of business
truth. GitHub, Jira, Git, and the native audit/challenge artifacts remain
authoritative. A campaign can derive and cache observations from them; it cannot
override them or manually advance externally derived state.

The v1 storage model is one JSON file. By default it is written below ignored
`build/ai-campaign/`, so it survives an agent session without entering a
candidate/product commit. An explicit destination outside the repository is also
allowed. Repository-local destinations outside ignored `build/` are rejected.
Writes use a directory-anchored temporary file, file sync, atomic rename, and
directory sync. This is proportionate local durability and a human-inspectable
base for later distributed claims without implementing a claim protocol now.

## Import

```sh
bash scripts/ai-campaign import qualified.json --audit audit.json
```

Use `--output <path>` to select another campaign artifact. Reimporting the same
campaign at the same path is idempotent and preserves its original import time
and cached observations. Replacing a different or malformed campaign is refused.

Import requires both artifacts because the qualified result contains the
qualification set while the source audit contains the original finding set and
metadata. `ai-audit-challenge` records `sourceAuditDigest` from its private audit
snapshot in every newly published qualified result. Import recomputes that
SHA-256 digest and rejects a different or modified source report.

The importer fails closed on:

- unknown or malformed source/challenge fields;
- an unsupported campaign schema version;
- duplicate finding IDs;
- an invented or omitted qualification result;
- changed severity or title;
- mismatched audit ID or audited SHA;
- a mismatched source audit digest;
- a non-canonical finding ID or unsupported qualification.

All `CONFIRMED`, `LIKELY`, `QUESTION`, and `REJECTED` findings are retained.
Only `CONFIRMED` has `dispatchable: true`. Import sorts findings by stable ID and
derives `campaignId` from the schema version, audit identity, audited SHA, and
both artifact digests. Import time is recorded separately and does not enter the
logical identity.

The schema is [`scripts/ai-campaign.schema.json`](../../../scripts/ai-campaign.schema.json).
The artifact separates:

- `sourceFacts` (`SOURCE_FACT`): immutable imported identity, minimal finding
  metadata, qualification, digest-bound JSON references into the two source
  artifacts, and per-finding/source-set identity digests;
- `observations` (`DERIVED_OBSERVATION`): refresh time, provider freshness,
  target SHA, exact bindings, projected states, blockers, and next actions;
- local/imported bindings: none in v1. There is deliberately no binding mutation
  command. A future explicit binding must use a distinct
  `LOCAL_IMPORTED_BINDING` class rather than masquerading as external truth.

Stable digest-plus-JSON-pointer references preserve original invariant and
challenge-summary identity without copying arbitrary prose into campaign state.
The native artifacts remain the content source.

## Inspect

```sh
bash scripts/ai-campaign inspect build/ai-campaign/<campaign-id>.json
bash scripts/ai-campaign inspect build/ai-campaign/<campaign-id>.json --offline
```

Live inspection queries:

- GitHub for PR state, current head, base, merge state, current review decision,
  and check summary;
- Jira for issue existence, status, and assignee;
- Git for the current `release/v3.0` target and same-repository PR branch refs.

The defaults are `formancehq/ledger`, `EN`, `origin`, and `release/v3.0`; the
command exposes read-only overrides. The campaign never treats current reviews
or checks as an exact-current readiness verdict. It does not recreate the
publication/readiness engine.

Every provider records `FRESH`, `STALE`, or `UNAVAILABLE` plus its observation
time and a concise failure. Campaign/finding freshness is `FRESH`, `PARTIAL`,
`STALE`, or `UNAVAILABLE`. When a live provider fails, cached values remain
visible but the affected binding status becomes `UNKNOWN`. `--offline` performs
no external calls and marks cached observations stale. A confirmed finding never
reports a readiness-like next action from partial, stale, or unavailable
external truth; its next action is `REFRESH_REQUIRED`.

`refreshedAt` is the last time at least one external provider refreshed data. It
is not advanced by a wholly offline/unavailable inspection.

## Exact binding markers

Jira discovery reuses the publisher marker exactly as written in issue
descriptions:

```text
AI-AUDIT:<finding-id>
```

Jira search is only a prefilter. An issue binds only when its description
contains a trimmed line exactly equal to the marker. Prefix matches, summary-only
matches, and fuzzy text do not bind.

A PR carrying that same exact body line is an explicit audit binding. When no PR
has the audit marker, inspect may retain a weaker historical binding through one
exact known Jira reference line (`Jira: EN-123`, `Fixes EN-123`, or the existing
linked-key form). An explicit audit-marker match takes precedence over Jira-only
references. Multiple matches at the selected exactness level produce an
ambiguous binding; inspect never chooses one. A merged Jira-only PR remains
`VERIFY_RESOLUTION` with `MERGED_WITHOUT_EXACT_AUDIT_MARKER`; it is not proof that
the audit finding is fixed.

## Derived finding states

| State | Mechanical meaning |
|---|---|
| `NON_DISPATCHABLE` | Qualification is `LIKELY`, `QUESTION`, or `REJECTED`. |
| `CONFIRMED_UNASSIGNED` | Confirmed, current audit target, no Jira or PR binding. |
| `TRACKED` | Confirmed, current audit target, exactly one Jira issue, no PR. |
| `PR_OPEN` | Exactly one bound PR is open and its branch/head binding is intact. |
| `PR_CLOSED` | Exactly one bound PR is closed without merge. |
| `MERGED` | Exactly one bound PR is merged; resolution still needs verification. |
| `AMBIGUOUS` | Multiple exact Jira or PR bindings exist. |
| `BROKEN_BINDING` | A bound unmerged PR has an unknown state or missing branch/head ref. |
| `BLOCKED` | Current target identity is unavailable or advanced without a bound PR. |
| `SUPERSEDED` | Reserved for a future externally evidenced supersession contract. |

State is recomputed from immutable qualification plus the latest observation;
there is no manual state-transition command. Target advancement does not reuse
an older `READY` interpretation. A confirmed finding with no PR becomes
`BLOCKED` / `REQUALIFY_ON_CURRENT_TARGET` until qualification is established on
the current target.

## Next action

```sh
bash scripts/ai-campaign next build/ai-campaign/<campaign-id>.json
```

`next` does not contact external systems or mutate the artifact. It projects one
mechanically safe workflow transition per finding from cached state:

| Condition | `nextAction` |
|---|---|
| Confirmed, no Jira or PR | `PUBLISH_JIRA_OR_REVIEW_POLICY` |
| Confirmed, Jira, no PR | `READY_FOR_CLAIM_OR_DISPATCH` |
| Open PR | `CONTINUE_PR` |
| Merged PR | `VERIFY_RESOLUTION` |
| Closed unmerged PR | `REVIEW_CLOSED_PR` |
| Broken binding | `REPAIR_BINDING` |
| Ambiguous binding | `RESOLVE_BINDING` |
| Advanced target without PR | `REQUALIFY_ON_CURRENT_TARGET` |
| Stale/incomplete external truth | `REFRESH_REQUIRED` |
| `QUESTION` | `HUMAN_DECISION_REQUIRED` |
| `LIKELY` or `REJECTED` | `NO_ACTION` |

`READY_FOR_CLAIM_OR_DISPATCH` is a label for the next future workflow phase, not
a claim, dispatch, priority decision, or authorization to write. The command
does not rank unrelated findings.

## Output contract

Every command writes a concise human summary to stderr and stable structured
JSON to stdout. This keeps interactive use readable while letting future agents
consume stdout without scraping prose. Inspection JSON includes campaign/audit
identity, source digests, refresh/freshness data, provider status, and structured
finding identity, severity, qualification, state, Jira/PR details, observed
candidate/target SHAs, blockers, next action, freshness, and confidence.

## Explicit non-goals

This milestone does not implement distributed claims, claim leases, dispatch,
PR-binding mutation, publication resume/events, merge, Jira mutation, composite
exact-SHA readiness, or a shared structured failure envelope. Those require
separate contracts and trust reviews.
