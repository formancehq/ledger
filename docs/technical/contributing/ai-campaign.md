# Audit campaign coordination and inspection

`scripts/ai-campaign` is a deterministic coordination and projection layer over
qualified native audit results. It preserves immutable audit provenance, caches
external observations, reports the mechanically next valid workflow transition,
and coordinates exclusive finding ownership through remote Git claim leases. It
does not perform engineering reasoning, choose product priority, fix findings,
publish issues, dispatch agents, mutate pull-request bindings, or merge pull
requests.

## Motivation and boundary

Qualified audit findings currently survive as native JSON artifacts and,
optionally, independently published Jira issues. That does not give an
interrupted agent or operator one durable, machine-readable view of every
qualification and its current external bindings.

The campaign artifact provides that local projection view. It is
operational/informational state, not Ledger product state or a source of business
truth. GitHub, Jira, Git, and the native audit/challenge artifacts remain
authoritative. A campaign can derive and cache observations from them; it cannot
override them or manually advance externally derived state.

The remote claim ref is the exclusivity authority. Campaign JSON is only a local
cache and never proves ownership. A session may begin or continue engineering
work only after it has successfully acquired or renewed the corresponding remote
claim. Claims coordinate ownership; they do not qualify findings, prove a branch
or PR correct, authorize merge, or override GitHub, Jira, Git ancestry, or review
and validation results.

The v1 storage model is one JSON file. By default it is written below ignored
`build/ai-campaign/`, so it survives an agent session without entering a
candidate/product commit. An explicit destination outside the repository is also
allowed. Repository-local destinations outside ignored `build/` are rejected.
Writes use a directory-anchored temporary file, file sync, atomic rename, and
directory sync. This is proportionate local durability, but it is not a
distributed lock and is never used as an ownership fallback.

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
logical identity. A qualified clean audit with empty `findings` and `results`
arrays imports as a valid zero-finding campaign; those arrays remain explicit
empty JSON arrays in campaign, inspection, and next-action output.

The schema is [`scripts/ai-campaign.schema.json`](../../../scripts/ai-campaign.schema.json).
The artifact separates:

- `sourceFacts` (`SOURCE_FACT`): immutable imported identity, minimal finding
  metadata, qualification, digest-bound JSON references into the two source
  artifacts, and per-finding/source-set identity digests;
- `observations` (`DERIVED_OBSERVATION`): refresh time, provider freshness,
  target SHA, exact bindings, projected states, blockers, and next actions;
- claim observations: cached remote ownership, lease, and optional work-branch
  binding. They remain derived observations rather than imported facts.

Stable digest-plus-JSON-pointer references preserve original invariant and
challenge-summary identity without copying arbitrary prose into campaign state.
The native artifacts remain the content source.

## Remote claim backend

One finding maps deterministically to:

```text
refs/heads/ai-claims/v1/<sha256("ai-claim-ref/v1" NUL auditId NUL findingId)>
```

The `refs/heads` namespace is used because GitHub reliably advertises and accepts
atomic updates to branch refs. Arbitrary top-level namespaces such as
`refs/ai-claims/*` are not a portable GitHub coordination interface. The fixed
`ai-claims/v1/` prefix is reserved for coordination; it must never be used for a
product or work branch, and claim refs are not intended for pull requests or
merge. The hash input comes only from validated campaign identity fields, never
from a candidate-provided ref name. It intentionally excludes source digests:
re-importing the same human finding ID with changed qualified evidence reaches
the same ref and is rejected as an identity conflict rather than creating a
second ownership lane.

Each ref points to a Git commit whose tree contains only `claim.json`. Initial
claims are parentless. Renewal and takeover commits have the exact prior claim
commit as their sole parent, leaving an inspectable object history. The record
schema is [`scripts/ai-claim.schema.json`](../../../scripts/ai-claim.schema.json)
and binds the campaign, audit and finding IDs, per-finding and qualified-result
digests, audited SHA, `CONFIRMED` qualification, claimant, lease times, target
branch/SHA, optional work branch, renewal metadata, and takeover predecessor.
Unknown fields, duplicate JSON keys, malformed trees, multiple parents,
unsupported versions, and identity mismatches fail closed.
Renewal/takeover transitions are checked against their parent record, and a
cached previously observed SHA must remain an ancestor of a changed live ref;
an external history rewrite becomes `CLAIM_HISTORY_REWRITTEN` / `AMBIGUOUS`.

Create pushes use `--force-with-lease=<ref>:`: the empty expected value requires
the ref not to exist. Renewal and takeover use
`--force-with-lease=<ref>:<observed-sha>`, and release uses the same exact lease
for deletion. A preliminary `ls-remote` is observation only; exclusivity comes
from the push compare-and-swap. Two clones may both observe absence, but only one
can create the ref. No local lock or remote-tracking ref is authoritative.

## Claim, renew, release, and takeover

```sh
bash scripts/ai-campaign claim <campaign> --finding <audit-id/finding-id> \
  [--claimant <session-id>] [--lease 24h] [--work-branch <branch>] \
  [--target release/v3.0]

bash scripts/ai-campaign renew <campaign> --finding <audit-id/finding-id> \
  --claimant <session-id> [--lease 24h] [--work-branch <branch>] \
  [--expected-claim-sha <sha>] [--target release/v3.0]

bash scripts/ai-campaign release <campaign> --finding <audit-id/finding-id> \
  --claimant <session-id> [--expected-claim-sha <sha>] \
  [--target release/v3.0]

bash scripts/ai-campaign takeover <campaign> --finding <audit-id/finding-id> \
  [--claimant <new-session-id>] [--lease 24h] \
  [--expected-claim-sha <sha>] [--target release/v3.0]
```

Only an immutable imported `CONFIRMED` finding is claimable. Claim also requires
the freshly observed target branch SHA to equal the campaign's audited SHA.
Every read and mutation requires the record's target branch and SHA to match the
active `--target` and campaign audited SHA; another branch pointing at the same
commit is still a distinct claim binding. A `LIKELY`, `QUESTION`, or `REJECTED`
finding is refused even when Jira or PR
evidence exists. The optional work branch is a claim binding only; these commands
never create a branch or mutate a PR binding. `renew --work-branch` can add or
replace that binding through the same exact-SHA CAS.

The default lease is 24 hours. Explicit leases must be from one hour through
seven days. Times are client-observed UTC values. Inspection and takeover treat
a claim as expired only after `expiresAt` plus a five-minute skew allowance; the
allowance can delay takeover but cannot make a live claim stealable. A clock that
appears to precede claim creation fails conservatively. Expiry never steals or
deletes a claim automatically. The owner must explicitly renew/release it, or a
new session must explicitly take it over. Takeover preserves the prior SHA,
claimant, creation/expiry times, and `EXPIRED_TAKEOVER` reason in `predecessor`.

When `--claimant` is omitted for claim/takeover, the tool generates a
human-readable non-secret value from sanitized user and machine labels plus 128
random bits. Set `AI_CAMPAIGN_CLAIMANT` once per task or pass the emitted claimant
to subsequent commands. Renew and release require an exact claimant match. A
recovered session may assume the same textual claimant and renew the exact claim;
this is operational recovery, not cryptographic identity proof. Repository write
authentication remains the security boundary, and remote commit/ref audit data
is the evidence of who performed an update. Do not put tokens, email addresses,
or other secrets in claimant IDs.

Structured mutation outcomes include `CLAIMED`, `ALREADY_CLAIMED`,
`CLAIM_EXPIRED`, `CLAIM_CONFLICT`, `FINDING_NOT_CLAIMABLE`,
`BROKEN_CAMPAIGN_BINDING`, `RENEWED`, `RELEASED`, `TAKEN_OVER`, `NOT_OWNER`,
`CLAIM_CHANGED`, `CLAIM_MISSING`, `HUMAN_DECISION_REQUIRED`, and
`REMOTE_UNAVAILABLE`. An ambiguous push response is re-observed when possible;
the tool does not claim success unless it can prove the outcome. If the remote
cannot be reached, claim, renew, release, and takeover perform no local fallback
and report `REMOTE_UNAVAILABLE`.

## Inspect

```sh
bash scripts/ai-campaign inspect build/ai-campaign/<campaign-id>.json
bash scripts/ai-campaign inspect build/ai-campaign/<campaign-id>.json --claimant "$AI_CAMPAIGN_CLAIMANT"
bash scripts/ai-campaign inspect build/ai-campaign/<campaign-id>.json --offline
```

Live inspection queries:

- GitHub for PR state, current head, base, merge state, current review decision,
  and check summary;
- Jira for issue existence, status, and assignee;
- Git for remote claim records, the current `release/v3.0` target,
  same-repository PR branches, and claim-bound work branches.

The defaults are `formancehq/ledger`, `EN`, `origin`, and `release/v3.0`; the
command exposes read-only overrides. `--claimant` (or
`AI_CAMPAIGN_CLAIMANT`) lets inspection distinguish this session from another
live claimant. The campaign never treats current reviews
or checks as an exact-current readiness verdict. It does not recreate the
publication/readiness engine.

Every provider records `FRESH`, `STALE`, or `UNAVAILABLE` plus its observation
time and a concise failure. Campaign/finding freshness is `FRESH`, `PARTIAL`,
`STALE`, or `UNAVAILABLE`. When a live provider fails, cached values remain
visible but the affected binding/claim status becomes `UNKNOWN`. `--offline` performs
no external calls and marks cached observations stale. A confirmed finding never
reports a readiness-like next action from partial, stale, or unavailable
external truth; its next action is `REFRESH_REQUIRED`.

`refreshedAt` is the last time at least one external provider refreshed data. It
is not advanced by a wholly offline/unavailable inspection.

Every finding exposes `claim.state`, `claimant`, `createdAt`, `expiresAt`,
`remoteRef`, `workBranch`, `observedClaimSha`, `freshness`,
`ownedBySession`, and a concise integrity problem when applicable. A missing ref
is unassigned only after a successful remote refresh. When refresh fails, cached
claim information is retained as stale/unknown and the finding requires refresh.
A previously observed claim that disappears outside the explicit local release
path becomes `CLAIM_HISTORY_MISSING`; it is not silently projected as safe to
claim. A missing bound work branch becomes `BROKEN_BINDING` and never releases
the claim.

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
| `CLAIMED` | One valid live remote claim exists; ownership may be this session or another. |
| `CLAIM_EXPIRED` | The valid remote claim is beyond expiry plus the skew allowance and needs explicit disposition. |
| `PR_OPEN` | Exactly one bound PR is open and its branch/head binding is intact. |
| `PR_CLOSED` | Exactly one bound PR is closed without merge. |
| `MERGED` | Exactly one bound PR is merged; resolution still needs verification. |
| `AMBIGUOUS` | Multiple exact Jira/PR bindings, a malformed/conflicting claim, or missing prior claim history prevents a safe projection. |
| `BROKEN_BINDING` | A bound unmerged PR or claim work branch is missing/unknown. |
| `BLOCKED` | Current target identity is unavailable or advanced without a bound PR. |
| `SUPERSEDED` | Reserved for a future externally evidenced supersession contract. |

State is recomputed from immutable qualification plus the latest observation;
there is no manual state-transition command. Target advancement does not reuse
an older `READY` interpretation. A confirmed finding with no PR becomes
`BLOCKED` / `REQUALIFY_ON_CURRENT_TARGET` until qualification is established on
the current target. For same-repository PRs, `branchExists` is true only when
the configured Git remote contains the exact observed head ref and SHA. Version
1 does not query a fork's remote directly, so an unmerged cross-repository PR is
conservatively `BROKEN_BINDING` with
`CROSS_REPOSITORY_BRANCH_UNVERIFIED`; its retained GitHub head OID is not treated
as proof that the source branch still exists.

## Next action

```sh
bash scripts/ai-campaign next build/ai-campaign/<campaign-id>.json
```

`next` does not contact external systems or mutate the artifact. It projects one
mechanically safe workflow transition per finding from cached state:

| Condition | `nextAction` |
|---|---|
| Confirmed and freshly unclaimed | `CLAIM` |
| Live claim owned by current session | `CONTINUE_CLAIMED_WORK` |
| Live claim owned by another session | `WAIT_OR_COORDINATE` |
| Expired claim | `REVIEW_EXPIRED_CLAIM` |
| Open PR | `CONTINUE_PR` |
| Merged PR | `VERIFY_RESOLUTION` |
| Closed unmerged PR | `REVIEW_CLOSED_PR` |
| Broken binding | `REPAIR_BINDING` |
| Ambiguous binding | `RESOLVE_BINDING` |
| Advanced target without PR | `REQUALIFY_ON_CURRENT_TARGET` |
| Stale/incomplete external truth | `REFRESH_REQUIRED` |
| `QUESTION` | `HUMAN_DECISION_REQUIRED` |
| `LIKELY` or `REJECTED` | `NO_ACTION` |

`CLAIM` is an instruction to attempt the remote compare-and-swap, not proof that
ownership was acquired. The command does not dispatch or rank unrelated
findings.

## Output contract

Every command writes a concise human summary to stderr and stable structured
JSON to stdout. This keeps interactive use readable while letting future agents
consume stdout without scraping prose. Inspection JSON includes campaign/audit
identity, source digests, refresh/freshness data, provider status, and structured
finding identity, severity, qualification, state, Jira/PR/claim details, observed
candidate/target SHAs, blockers, next action, freshness, and confidence.

## Failure, cleanup, and trust boundaries

Network partitions are conservative. Inspection may show cached claim data, but
it never turns an unavailable claim provider into `UNCLAIMED`. Mutations fail
with `REMOTE_UNAVAILABLE` or, when a concurrent/ambiguous update is observed,
`CLAIM_CHANGED`. Operators must refresh and inspect the exact remote SHA before
retrying. No unpublished work done before claim acquisition, or in a disconnected
clone that never acquired a claim, can be discovered perfectly. Claims prevent
duplicate work only after successful acquisition.

Explicit release deletes the ref by exact-SHA CAS and is sufficient cleanup for
this milestone. The local campaign cache is cleared after proven release when it
can be written safely. A merged PR does not automatically release a claim, and
abandoned claims remain visible until explicit release or expired takeover.
Later reconciliation may clean terminal claims, but no such automation exists
here.

Claim mutation tooling is policy code. Invoke `scripts/ai-campaign` from a clean,
base-pinned trusted-tool worktree, even when the campaign file or candidate work
branch is elsewhere. The launcher resolves its Go package relative to the script
itself so an absolute base-pinned invocation does not silently switch to the
candidate's package. Candidate comments, campaign prose, work branch contents,
and arbitrary remote JSON are untrusted data and must not choose mutation code or
ref names.

CAS prevents two conforming clients from both winning, but it cannot stop a
repository administrator or another writer from force-updating the namespace.
External rewrites are detected on the next exact observation; malformed or
identity-conflicting content fails closed. Claimant strings are diagnostic and
can be spoofed by a writer with repository permission, so repository Git
authentication and audit logs remain authoritative. The design relies on
SHA-256 collision resistance for the finding lane and on Git object-hash
collision resistance for exact CAS. No branch-protection or ruleset changes are
made by this tooling.

## Explicit non-goals

This milestone does not implement confirmed-finding dispatch, automatic
branch/worktree creation, PR-binding mutation, publication resume/events,
automatic merge, Jira mutation, composite exact-SHA readiness, a shared
structured failure envelope, or automatic terminal reconciliation. Those
require separate contracts and trust reviews.
