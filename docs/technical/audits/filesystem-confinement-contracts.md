# Filesystem confinement audit evidence contract

This [manifest](filesystem-confinement-contracts.json) follows an external name
all the way to filesystem authority. It fills a reusable gap across snapshot
serving, follower reception and restore download destinations. Preparing or
merging it does not launch a product audit, qualify findings or publish Jira.

## Why a separate domain

The preparation base is `release/v3.0` at
`22f378c1a492316f3638fdb74afd9506111b5421` (2026-09-11).
The following existing manifests were inspected. Overlapping source paths do
not mean that their evidence contracts cover final filesystem confinement.

| Neighbor | Ownership and boundary |
| --- | --- |
| `persistence-restore-replay` | Durable bytes, checkpoint completeness, hashes/footer, formats, replay and recovery. This domain owns which local filesystem object a name can read/write/remove, including staging namespace collisions; it does not re-prove content integrity. |
| `api-boundary-contracts` | Decoding, representation, errors and service stream framing. Its companion explicitly excludes snapshot/file transfer and restore execution. Here adapters establish name provenance and entry reachability. |
| `authentication-authorization-boundaries` | Who may invoke the operation. An authenticated peer's file path still needs confinement; do not re-audit auth policy or invent new authorization requirements. |
| `configuration-startup-contracts` | Root-path value resolution, validation and wiring. Here the selected root bounds actual filesystem operations; do not re-audit flags or persisted configuration. |
| `process-boundary-recovery` | Real exec, hard death and durable reopen. No process crash is required to demonstrate an escaping path. |
| `concurrency-lifecycle-shutdown` | Active-user leases, cancellation/join and shutdown ownership. This domain owns namespace containment and which paths cleanup removes, not when a session may be retired. |
| Other domains | Accounting, query consistency, Raft, retry identity, operator reconciliation and repository-wide test enforcement remain with their owners. A missing regression supports a proven mechanism, not a standalone test-gap finding. |

Deduplicate by root cause and required correction using stable ids
`filesystem-confinement-contracts/<root-cause-name>`. If a correction belongs
entirely to a neighbor, record the boundary in inspected areas or residual risk
instead of emitting another finding.

## Source-backed entry inventory

The paths are bounded discovery seeds, not permission to inspect every behavior
in those files. Follow additional callers/callees only to establish an external
name-to-filesystem chain, loading the routed authoritative documentation.

| Entry and provenance | Chain and contract |
| --- | --- |
| Peer `FetchFileRequest.path` | `internal/bootstrap/module.go` registers SnapshotService on the peer server; `server_snapshot.go: FetchFile` acquires the session, checks `filepath.IsLocal`, then calls `file_streaming.go: streamOneFile`, which opens through the checkpoint `os.Root`. `AGENTS.md` and follower-sync's Filesystem confinement section require the rooted final operation. Check the actual installed peer guard, not a hypothetical public HTTP endpoint. |
| Peer `PrepareSnapshotResponse.manifest.files[].path` | `snapshot_fetcher.go: fetchWithSession` obtains the manifest and calls `validateSnapshotManifest` before starting parallel downloads. `file_fetcher.go: fetchFileOnce` checks locality again and uses rooted parent creation, temporary creation, removal and rename. Follower-sync's Manifest path uniqueness section establishes cleaned duplicates and destination versus raw-path-plus-`.tmp` collisions. |
| Backup checkpoint manifest map keys | Restore-mode registration in `internal/bootstrap/module_restore.go` reaches `server_restore_download.go: StartDownloadBackup`, `prepareDownload`, `executeDownload`, `downloadCheckpointFiles` and `downloadOneFile`. `backup.DecodeManifest` supplies checkpoint filenames; `safeStagingPath` in `server_restore.go` precedes `os.MkdirAll` and `os.Create`. The global AGENTS path rule applies; `docs/ops/backup-restore.md` describes staging recreation and cleanup. A lexical helper is not evidence of final symlink containment. Establish how any claimed malicious namespace state survives recreation or can arise afterward before reporting a defect. |
| Conditional archive seed | Whole-repository Go caller search at the preparation base found `tarutil.ExtractTar` only in its own tests and `internal/infra/attributes/prepare_test.go`. No production caller was established. Its `header.Name` to `filepath.Join` to `MkdirAll`/`OpenFile` chain is a helper observation, not a reachable product vulnerability. Recheck callers at the later audited SHA before promoting this seed. |

The backup storage abstraction has no filesystem driver. Remote object keys and
URLs are not automatically local paths. Resolve the effective build tags and
storage factory before claiming a real restore entry: local injected storage is
a diagnostic fixture, while production S3 support requires the appropriate build.

## Evidence required for every hypothesis

1. Identify the registered entry and mode, actual external field, installed
   caller/session guards and supported input that reaches the consumer.
2. Trace normalization, filename derivation and root selection through every
   parent creation, open/create, publication rename and cleanup operation.
3. Cite the violated repository contract and show who can influence each
   namespace component. An arbitrary hostile host process is not an assumed
   attacker capability; a symlink race needs a concrete feasible precondition.
4. Identify the escaped object or cross-operation alias and observable effect.
   Inspect existing guards and tests that could invalidate the hypothesis.
5. Give a safe regression plan with a valid control, distinguishing operation
   observations and unchanged unrelated filesystem state. A helper returning an
   error cannot prove the wire consumer rejected the same input before effects.

`filepath.IsLocal` is a lexical gate; `Join`, `Clean` and `Rel` do not resolve
symlinks or close validation/use races. Inspect the final root capability and
root acquisition as well as earlier side effects. Root confinement alone does
not promise inode isolation from pre-existing hard links: establish supported
filesystem semantics, provenance and attacker influence before extending a
claim to that case. Preserve allowed nested paths and do not impose a blanket
symlink, archive-type or filename policy absent from the contract.

A supported archive consumer, if later found, must be inspected for acted-on
header types, pre-existing symlinks, repeated names, partial extraction and
cleanup ownership. The current extractor handles directories and regular files;
ignored link headers are not proof of link creation. No all-or-nothing extraction
or universal permission policy is established here.

P0/P1/P2 require a concrete path under the native evidence standard. Missing tests,
a raw path API or an absent preferred helper alone do not confirm a defect.
Unclear platform semantics or conflicting authoritative contracts become audit
questions. Do not assume SSRF restrictions, new resource budgets, manifest
signatures, generic secret-redaction rules or host-wide filesystem hardening.

## Safe local diagnostic matrix

These are optional checks for a later authorized audit, not checks performed by
manifest preparation. Use pinned Nix tools and shared cache generations normally.

| Surface | Existing fixture or proposed stimulus | Required observation and limit |
| --- | --- | --- |
| Serving | `TestStreamOneFile_PathConfinement`, `TestSnapshotService_FetchFileRejectsNonLocalPath`; valid root/nested controls, traversal, absolute name, symlink to owned outside sentinel | Actual consumer reached; rejection discloses no sentinel bytes. Helper and RPC evidence are distinct. |
| Follower | `TestFileFetcher_FetchFileOnceRejectsNonLocalPath`, `TestGRPCSnapshotFetcher_RejectsNonLocalManifestPath`, `TestGRPCSnapshotFetcher_RejectsSymlinkEscape` | No outside write; final bytes for valid control; observe rooted creation/rename/cleanup, not merely string rejection. |
| Whole peer manifest | `TestValidateSnapshotManifest_RejectsCollidingEntries`, `TestGRPCSnapshotFetcher_RejectsStagingPathCollision`, `TestGRPCSnapshotFetcher_RejectsDuplicateManifestPath` | Rejection before any FetchFile; retain valid distinct staging names. Other aliases require a concrete supported-filesystem interleaving. |
| Restore destinations | Existing `TestSafeStagingPath_*` and `server_restore_download_test.go` injected `storageFactory`; synthetic manifest plus local content | Trace admission through actual download and staging recreation. Lexical tests alone do not prove final containment. No live S3 or cloud credentials. |
| Retry/cleanup | Existing `TestGRPCSnapshotFetcher_RetryRewritesPartialFile`; bounded failure then success and sibling sentinels | Attempt count, correct final bytes, temporary-file cleanup, unrelated state unchanged. No assertion of durable restart parity. |
| Namespace races | Temporary synchronized replacement at the precise validation/use or staging/publication boundary | Owned parent containing separate root and sentinel directories; deterministic barrier, bounded cleanup, no sleeps or arbitrary host paths. If no feasible influence is established, report the limitation. |
| Archive seed | Caller search first; optional isolated tar helper characterization | Clearly label helper-only results. No product finding without an actual production consumer. |

Read-only provider diagnostics must not add tracked tests or modify product
code. All filesystem fixtures, including outside sentinels, stay within one
owned disposable parent. Never probe real secrets or delete another task's
worktree, caches or artifacts. Record the exact tests run and untested cases;
green CI and a proposed diagnostic matrix are not proof of product correctness.

## Existing work snapshot and later refresh

Preparation used bounded GitHub title/body searches and Jira searches, then read
these concrete records on 2026-09-11. This is not exhaustive historical backlog
or prior audit-artifact deduplication; broad searches were noisy and paginated.
No equivalent filesystem manifest appeared in the checked-in inventory or the
exact GitHub title search.

| Work | Observed state and treatment |
| --- | --- |
| [EN-1633](https://formance-team.atlassian.net/browse/EN-1633), [PR #1652](https://github.com/formancehq/ledger/pull/1652) | Jira done; PR closed, not merged. The current base nevertheless contains the serving and receiving protections in [merged PR #1751](https://github.com/formancehq/ledger/pull/1751). Reconcile actual code rather than equating the closed original PR with missing protection. Known traversal/symlink and staging-collision guards are regression anchors. |
| [EN-1960](https://formance-team.atlassian.net/browse/EN-1960), [PR #1917](https://github.com/formancehq/ledger/pull/1917) | Jira done; PR merged. Active FetchFile session lifetime is concurrency ownership, distinct from path confinement. Do not reopen the known cleanup-before-read cause here. |
| [EN-1961](https://formance-team.atlassian.net/browse/EN-1961), [PR #1908](https://github.com/formancehq/ledger/pull/1908) | Jira done; PR merged. Restore cancellation/join and retained resources belong to lifecycle ownership. Local cleanup target confinement is separately in scope only with a distinct correction. |

Before execution, the trusted outer workflow must refresh the exact HEAD,
registrations, tar caller inventory, current tickets/assignments and PRs, and
available audit/challenge artifacts. Pass relevant deduplication evidence to the
worker and record any inaccessible history as residual risk. A ticket or PR
states intent; only current code proves a protection exists at the audited SHA.

## Execution boundary

After a separate execution decision, the trusted outer workflow reads
[ai-audit](../contributing/ai-audit.md) and runs
`bash scripts/ai-audit filesystem-confinement-contracts` at a clean exact HEAD,
then independently qualifies that report under
[ai-audit-challenge](../contributing/ai-audit-challenge.md) at the same SHA.
Providers remain leaf workers and never launch audit/challenge/Jira orchestration.
First-pass findings are hypotheses. Jira publication and product/reproducer
implementation remain separate authorized downstream actions.

Preparation validates only native manifest shape and referenced paths without
starting a provider, plus the repository's baseline and exact-base PR checks.
It introduces no runner, schema field or product change.
