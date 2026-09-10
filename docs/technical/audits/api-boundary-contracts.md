# API boundary audit evidence and ownership

The [manifest](api-boundary-contracts.json) scopes a reusable audit of how HTTP,
service gRPC and `ledgerctl` translate inputs and expose results. Existing domains
cover the correctness of the resulting business operations and durable state;
they do not provide a focused inventory of transport presence, codecs, limits,
schemas and stream framing. This domain adds that inventory to the native audit
workflow without adding a runner or changing product guarantees.

## Evidence contract

For each hypothesis, identify a registered entry point, its middleware or
interceptors, the original input, the exact conversion chain, the downstream
request and the observable response. Cite the specific authoritative contract
and inspect guards and callers before asserting a violation. The API subsystem
README identifies gRPC as the primary service contract and HTTP as a compatibility
surface; that does not promise identical features, defaults or encodings.

Use `http-api.md` for response encoders and error presentation,
`protocol-compatibility.md` for the service gate, `api-comparison.md` and
`openapi.yml` for endpoint contracts, and the relevant section of `docs/ops/cli.md`
for CLI behavior. Reconcile these sources with registered routes, protobuf
presence, command flags, implementation comments and tests at the audited SHA.
A stale table is not proof of a missing route. Conflicting sources or unspecified
null/unknown-field/trailing-input policies become **questions**, not assumed bugs.

A reproduction plan must name the input, entry path, guard crossed, failure point
and distinguishing observation: exact outgoing value, status, header, body,
terminal stream error or trailer. Reject helper-only states that production
validation cannot admit. For severity, use the native P0–P3 standard and actual
impact: a cosmetic schema difference cannot establish corruption or authorization
bypass. P0/P1/P2 need a concrete failure path. Every first-pass finding remains a
hypothesis until an independent challenge tries to disprove it.

The output projection need not round-trip internal types. In particular,
LedgerLog JSON loses some typed metadata distinctions by design and is not a
backup/replay representation. JSON number precision must be assessed using exact
values, not a test oracle that has already rounded through `float64`.

## Boundaries

The broad adapter and CLI globs locate request builders, shared encoders and
existing tests. They do not authorize auditing every subsystem reachable from
those directories. Bucket, Cluster and Restore service request/response adapters
are in scope; snapshot/file transfer, restore execution, membership algorithms,
CLI self-upgrade, local credential stores and deployment orchestration are not.
Follow an outside caller only far enough to establish boundary reachability or
disprove a hypothesis, loading its routed documentation when needed.

| Neighbor or workstream | Ownership and deduplication |
| --- | --- |
| `accounting-invariants` | Owns amounts, balancing and business outcomes. This domain owns faithful conversion of supported values, without re-proving the accounting rules. |
| `idempotency-retries-partial-failures` | Owns keys, attempt identity, commit/acknowledgement ambiguity, bulk atomicity and retry safety. This domain owns error/status presentation, including the shape of bulk errors. Cancellation never proves noncommit. |
| `read-consistency-projections` | Owns consistency-mode propagation, barriers, query semantics and pagination under concurrent writes. This domain owns syntactic list options, stream completion and opaque trailer transport; route a lost consistency mode to the read domain. |
| `concurrency-lifecycle-shutdown` | Owns goroutine/resource ownership and shutdown. This domain owns propagation of an existing request context and the observable stream terminal contract. |
| `raft-membership-leadership`, `process-boundary-recovery`, `persistence-restore-replay` | Own consensus, process recovery and persisted formats/restore. A Cluster/Restore RPC conversion may be in scope; the operation's safety, file transfer and recovery are excluded. |
| `operator-reconciliation-durability` | Owns operator parsing of CLI output and reconciliation consequences. Trace a CLI representation change to establish its consumer contract, but deduplicate the resulting controller failure with that domain. |
| `test-reachability-enforcement` | Owns repository-wide test wiring and vacuity. This domain must still show its own probes reach the intended adapter. |
| Proposed `configuration-startup-contracts` ([PR #1972](https://github.com/formancehq/ledger/pull/1972)) | Owns server parameter sources, defaults, startup validation and consumer wiring. This domain owns request/response behavior under the installed adapter limits, not how startup selects those limits. |
| Authentication and credentials | Authentication internals, JWT trust/audience, credential storage, rotation and redaction are excluded. Use route/interceptor guards to establish reachability without re-auditing them. The protocol-revision declaration is independently in scope. |

## Existing work to check before reporting

This is a discovery snapshot from 2026-09-10 at base
`50dc4dcdca24a61d000294e7664c4172df721d81`, not a permanent claim about PR status.
Refresh the target, open PRs and available prior audit results in the trusted
outer preparation before a later run. Pass relevant deduplication context to
the read-only worker; a provider must not launch orchestration or publish Jira.

- [EN-1980 / PR #1953](https://github.com/formancehq/ledger/pull/1953): active
  forwarded error boundary work, including lazy cursor/status behavior. Do not
  restate that implementation as a new defect.
- [EN-1790 / PR #1947](https://github.com/formancehq/ledger/pull/1947): already
  merged LedgerLog JSON alignment. Use the merged output contract, not the old
  wrapper shape; any later finding needs a distinct remaining mechanism.
- [EN-1926 / PR #1962](https://github.com/formancehq/ledger/pull/1962): active JWT
  audience validation, excluded with authentication internals.
- [PR #1963](https://github.com/formancehq/ledger/pull/1963) and
  [PR #1970](https://github.com/formancehq/ledger/pull/1970): active credential
  redaction/typed projection work, excluded from this audit campaign.
- [EN-1829 / PR #1957](https://github.com/formancehq/ledger/pull/1957) and
  [EN-1954 / PR #1952](https://github.com/formancehq/ledger/pull/1952): active
  metadata limits and mutation-RPC reshaping. Check their current scope before
  reporting a boundary-limit or endpoint mismatch.

The historical duplicate `400` mapping under `/v3/{ledgerName}/promote` is a
requalification question. Inspect raw YAML, the actual loader's behavior and
client/schema consequences, then history and active work. Do not label it a
confirmed runtime defect or fix it as part of preparing this manifest.

Use stable ids `api-boundary-contracts/<root-cause-name>` and deduplicate by the
mechanism and required correction, not by transport or symptom. Where ownership
belongs elsewhere, record the handoff/overlap without publishing a second
finding. If backlog/history is unavailable, state that deduplication limitation;
do not claim it was checked.

## Validation and later execution

`TestAPIBoundaryManifestContract` uses the actual launcher's manifest validation
in a deliberately dirty disposable fixture. It must reach the clean-HEAD gate,
then stop before any provider runs; a malformed manifest must fail earlier.
The same test checks that scope globs match files and related documents exist.
It validates the manifest contract, not product correctness.

A later authorized campaign runs `bash scripts/ai-audit api-boundary-contracts`
at a clean exact HEAD, followed separately by
`bash scripts/ai-audit-challenge <audit-result>` at that same HEAD. Jira handling
and reproducer/product changes remain separate authorized work. Dynamic checks
in this manifest are proposals: record which actually ran and their limitations.
Use existing disposable fixtures and the pinned validation/cache workflow, with
no live user services or cleanup of another task's artifacts.
