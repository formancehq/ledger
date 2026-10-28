# Configuration startup contracts audit

This domain follows a server parameter from supported input to the component
that consumes it. Its reusable delta is value semantics: provenance, precedence,
parsing, defaults, validation, wiring and local versus replicated authority.
These can fail on an ordinary first startup without process death, restart or
reconciliation retries. Extending `process-boundary-recovery` would conflate
that chain with the independent evidence needed for a real OS process boundary.

## Evidence contract

For each candidate, establish the complete chain:

1. A supported CLI/environment input or an admitted/defaulted Cluster spec.
2. The selected source, parser, explicit/unset state and normalization order.
3. The validator and the specific documented constraint or consumer precondition.
4. The bootstrap constructor/options and actual component that use the value.
5. A concrete incorrect result and a feasible regression plan, including the
   existing protection or test that does not cover this path.

The manifest paths are entry points, not permission for a general audit of all
code in those directories. Follow additional callees only to establish this
chain, loading their authoritative subsystem contract. Inspect `main.go` and
resolve the exact go-libs version from `go.mod`/`go.sum` when following binding;
a test calling `LoadConfig` directly cannot prove production environment binding.
Environment-only values need their own precedence evidence. There is no implied
server configuration-file layer or universal precedence between every CR field,
`extraEnv`, shell argument and direct environment read.

The contract anchors are deployment's Configuration and Configuration Safety
Checks sections; `cmd/server` flag comments and sentinel/default tests;
`bootstrap.Config.Validate`, delegated consumer validators and persisted-config
checks; current operator API/CRD/defaulting and `buildEnvVars`; and
`deterministic-fsm.md` sections 3.4–3.6. Different defaults are not defects merely
because they differ: prove a supported input violates its intended meaning.

Keep the existing unsafe identity/TTL override distinct from non-bypassable
schema rejection and the explicit restore-mode branch. Trace in-memory mutation,
write order and residual persisted bytes on each error; validation rejection does
not imply no prior writes. Existing historical backfill branches are evidence to
inspect. From `v3.0.0-rc.1` onward, assess whether the target release needs a
tested migration from the preceding release.

The current layout relocates the persisted-config anchor to
`ZoneClusterPersistent`. The loader checks the old physical `ZoneGlobal` anchor
before reading the new one and rejects that prior layout before first-boot
identity persistence. Test both physical keys and the unsafe-override path;
the old probe is a rejection guard, not a compatibility read path.

TTL and the query-checkpoint limit currently use committed `ClusterPolicy`;
node-local flags describe desired values. Cache/bloom settings also have a
replicated `ClusterConfig` handoff. The older admission-only checkpoint-limit
example in section 3.4 must be read alongside the specific section 3.6 policy
contract and current code. Historical deployment snippets likewise may use an
old CR shape. Conflicting sources, unspecified empty/zero semantics, or an
unclear strict-versus-soft limit become audit questions, not asserted defects.

## Cache rotation default handoff evidence

Follow explicit zero, the omitted CLI default and a positive custom threshold
through loading, validation, node construction and the serialized leader
proposal. The initial cache and the captured committed configuration must use
1000 for zero/default and retain positive custom values. Apply the captured
bytes through the FSM and verify the persisted threshold; a matching committed
1000 must not cause a redundant proposal when the desired input is zero.
Preserve the shared Bloom message and caller-owned raw configuration.

Use the bootstrap proposal-default regressions and the server subprocess
restart regression as distinct evidence. The latter must observe a full
persisted ClusterConfig, rather than the RPC's cache-only fallback, before
stopping and restarting against the same storage. This proves prevention for
new entries; it does not prove replay recovery of a historical committed zero.
Keep that recovery limitation and mixed-version leader restrictions explicit
in the cluster operations guidance.

## Byte-size input evidence

Byte-size flags accept only non-negative expanded counts representable by int64.
Check binary and decimal suffix boundaries before signed/unsigned conversion;
`16Ei` must fail parsing instead of wrapping to the zero/default sentinel.
Use the parser's overflow, negative-size and boundary regressions, then the
server command's overflow-rejection and zero/default cases. Rejected flag
updates must preserve the prior value. Literal zero remains valid and keeps
its consumer-specific meaning. These tests cover direct CLI parsing and
`LoadConfig`; they do not establish environment binding or listener behavior.

## Hash algorithm input evidence

For the log-chain hash algorithm, `LoadConfig` must reject unsupported selected
strings before returning a usable configuration. `blake3` and `xxh3` are the
supported values; omission and empty values retain the BLAKE3 default. Use
`TestLoadConfig_RejectsUnknownHashAlgorithm` and the `TestLoadConfig_HashAlgorithm`
cases as evidence. Environment cases must use the production binder and cover
`HASH_ALGORITHM`, including a valid CLI selection over an invalid environment
value; direct loader calls alone do not establish that precedence.

## Ownership and exclusions

For static Ed25519 authentication, follow `LoadEd25519KeySet` into
`buildAuthConfig`: every nonempty key ID must be unique by exact string equality,
including identical entries. Duplicate rejection must precede reading that
duplicate's public-key file, while an earlier invalid-file error can still fail
first. The loader returns no keyset or policy maps on failure, and bootstrap
propagates the named-ID error before installing static/composite authentication
or scope/superuser maps. Use
`TestBuildAuthConfig_DuplicateEd25519KeyIDs` with valid synthetic key files to
assert the exact wrapped error and nil `KeySet`, `Ed25519AllowedScopes` and
`Ed25519SuperuserKeys`; loader duplicate cases distinguish ordering and zero
results. This is constructor evidence, not production binding, TLS validation,
process exit or listener/readiness proof. Per-request signature-to-policy
binding remains owned by `authentication-authorization-boundaries`.

| Domain | Ownership |
| --- | --- |
| `configuration-startup-contracts` | Static parameter resolution/translation, validation predicate, consumer wiring and desired-versus-applied authority. |
| `process-boundary-recovery` | Real exec, signal delivery, exit status, listener/readiness sequencing, hard death, local durable reopen and residual-state recovery. Incorrect value resolution/validation belongs to `configuration-startup-contracts`; correct rejection hidden by process exit/readiness belongs to `process-boundary-recovery`. |
| `operator-reconciliation-durability` | Selection/recreation of workloads, TLS/credential rotations, retries, rollout and durable CR/Job/Secret/PVC transitions. Static admitted-spec-to-args/env value translation belongs to `configuration-startup-contracts`; temporal convergence belongs to `operator-reconciliation-durability`. |
| `concurrency-lifecycle-shutdown` | Goroutine ownership, cancellation, Stop/join and startup unwind resource ordering. |
| `persistence-restore-replay` | Generic durability, checkpoints, restore data correctness and replay. This domain only traces configuration authority and validation; it does not prove crash recovery. |
| `raft-membership-leadership` | Raft/membership/leadership algorithms. This domain checks parameter delivery to those components. |
| Other accounting/read/idempotency/test domains | Business semantics, query correctness, retry outcomes, test discovery and CI enforcement. A test gap supports a configuration finding rather than a separate coverage-only finding. |

Exclude standalone `ledgerctl` profiles/keychain precedence, generic auth/security
review, performance tuning, new dependencies, compatibility designs and the
operator work tracked as EN-1999 through EN-2002. Auth/TLS configuration value
validation is in scope; authorization algorithms and rotation workflows are not.
Do not turn a broad path into a second audit of those neighboring mechanisms.

Deduplicate by root cause and required correction, even when a parameter causes
both startup and later runtime symptoms. Use stable finding ids
`configuration-startup-contracts/<root-cause-name>`. When the correction belongs
entirely to a neighbor, record the boundary in inspected areas or residual risk
instead of issuing a duplicate. Compare available prior artifacts and active PRs;
do not claim backlog deduplication without actually inspecting that backlog.

## Execution boundary

The dynamic checklist contains optional existing tests and proposed reproductions.
Report which checks actually ran. Use the pinned Nix toolchain, native temporary
fixtures and separate root/operator modules. Environment-mutating cases require
isolation; server fixtures use leased listeners and bounded lifecycle cleanup.
No live cluster mutation, tracked test additions or product edits occur during
the read-only audit. The manifest uses the existing native JSON contract without
new launcher fields or dependencies.

After manifest review/merge, the trusted outer workflow can separately run
`bash scripts/ai-audit configuration-startup-contracts`, then
`bash scripts/ai-audit-challenge <audit-result.json>` at the same clean HEAD.
Provider workers remain leaves. First-pass findings remain hypotheses until
challenged; Jira preview/publication and product fixes require separate tasks
and explicit authorization. Creating this manifest runs neither audit campaign.

## EN-2479 recovery and identity

Audit-key validation is independent of persisted node/cluster ID validation.
Recovery reads the 32-byte key, rejects malformed or missing key state with
history, and checks genesis before swapping FSM state. Restore preparation
removes the source operational identity while preserving the key. A destination
may then persist its own cluster ID and extend the source chain. Inspect the
startup and follower-recovery paths separately; the checker owns full-history
verification, and the persistence domain owns checkpoint/delta preservation.

## EN-2923 bloom rate validation

Trace desired bloom settings through CLI loading into `Config.Validate` before
Fx construction of `NewFilterSet`. Enabled rates must be finite and strictly in
`(0,1)` for every server slot, including indexes. CLI rate zero normalizes to
`0.01`; zero expected keys disables the type and ignores its rate. The operator
retains empty-rate defaulting and explicit-zero rejection, while rejecting
`NaN` as well as infinities. The server `TestLoadConfigBloomRates` and bootstrap
`TestConfigValidateBloom` exercise accepted/invalid/disabled inputs; operator
`TestValidateClusterConfig_RejectsInvalidFPRate` independently covers non-finite
spec rates. A constructor panic is the original failure signal, not an accepted
startup rejection. No persisted or committed configuration semantics change.

## Health polling interval

`--health-check-interval` / selected `HEALTH_CHECK_INTERVAL` must parse to a
strictly positive Go duration before the health worker starts. Zero does not
disable it; `health-clock-skew-threshold=0` has a separate disable meaning.
The default remains `30s`. Include negative, zero, malformed, overflow and
fractional values rounding to zero, along with smallest-positive values, in the
input evidence. Check explicit CLI precedence over invalid environment values.

For the typed operator `spec.health.interval`, nil/empty retains omission and
nonempty values must parse to a positive duration before workload reconciliation.
Rejected configuration records `ConfigValid=False`, `ValidationFailed`, and
`Phase=Error`, without creating or updating workloads or deleting an existing
StatefulSet. Cluster identity generation/persistence can precede validation;
failure status is written afterward. Correcting the spec clears the validation
failure and resumes reconciliation. `extraEnv` can override the typed field, so
the server remains the final validator for selected runtime configuration.

The server's config validation occurs after config loading/printing and possible
join discovery, but before Fx bootstrap. Do not claim it prevents all prior I/O.
`TestValidateHealthCheckInterval` exercises the otherwise-valid config boundary;
`TestHealthCheckIntervalStartup` uses the real service binder in subprocesses.
Its positive/default cases deliberately stop at a later threshold error, proving
interval acceptance without starting a cluster. Operator regressions must
exercise both first creation and existing-workload preservation, followed by a
real invalid-to-valid reconcile sequence. These checks do not prove recovery
across process death or Kubernetes admission rejection.
