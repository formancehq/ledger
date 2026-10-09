# FSM determinism, cache convergence, and coverage evidence contract

The [manifest](fsm-determinism-cache-coverage.json) defines a reusable audit of
the transition executed for every committed Ledger proposal. The operational
need is stronger than agreement on accounting arithmetic: replicas with
different process configuration and incidental cache histories must derive the
same apply result, audit evidence, in-memory state, and durable write set from
the same committed inputs. This domain owns both that oracle and the capability
boundaries that make it enforceable.

The manifest adds scope and evidence requirements to the native deep-audit
workflow. It does not change product behavior, add a runner, or authorize an
audit before this contract has been reviewed and merged.

## Equivalence oracle

Evaluate one transition as a tuple:

```text
T(prior committed state, proposal bytes, raft index) =
  (apply result, audit bytes, FSM counters, cache delta, durable batch)
```

For equivalent prior committed state, `T` must be identical on every replica
and during replay. Incidental local state is deliberately not an input: wall
clock, timers, random sources, goroutine scheduling, environment, flags, binary
defaults, process identity, cache residency, Bloom readiness, loader timing, and
map insertion order may differ. A finding must identify how one such difference
reaches a branch, ordering decision, read result, mutation, or encoding in `T`.

Compare semantic durable batches when storage iteration or physical encoding is
documented as irrelevant. Otherwise compare exact bytes, especially for audit
preimages and ordered protobuf output. A log statement or metric may differ
only when it is explicitly node-local and cannot affect apply control flow or
authoritative output.

## Declaration-to-enforcement proof

Every cache-keyed read needs a complete chain at the audited SHA:

1. The request/order/technical-update producer declares the canonical key and
   intent in its own `plan.Coverage`. Dynamic Numscript discovery enriches the
   matching aggregate and per-order declaration before `Build`.
2. An attribute resolver maps the declared sub-attribute code to the correct
   preload/cache lookup, and the execution plan carries a valid ID and intent.
3. The matching per-operation coverage bit is encoded after the final plan is
   known. Proposal-wide scope is used only by explicitly documented aggregate
   validation.
4. Apply constructs a fresh logical scope for that operation. Empty bits admit
   nothing; reuse of backing arrays must not retain prior authorization.
5. The handler reads through `processing.Scope`; `state.gatedScope` checks the
   correct attribute kind and canonical ID before an overlay, parent
   `KeyStore`, or `AttributeCache` can answer.

Build a matrix for all production dispatch variants and the complete attribute
registry. A declaration without a reachable read is harmless excess unless it
creates another concrete correctness failure. A missing declaration or gate is
not by itself a confirmed defect: demonstrate a reachable operation and the
incorrect rejection, mutation, or cross-node result. Conversely, one passing
test cannot prove registry exhaustiveness.

Idempotency coverage uses its documented dedicated channel rather than an
attribute slot. Non-cache committed state can have another scoped contract; do
not force it into this matrix, but verify that its access remains deterministic.

### Declarations derived from a read, not from the request

Step 1 is correct by construction only when the producer declares keys it can
name from the request. Some declarations instead come from an **observation** of
state: a revert's volume keys are knowable only from the stored target
transaction, and Numscript's from resolved balances and metadata. Those
observations can be wrong by apply time — admission reads the local store with no
read barrier, so a target committed but not yet applied there reads as absent and
the order declares no volume keys at all.

A fixture where the producer's read view already matches apply cannot expose
this. Exercise the divergence directly: make the dependency absent at admission
and present at apply. The chain to prove is that the producer binds its
observation into the proposal, the handler re-derives it from a key it is
*already* authorized to read, and the mismatch is rejected before the dependent
reads — not that the gate catches it afterwards. Reaching the gate for this cause
is itself the finding, because the gate's documented meaning is an admission bug.

The ordering half of that chain holds only where the observation keys on a
dependency the plan declares unconditionally, as a revert's target state does.
Numscript is the documented exception and is not a finding on its own: its
re-resolution can derive an account from changed metadata and perform the gated
read before the inputs hash is compared, so that miss is deliberately left fatal
— see [preload.md](../architecture/subsystems/fsm/preload.md#coverage-derived-from-a-read-must-bind-that-read).
What *is* a finding is widening that exception, softening it into a retryable
outcome, or introducing a new read-derived declaration that carries neither the
binding nor the ordering.

The comparison also sits behind, not in front of, the handler's existing checks
on the same dependency. A revert whose target is unknown, already reverted, or
backed by an inconsistent projection keeps returning those reasons; a stale
observation must not repaint a terminal verdict as a retryable mismatch.

Classification is part of the contract, and getting it wrong is a real defect in
both directions. A mismatch a re-admission could resolve must be retryable; one
it can never resolve — a target the same batch creates, so that rejecting the
batch un-creates it — must be permanent, or the client re-admits forever. And a
proposal whose observation *matches* must still reach the gate if it reads an
undeclared key: the observation check must not become a way to soften a genuine
under-declaration.

## Cache-horizon and preload proof

For a preload/cache hypothesis, establish the complete timeline:

1. the cache generation and canonical boundary observed by admission `Build`;
2. the actual keys and values resolved from Gen0, Gen1, Bloom-assisted storage,
   or the in-batch overlay;
3. guard acquisition, any generation-boundary rebuild, predicted-index stamp,
   proposal assignment, and loader release;
4. apply-time `MirrorPreload`, scope selection, reads, writes, tombstones, and
   rotation at the committed index;
5. `WriteSet.Merge`, cache mirror/Bloom updates, durable commit, and the next
   proposal's observable cache horizon.

Use at least two replicas with equivalent committed state but different
incidental cache placement when claiming divergence. Include absent, Gen0,
Gen1, tombstoned, and Bloom ready/not-ready states as applicable. A Bloom false
positive is a performance cost; a ready false negative that suppresses required
preload is a correctness mechanism. Loader retention is likewise a performance
optimization unless a concrete overwrite or missing-seed path defeats the
apply-time protections.

Snapshot, follower synchronization, and recovery may supply the differing
initial cache histories, but their own durability defects belong to adjacent
domains. This audit owns the defect when the next committed apply fails to
normalize or safely tolerate an otherwise valid local history.

## Post-commit lifecycle expectations

The volume sentinel must reduce successful effects in commit order. A ledger
deletion invalidates earlier and same-proposal updates because its cascade is
staged after projection writes. Ephemeral purges invalidate individual keys;
unrelated ledgers and later surviving updates retain their exact expected
values. Captured deletion names must not alias the next proposal's reused
`WriteSet` slice, and rejected orders must not be treated as deletion effects.
The aggregate scan must reject any volume rows left by a successful deletion,
even when those rows balance or the deletion is the only order in its batch.

Use `TestDeleteLedgerSentinel*` in state and node as focused entry points:
require successful same-proposal, multi-entry and separate-batch deletion,
multiple deletions and unchanged surviving balances. Deliberately missing or
changed surviving rows and balanced leftover rows of a deleted ledger must
still fail. The node regression reopens real
WAL/Pebble stores and calls `Applier.RecoverAndReplay`, and separately drives
asynchronous follower catch-up. It establishes those paths, not an OS-process
restart or a new remote Antithesis campaign. A fixture rejected by policy or
coverage guards never establishes the sentinel failure.

Preparation mutates the in-memory FSM before durable commit; sentinel reads
occur after that commit, so a failed check leaves the writes durable and
propagates a fatal error. This domain owns the expectation reduction; durable
WAL layout and checkpoint reconstruction remain with the recovery domain.

### Lifecycle coverage is a closed set

Lifecycle cleanup is correct only when the proposal declares the complete set
of current-state keys that the FSM may inspect or delete. The declaration must
cover every affected current-state key while preserving immutable history.

Review the boundary between the producer and apply for each lifecycle path:
ordinary writes, metadata-only orders, state transitions, skipped orders,
idempotent replays, and mirror ingestion. A producer must expose the trigger
and expected cost of any state-dependent enumeration, and apply must not widen
the read horizon to compensate for an incomplete plan. A candidate set may
depend on persisted state, but its cardinality, cost shape, and validation
evidence must be explicit. Admission must release every lifecycle lock before
waiting for the proposal future and must never retain one through FSM
application.

The minimum evidence is a deterministic matrix that compares the declared
keys, proposal bytes, and durable deletion set for the same request with
unrelated persisted rows added. Add separate rows for type transitions and
for a delayed or cancelled proposal. If the candidate set or cost changes,
record the trigger and cardinality that explain it. A functional cleanup test
alone does not prove that the enumeration cost or lock lifetime is safe.

## Hot-path capability proof

Do not stop at names such as `WriteSession` or `Scope`. Inspect concrete fields,
embedded types, interfaces, closures, callbacks, iterators, error paths, and
technical-update dispatch reachable from apply. The transition may write a
Pebble batch, but it must not call `Get`, `NewIter`, or another node-local read
source to decide the committed result.

Likewise, direct access to a registry, raw `WriteSet`, derived parent,
`KeyStore`, cache iterator, or retained accessor is a potential coverage bypass.
Report it only after tracing a production-reachable call and showing an
undeclared value can influence `T`. Test-only helpers and recovery/admission
readers are not violations merely because they use the same lower-level types.

The write side is capability-restricted as well. Inventory every
`OpenWriteSession` call site and establish its lifecycle owner, batch boundary,
close/error paths, and repository allowlist status. Apply handlers may use the
existing scoped write path, but must not open or retain their own main-store
session. A new call site is in scope when it expands apply-reachable capability
without both a concrete lifecycle justification and the repository's static
enforcement; a separately owned recovery or synchronization writer is not an
apply violation merely because it uses the same DAL API.

## Accepted-order immutability and failure prefixes

Capture `processing.MarshalOrderBusinessIntent` before and after conversion,
coverage enrichment, planning, apply, skip/rollback, audit construction, and
response construction. Compare bytes and ownership of nested protobuf messages,
maps, slices, postings, metadata, and ledger configuration. Technical fields
explicitly excluded from the business-intent hash may change; business payload
may not. An alias is a finding only when a reachable mutation occurs before
audit capture or changes authoritative evidence or behavior.

For rejection, coverage miss, skip, panic/invariant, and merge failure, enumerate
the actual mutation sequence and persisted prefix. State which temporary
overlay changes are discarded and which audit/sequence/idempotency effects are
contractual. A silent `return nil` or `continue` is not automatically wrong;
prove that its branch is contractually impossible or that it leaves a divergent
or falsely successful result. Assertions must uniquely identify the intended
failure branch.

## Numscript VM execution state

A scripted order executes the compiled artifact admission bound to its
`OrderTechnical` (see
[numscript-library.md](../architecture/subsystems/scripting/numscript-library.md)).
The FSM keeps two pieces of node-local state across proposals in the
`RequestProcessor`'s `NumscriptCache`: parsed scripts, and one decoded,
verified VM instance per script hash, reused by every later apply of the same
program bytes. Admission owns a separate cache instance; it never shares the
FSM's warm VMs, nor the reverse — admission's instance exposes, from its
existing parsed-script side, whether it had compiled a script before this
proposal (`CompiledScript.AlreadyCompiled`, backed by
`lruEntry.compileParsed`, service protocol revision 24), consulted only by
admission to send the bytecode by value or by reference; the FSM learns of
that decision only through the committed shape.

**Inputs.** The committed inputs of one scripted execution are the resolved
script text, the artifact's program bytes or program hash, vars bytes and
script hash, the order's `force` flag, and the balances and metadata read
through the gated `Scope`. Everything else is incidental: the cache size
(node-local `NumscriptCacheSize`), LRU residency and eviction, whether the
entry is cold or warm, which compiled bytes for the same script hash were
cached first, the verification record produced by an earlier order's vars,
and the registers and run state an earlier run left in the instance.

**Ownership.** This domain owns the equivalence of the transition across those
incidental states and the lifetime of what a cached instance retains: after
every exit — success, error, or recovered panic — the instance must not keep
the run's store, and through it the apply `Scope` and the proposal's coverage
plan, reachable. The Numscript library releases its store when `Exec` returns;
the ledger pins that contract with a regression test. Numscript arithmetic and
semantic equivalence with direct postings belong to `accounting-invariants`;
replaying audited orders, which never carry an artifact, belongs to
`persistence-restore-replay` and the checker.

**Artifact presence.** An absent artifact — program, program hash, vars, and
script hash all absent — is recompiled from the script text, with an
Antithesis `assert.Unreachable` outside audit replay that never feeds the
outcome. A program by reference — `compiled_program_hash` in place of the
bytes, next to a present vars and script hash — is not an admission bug but
admission deliberately sending the bytecode once per script per instance
(`CompiledScript.AlreadyCompiled`, backed by `lruEntry.compileParsed` on
admission's own cache instance: a signal about what that instance has sent,
never about what any replica's apply-side cache holds — admission and the
FSM apply path each construct their own `NumscriptCache` and share no state;
service protocol revision 24). The FSM runs a committed artifact when its own
library can use it, and otherwise derives program and vars from the script
text with that library (`numscript.SafeExecCommitted`): by value, when the
library reads the bytecode version; by reference, when bytes with the
committed program hash are at hand — its own cache entry when that entry's
bytes have the hash, otherwise its own compile of the text when that
reproduces the hash, then cached. The committed vars run only against bytes
with the committed hash: the hash is what makes the program/vars pairing a
checked property. An unreadable version or an irreproducible hash means
another library version produced the artifact (a replica mid rolling upgrade)
and is never a failure: the replica derives program and vars from the text,
the path audit replay (`persistence-restore-replay`) takes for every order,
and cross-version agreement rests on the library keeping script semantics
stable. Any other combination of the four fields, a half whose header does
not parse (truncated or with a bad magic, whatever bytecode version the other
half carries — both headers are inspected before the version decides
anything, and in the producer next to the shape classification, ahead of the
stale-inputs re-resolution), and a program the library reads but which fails decoding,
verification, or the script-hash binding, fail the order with
`ErrNumscriptRuntime` identically on every replica running the binary — a
wrong shape before any cache access. Repairing such a corrupt program from
the text is a finding: it would let corrupt committed bytes and a correct
replica's failure diverge. So is running committed vars against bytes whose
hash was not checked against the committed one, and so is failing an order
because the artifact came from another library version.

The script hash is XXH3-128 and is not collision-resistant against chosen
inputs. A crafted collision is not a finding here while write scopes are
cluster-wide (see `authentication-authorization-boundaries`); an accidental
collision, or a path where two different texts sharing a hash yield different
results for honest orders, still is.

**Equivalence scenarios.** For the same committed artifact, compare the
complete result — postings, metadata, error reason, audit bytes — across:

1. a cold entry (decode and verify) and a warm hit;
2. a cache of size 1 with interleaved scripts forcing eviction between runs;
3. an entry holding other compiled bytes for the same script hash, which must
   be replaced so the node runs the committed bytes;
4. an instance left dirty by a missing-funds failure and by a recovered store
   panic;
5. orders of one script with different vars, sharing one verification record;
6. a missing artifact recompiled in audit replay against the same order applied
   with its artifact in the cluster;
7. a program by reference: a replica whose cache holds bytes with the
   committed hash (serves them directly) against one that misses and
   recompiles from the text, and against one whose entry holds other bytes
   for the same script hash (replaced by the recompile);
8. an artifact this library cannot use — a bytecode version it cannot read,
   or a reference its compiler does not reproduce — derived from the text
   with the business script vars, against the same order applied with a
   usable artifact: equal under one library version, and never a failure.

The focused entry points are `TestSafeExecCompiled_*`,
`TestSafeExecCommitted_*` and `TestProduce_*` (artifact presence and shape,
warm reuse, panic recovery, same-script-different-bytes, source release,
by-reference cache hit and recompile, foreign version and irreproducible
reference derived from the text). A finding needs a concrete pair of
incidental histories that yields a different component of `T`; a slower cold
path or a cache miss is not one.

## Reachability, evidence, and rejection

Each finding must provide:

1. exact proposal/order/technical-update entry and production dispatch path;
2. equivalent committed pre-state plus the local difference being varied;
3. the first branch, read, iteration, alias, or capability that observes it;
4. exact mutation order and the differing component of `T`;
5. existing guards and why they do not reject or normalize the state;
6. a deterministic reproduction or executable test plan and the test gap.

Reject or downgrade a hypothesis when the path is test-only, admission-only,
unreachable under protobuf/plan validation, normalized before any authoritative
effect, or requires different committed inputs. Missing tests, broad type
capabilities, stale comments, possible map iteration, or theoretically
different cache contents are investigation leads, not P0/P1/P2 evidence.
Conflicting or absent intended behavior becomes an audit question.

## Severity

Use the native P0–P3 scale with demonstrated impact:

| Priority | Domain application |
| --- | --- |
| P0 | Reachable replica or replay divergence that can corrupt or permanently fork authoritative ledger/audit state, or an equivalent catastrophic integrity failure. |
| P1 | High-impact deterministic failure of committed apply, broad cache/audit corruption, or loss of an essential enforcement boundary with a concrete reachable path. |
| P2 | Bounded operation rejection, partial-effect, or cache/coverage correctness failure with material business or recovery impact but no established cluster-wide corruption. |
| P3 | Minor correctness impact supported by evidence; style, optional hardening, and performance-only suggestions remain excluded. |

The audit domain is P0 because its worst credible failure is replica divergence;
individual findings still use demonstrated severity. Do not label every coverage
miss or cache bug P0. P0/P1/P2 require the native concrete-path evidence
standard, and every first-pass finding remains a hypothesis until challenged.

## Ownership and deduplication

One finding represents one root cause and required correction even when it has
several operation types or cache symptoms. Use stable IDs
`fsm-determinism-cache-coverage/<root-cause-name>` without line numbers,
severity, run identifiers, or changing symptom counts.

| Adjacent domain | Boundary |
| --- | --- |
| `accounting-invariants` | Owns conservation, amount ranges, balance/volume truth, transaction atomicity, and Numscript/direct-command semantic equivalence. This domain owns whether apply obtains and uses its inputs identically; deduplicate by root cause and correction. |
| `raft-membership-leadership` | Owns quorum, log commitment, membership, leadership, transport, and snapshot-install ordering. This domain assumes a committed proposal/index and owns pure transition equivalence and cache convergence. |
| `persistence-restore-replay` | Owns checkpoint, durable layout, export/import, and reconstruction correctness. This domain owns replay transition equivalence and the next apply's tolerance of valid reconstructed cache state. |
| `read-consistency-projections` | Owns served-index barriers, projection convergence, rebuild, and pagination. This domain ends at authoritative apply/cache state, though projection mismatches may be evidence of it. |
| `configuration-startup-contracts` | Owns configuration resolution, validation, persisted identity/schema, and wiring. This domain owns the strict prohibition on node-local configuration influencing committed apply. |
| `idempotency-retries-partial-failures` | Owns logical request identity and retry/unknown-outcome semantics. This domain owns deterministic application of the committed idempotency state and proposal. |
| `integrity-verifier-soundness` | Owns whether checker comparisons can detect corruption. Checker evidence does not replace this domain's apply equivalence proof. |
| `concurrency-lifecycle-shutdown` | Owns goroutine lifetime, cancellation, race, and shutdown behavior. This domain owns scheduling dependence only when it changes the committed transition. |
| `api-boundary-contracts` and `authentication-authorization-boundaries` | Own wire conversion, public errors, presence, protocol admission, authentication, and permission. This domain begins after committed proposal bytes are fixed. |
| `test-reachability-enforcement` | Owns whether tests/models are collected and enforced by CI. Test gaps here support runtime findings but are not separate coverage-only findings. |

Follow calls outside manifest paths only when necessary to prove the apply/cache
failure, loading the relevant authoritative contract. If the root cause and
required correction are entirely in a neighboring domain, record the boundary
or residual risk rather than duplicating its finding.

## Contract limits

- Admission may use node-local policy to reject before proposal. Different
  admission decisions across leaders are not apply divergence.
- Cache contents need not be byte-identical when the architecture declares a
  semantically irrelevant representation difference; every read result and
  subsequent deterministic transition must still agree.
- Apply-time writes to a `dal.WriteSession` are allowed. Apply-time reads from
  the main store are not.
- Coverage misses are documented deterministic business rejections of malformed
  plans, not proof that replicas diverged. Investigate their producer and
  mutation ordering before assigning severity.
- The domain does not require backward compatibility between unreleased v3
  formats or mixed development revisions. Compare binaries only within a
  documented supported rolling-upgrade contract.
- Dynamic scenarios are optional evidence strategies, not authorization to edit
  tracked code, operate user infrastructure, or launch the audit from inside an
  audit provider.

## Execution and publication

After this manifest has been reviewed and merged, the trusted outer workflow
may run:

```bash
bash scripts/ai-audit fsm-determinism-cache-coverage
bash scripts/ai-audit-challenge <audit-result.json>
```

Use the same clean exact `HEAD` for both passes. Raw and qualified artifacts
remain under ignored `build/ai-audit/`. Inspect the qualified report before
reporting confirmed findings. Jira publication is a separate explicitly
authorized action and is not part of this manifest PR.

## EN-2479 audit-key transition

Treat the 32-byte audit key as ordered Raft input. The leader's `crypto/rand`
call is outside apply; `TechnicalUpdate_AuditKey` carries bytes that each FSM
installs once. Prove two replicas receive identical bytes and hashes at the
same indexes, a second initialization cannot rotate the key, and orders cannot
commit before the key is durable. Cluster ID is node-local operational identity
and must not enter audit hashing. A retrying leader may generate a different
candidate; only the first committed value is authoritative. The adjacent
persistence audit owns checkpoint and cross-cluster restore parity.

## Atomic creation metadata (EN-2686)

CreateLedger stages initial metadata in the canonical ledger-metadata keyspace in the same proposal as ledger identity, boundaries and initial schema. Committed metadata limits apply before staging; creation metadata requires no additional preload read and cannot mutate the audited order.

For normal and mirror creation, do rejection, proposal rollback and idempotent replay preserve initial metadata atomically without a second save, new metadata read dependency, duplicate allocation or order mutation?

See [the creation contract](../architecture/subsystems/api/atomic-ledger-creation.md) for the authorized semantics and regression evidence. Treat HTTP response/forwarding, actual FSM readback, keyed replay, nonempty checkpoint-plus-delta restore, and primary-projection tampering as separate evidence oracles; a helper-only test does not prove every boundary.
