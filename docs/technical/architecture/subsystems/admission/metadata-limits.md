# Metadata size limits

Ledger bounds the metadata a single command may carry. Without those bounds a
transport-sized payload (HTTP 4 MiB, gRPC 64 MiB) replicates through Raft into
the audit chain, the FSM cache, the primary-store projections and every
downstream event — none of which can drop it afterwards.

The contract is `domain.MetadataLimits` (`internal/domain/metadata_limits.go`).
It is one contract for every entry path: direct HTTP, public gRPC, bulk, mirror
ingest, and the metadata a Numscript program produces.

## The ceilings

| Dimension | Default | Flag | Policy field |
|---|---|---|---|
| Entries per entity | 128 | `--metadata-max-entries` | `metadata_max_entries_per_entity` |
| Key size | 256 B | `--metadata-max-key-bytes` | `metadata_max_key_bytes` |
| Value size | 16 KiB | `--metadata-max-value-bytes` | `metadata_max_value_bytes` |
| Total per entity | 64 KiB | `--metadata-max-entity-bytes` | `metadata_max_entity_bytes` |
| Total per command | 1 MiB | `--metadata-max-command-bytes` | `metadata_max_command_bytes` |

An *entity* is one transaction, one account, or one ledger. A *command* is one
atomic, signed Raft proposal (an `ApplyBatch` or mirror batch), so
the per-command ceiling is what stops a caller from defeating the per-entity
bound by spreading a payload across many entities in one request.

## Scope: per command, not per stored entity

**The ceilings bound what one command carries or produces. They do not bound an
entity's accumulated stored metadata.** 128 entries per command, repeated across
N commands, still accumulates.

This is deliberate. Bounding the accumulated total would require one of:

- reading the entity's whole metadata namespace during apply — no proposal's
  declared `plan.Coverage` authorises that, and widening the read horizon inside
  apply is forbidden (invariant #6); or
- a new persisted per-entity size counter — a primary-store projection, which
  needs checker verification (invariant #8) and an incremental-restore
  classification (invariant #11).

Either is a larger change than the risk this contract addresses, which is what a
*single accepted write* can replicate. Treat the accumulation gap as known and
intentional; closing it is separate work.

## Measurement

`domain.MetadataEntrySize` is the single measurement rule: the key's UTF-8 bytes
plus the value's measured bytes.

| Value variant | Measured as |
|---|---|
| `string_value` | its byte length |
| `null_value` | the byte length of `original` (the text that failed to coerce) |
| `int_value`, `uint_value`, `datetime_value` | 8 bytes |
| `bool_value` | 1 byte |
| absent / unknown | 0 bytes |

These are accounting weights, not wire sizes. A fixed weight per variant keeps
the measurement a pure function of the value, which is what makes the FSM-side
check identical on every node and stable across protobuf encoding changes.

Admission and FSM apply both call this function, so the two layers can never
disagree about whether the same payload fits.

## Where the limits are enforced

**Admission** — `validateCommandMetadata`
(`internal/application/admission/validate_order.go`), called from `Admit` once
the orders exist. It walks every metadata-bearing order shape through
`domain.WalkOrderMetadata` (`internal/domain/order_metadata.go`), the single source of truth for *where* metadata lives in an
order; the shape validation, the size validation and the byte accounting all
traverse it, so they cannot drift apart. HTTP, public gRPC and bulk converge on
`requestsToOrders`. Mirror workers use a separate proposal path.

**Mirror ingestion** — `Worker.processBatch`
(`internal/application/mirror/worker.go`) checks the translated and rewritten
orders against the committed cluster policy before building or proposing the
batch. `domain.ValidateCommandMetadata` checks metadata shape, per-entity ceilings
and the total across all orders through the shared walker. Admission uses the
same helper, including deterministic account and key error selection. This includes external HTTP and PostgreSQL
sources, which need not have enforced the destination's limits.

`processMirrorIngest` rechecks the order and proposal-wide byte budget against
the FSM's committed policy before mutating state. This protects against a policy
change between the worker's check and apply. Already-applied source entries
remain idempotent no-ops. A rejected batch does not advance the durable mirror
cursor; the worker reports the error and retries. Reduce the source batch size
for aggregate overflow, or correct the source/rewrite output or raise the
replicated limits for an oversized individual entry.

**FSM apply** — `processCreateTransaction`
(`internal/domain/processing/processor_transaction.go`) re-checks the *merged*
transaction and per-account maps. Admission cannot do this: the union of caller
metadata and `set_tx_meta` / `set_account_meta` output only exists after the
script runs, and two individually-legal halves can exceed the entity ceiling
together. The FSM reads the ceilings from the committed policy through the Scope
— never from node-local configuration, which would make one committed entry
apply differently per node (invariant #2).

Direct account, transaction and ledger metadata saves, and transaction
reversals, recheck non-empty caller maps against the current committed policy
before changing metadata, balances or reversal state. They also validate the
proposal-wide budget, so a policy tightened between admission and apply cannot
be bypassed by spreading caller metadata across several orders. Empty maps
store no metadata and contribute no bytes. Existing transaction-not-found and
already-reverted checks still precede metadata validation for reversals.

Metadata deletion and field-type set/remove orders recheck their bare key and
the same proposal-wide budget against the committed policy before mutation.
This includes account, transaction and ledger metadata deletion. The limit check precedes
key-existence checks so a skippable `METADATA_NOT_FOUND` outcome cannot bypass
the ceiling. A batch containing only bare keys therefore cannot bypass a tighter
key or command ceiling committed after admission.

`ProcessOrders` seeds a proposal-wide budget with every order's input metadata,
including bare keys in deletion and schema orders. Each transaction replaces
its input contribution with the merged transaction and account maps, then
checks `ValidateCommandBytes`. This includes earlier scripts' output and later
orders' input, so splitting generated metadata across accounts or orders cannot
bypass the command ceiling. Caller values win collisions and are counted once.
The budget is local to one proposal; a rejected proposal discards its staged
writes through the existing FSM rollback path.

Both layers reject deterministically. `ValidateMap` checks the entry count and
the entity total before the per-entry rules, and reports the lexicographically
smallest offending key rather than whichever one Go's randomised map iteration
reaches first; `validateMergedAccountMetadata` does the same across accounts.
Without that, one committed entry could reject with different messages on
different nodes — and the rejection is hash-bound into the audit chain.

## Configuration

The effective ceilings live in the Raft-replicated `common.ClusterPolicy`, not
in node-local flags. FSM apply consults them, so a node-local value would breach
the deterministic-FSM configuration boundary. The flags shape only the policy a
**leader proposes**; `reconcileClusterPolicy` (`internal/bootstrap/module.go`)
turns them into an audited `SetClusterPolicy` order.

Because the reconciler proposes only when the desired revision exceeds the
applied one, **changing a ceiling requires bumping `--cluster-policy-revision`**,
exactly like any other policy field. Setting a new ceiling without bumping the
revision logs a payload-divergence error and changes nothing.

`Discovery.cluster_policy` exposes the committed policy, including its revision
and effective metadata ceilings. It is absent before policy initialization; a
client must not substitute the server's built-in defaults for an absent policy.
This read is local to the serving replica and can briefly lag a newly committed
revision until that replica applies it. A connector should derive its budgets
from the returned policy and still handle a typed limit rejection if the policy
changes between discovery and its write. This read does not alter admission or
the deterministic FSM limit checks.

EN-2105's connector audit found 14 budget or state-capacity cases, but they do
not establish a common higher value or entity default. In particular, the
current cursor is a verbose JSON envelope stored as one base64 metadata value;
compression or a compact encoding can recover more than the proposed fourfold
cursor increase for measured shapes. Connector page splitting, generated
metadata accounting and genuinely indivisible evidence need separate
qualification. The existing 1 MiB command ceiling from #2081 remains the
replication bound; raising value or entity ceilings would also increase Raft,
audit, cache and downstream memory exposure for every accepted write.

### EN-2105 RC qualification

The [September 17 connector inventory](https://github.com/formancehq/connectivity-plugins-poc/issues/628)
is a failure snapshot, not a current fleet-wide maximum or a proof that a
larger Ledger policy resolves the failures. Its 14 connector cases route as
follows (bytes are reported plugin estimates, not measured Raft entry sizes):

| Connector | Reported binding case | Remaining qualification |
|---|---:|---|
| Alpaca | page 147,481 / 87,381 B | Byte-aware page delivery and cash/activity checkpoints |
| Banking Bridge | page 101,036 / 87,381 B | Group sizing including generated metadata |
| Braintree | page 402,051 / 87,381 B | Smaller resumable payment groups |
| Fireblocks | page 354,658 / 87,381 B | Identify endpoint and full mapped output size |
| PayPal | page 770,621 / 87,381 B | Bound reporting override and preserve recovery |
| Routable | page 887,232 / 87,381 B | Split rich obligation groups |
| Synthetic | page 87,384 / 87,381 B | Reproduce late-sequence workload; byte-aware generation |
| Unit | page 90,254 / 87,381 B | Journal/reservation delivery and checkpoints |
| AWS Costs | one contributing row over 16 KiB | Qualify complete indivisible evidence separately |
| Bank CAMT | observation state over 6,144 B | Reconstruct lifecycle/correlation state |
| Formance Payments | observation state over 6,144 B | Preserve payment and identity state compactly |
| Bridge.xyz | raw cursor 12,409 / 12,288 B | Compact the envelope; measure complete cursor |
| Stripe Connect | in-flight worklist limit 5,000 | Correct hydration classification and handoff |
| Currencycloud | in-flight worklist limit 5,000 | Prove update-window recovery before handoff |

The reported page allowance of 87,381 B was a plugin estimate derived from
the former 256 KiB Ledger command default, and is not a Ledger ceiling. The
existing 1 MiB command default is committed only after a policy revision bump;
the 16 KiB value and 64 KiB entity defaults remain. The new Discovery field
adds a small read response, with no additional proposal, audit, FSM cache or
downstream event bytes. No representative post-optimization connector batch
has been measured through Ledger's Raft/apply/memory path yet, so this change
does not claim a safe larger value or entity ceiling. Before any such increase,
measure the full encoded command and peak apply/cache/event footprint against
both the current and proposed replicated policy using representative mapped
connector output, including generated metadata and the complete cursor.

Zero is never "unlimited" — it is the absence of configuration, and it fails
loudly at three points rather than silently removing the protection:

1. `Config.Validate` rejects a zero or mutually unsatisfiable flag set at boot,
   naming the offending flags.
2. `validateCommittedMetadataLimits` refuses to boot against a *committed* policy
   with no ceilings when `--cluster-policy-revision` cannot supersede it — the
   one case the reconciler provably cannot repair. It is not bypassable with
   `--unsafe-skip-config-validation`: the policy check runs before a forced
   identity override can persist the new identity.
3. `processSetClusterPolicy` refuses to commit such a policy at all.

The ceilings must also be mutually satisfiable: `key ≤ entity`,
`value ≤ entity`, `entity ≤ command`. A wider contradictory ceiling is
unreachable, so an operator raising it would observe no effect; both
`Config.Validate` and the FSM reject the combination instead. Both use
the shared `MetadataLimits.Validate` helper for configuration diagnostics.

### Kubernetes operator

When running via the Kubernetes operator the six cluster-policy fields are
exposed as typed fields on the `ClusterSpec` CRD instead of `spec.extraEnv`.
The fields map directly to their CLI flags through the uppercase-with-underscores
convention (e.g. `spec.metadataMaxCommandBytes` sets `METADATA_MAX_COMMAND_BYTES`).

| CRD field | CLI flag | Default |
|---|---|---|
| `spec.clusterPolicyRevision` | `--cluster-policy-revision` | *(must be set explicitly)* |
| `spec.metadataMaxEntries` | `--metadata-max-entries` | 128 |
| `spec.metadataMaxKeyBytes` | `--metadata-max-key-bytes` | 256 |
| `spec.metadataMaxValueBytes` | `--metadata-max-value-bytes` | 16384 (16 KiB) |
| `spec.metadataMaxEntityBytes` | `--metadata-max-entity-bytes` | 65536 (64 KiB) |
| `spec.metadataMaxCommandBytes` | `--metadata-max-command-bytes` | 1048576 (1 MiB) |

All fields are optional; an unset field lets the server apply its built-in
default. When changing any limit, bump `spec.clusterPolicyRevision` above its
current value in the same manifest update — otherwise the server logs a
divergence error and the policy is not updated. The mutual-satisfiability rule
(`key <= entity <= command`) is validated at boot and rejected by the FSM;
the CRD webhook does not enforce it independently.

## Client contract

A violation returns the typed `domain.ErrMetadataLimitExceeded`:

- gRPC `InvalidArgument`, HTTP 400;
- `ErrorInfo.reason` = `METADATA_LIMIT_EXCEEDED`;
- `ErrorInfo.metadata` carries `dimension` (`entries`, `key`, `value`, `entity`
  or `command`), `limit` and `actual`, so a client can tell "too many entries"
  from "one value too large" without parsing the message.

`Validation` — not `ResourceExhausted` — because the caller must send less: the
rejection is permanent, and a retryable code would make client retry policies
re-drive it. An FSM-side rejection is freezable, so a keyed retry replays it from
the audit chain instead of re-executing.

`openapi.yml` documents the default ceilings in descriptions rather than fixed
`maxProperties` or `maxLength` constraints: the effective limits come from the
replicated cluster policy and operators can raise them. String sizes are measured
in UTF-8 bytes, whereas `maxLength` counts characters. The schema retains the
NUL-byte restriction on metadata strings; the server enforces the configured
entry, key, value, entity, and command ceilings.

## Transport caps are not this contract

The public gRPC plane has its own message cap (`serviceMaxMsgSize`,
`internal/adapter/grpc/server.go`), separate from the internal Raft/snapshot
transport envelope (`transport.GRPCMaxMsgSize`) so the public request contract
can move without changing how peers replicate. HTTP caps bodies at 4 MiB
(`internal/adapter/http/params.go`).

Both are backstops. A transport cap cannot express per-entry or per-entity
bounds, and a request comfortably under one can still be rejected by these
ceilings.
