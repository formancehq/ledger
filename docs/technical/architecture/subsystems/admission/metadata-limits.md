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
| Total per command | 256 KiB | `--metadata-max-command-bytes` | `metadata_max_command_bytes` |

An *entity* is one transaction, one account, or one ledger. A *command* is one
`ApplyBatch` — the atomic, signed unit that becomes a single Raft proposal — so
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
traverse it, so they cannot drift apart. All four public entry paths converge on
`requestsToOrders`, so one gate covers them.

**FSM apply** — `processCreateTransaction`
(`internal/domain/processing/processor_transaction.go`) re-checks the *merged*
transaction and per-account maps. Admission cannot do this: the union of caller
metadata and `set_tx_meta` / `set_account_meta` output only exists after the
script runs, and two individually-legal halves can exceed the entity ceiling
together. The FSM reads the ceilings from the committed policy through the Scope
— never from node-local configuration, which would make one committed entry
apply differently per node (invariant #2).

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

Zero is never "unlimited" — it is the absence of configuration, and it fails
loudly at three points rather than silently removing the protection:

1. `Config.Validate` rejects a zero or mutually unsatisfiable flag set at boot,
   naming the offending flags.
2. `validateCommittedMetadataLimits` refuses to boot against a *committed* policy
   with no ceilings when `--cluster-policy-revision` cannot supersede it — the
   one case the reconciler provably cannot repair. It is not bypassable with
   `--unsafe-skip-config-validation`.
3. `processSetClusterPolicy` refuses to commit such a policy at all.

The ceilings must also be mutually satisfiable: `key ≤ entity`,
`value ≤ entity`, `entity ≤ command`. A wider contradictory ceiling is
unreachable, so an operator raising it would observe no effect; both
`Config.Validate` and the FSM reject the combination instead. Both use
the shared `MetadataLimits.Validate` helper for configuration diagnostics.

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
