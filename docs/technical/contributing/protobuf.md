# Protocol Buffers and gRPC

The Raft transport layer and ledger service use gRPC for communication. Protocol buffer definitions are stored in `misc/proto/`, and generated Go code is placed in internal packages.

## File Locations

### Protocol Definitions (`misc/proto/`)

| File | Contents |
|------|----------|
| `raft_transport.proto` | Raft transport messages |
| `common.proto` | Common types (Posting, Transaction, Log, Uint256, etc.) |
| `raft_cmd.proto` | FSM command types (CreateLedger, DeleteLedger, CreateLog, etc.) |
| `bucket.proto` | gRPC service definitions (BucketService), includes mirror sync, promote |
| `cluster.proto` | Cluster management (ClusterService) |
| `snapshot.proto` | Snapshot service definitions |
| `audit.proto` | Audit log messages |
| `signature.proto` | Request signature types |
| `events.proto` | Domain event types |
| `restore.proto` | Restore service |

### Generated Code (`internal/proto/`)

| Package | Contents |
|---------|----------|
| `commonpb/` | Common types |
| `raftcmdpb/` | FSM command types |
| `servicepb/` | gRPC service |
| `clusterpb/` | Cluster state |
| `signaturepb/` | Signature types |
| `snapshotpb/` | Snapshot service |
| `auditpb/` | Audit log types |
| `eventspb/` | Domain event types |
| `restorepb/` | Restore service |

Raft transport generated code lives in `internal/proto/rafttransportpb/` (`raft_transport.pb.go`, `raft_transport_grpc.pb.go`).

## Regenerating Code

```bash
just generate-proto
```

This reads `.proto` files, generates Go code using `protoc-gen-go`, `protoc-gen-go-grpc`, `protoc-gen-go-vtproto`, and the custom plugins under `tools/` — `protoc-gen-dethash`, `protoc-gen-reader`, and `protoc-gen-queryfilter-validity` — and places files according to the `go_package` option.

### Custom plugins

- **`protoc-gen-dethash`** — deterministic (sorted-map) VT marshalers.
- **`protoc-gen-reader`** — read-only interface / wrapper views.
- **`protoc-gen-queryfilter-validity`** — emits `common_queryfilter_validity.pb.go`, the single source of truth for per-target `QueryFilter` condition validity (EN-1504). It reads the `common.allowed_query_targets` field-option extension annotating each arm of the `QueryFilter.filter` oneof with the `QueryTarget`s the condition is valid on, and generates the `ConditionKind` enum, `ConditionKindOf`, and the `ConditionValidForTarget` table. Both `internal/query` (compile + audit compilers) and `internal/adapter/http` (REST decode) consume the generated table, so validity rules cannot drift. To change what a condition is valid on, edit the annotation in `misc/proto/common.proto` and re-run `just generate-proto` — never edit the generated file. **Every oneof arm MUST carry an explicit declaration**: one or more `[(common.allowed_query_targets) = QUERY_TARGET_...]`, or `[(common.valid_on_no_query_target) = true]` for an arm deliberately valid on no target. An arm with neither (a forgotten annotation) makes `just generate-proto` **fail** with a clear message — it is a build error, not a silent all-false row — which is what makes the anti-drift gate real. Declaring both is rejected as contradictory.

### Prerequisites

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@v0.6.1-0.20240319094008-0393e58bdf10
```

## Modifying Protocol Definitions

1. Edit the `.proto` file in `misc/proto/`
2. **Realign field numbers sequentially** when adding/removing fields (no gaps, remove obsolete `reserved` entries)
3. **Audit the hand-rolled wire sites** before renumbering — see below
4. Run `just generate-proto` **immediately**
5. Update Go code that uses the generated types
6. Rebuild: `go build ./...`

### Renumbering: audit the hand-rolled wire sites first

Most code goes through the generated marshallers and follows a renumbering automatically. A few
hot paths bypass them and encode or decode protobuf by hand, and those break **silently** — the
compiler sees nothing, and a wrong field number is not a decode error, just a field that never
matches. Grep **both** directions before you renumber:

```bash
# write side
grep -rn "protowireutil\|AppendTag\|AppendFixed64" --include="*.go" internal/
# read side — easy to miss, a write-side grep will not surface it
grep -rn "protowire.Consume" --include="*.go" internal/
```

The two known sites behave differently, and only one of them is self-healing:

| Site | Direction | Field numbers | Renumbering impact |
|------|-----------|---------------|--------------------|
| `internal/infra/plan/predicted_index.go` | write | read from the descriptor at init | Number self-heals. The **wire type** does not — it is baked into the `AppendFixed64` call, so changing the field's type silently corrupts the encoding. Pinned by `predicted_index_test.go`, which compares the append against canonical marshalling. |
| `internal/application/indexbuilder/protowire_postings.go` | read | **hardcoded literals** (`case num == 1`, …) | Breaks silently. The parser skips the unrecognised field via `ConsumeFieldValue` and yields empty postings — no error, no failing build. |

`protowire_postings.go` decodes `Log`, `LogPayload`, `ApplyLedgerLog`, `LedgerLog`,
`CreatedTransaction`, `RevertedTransaction`, `Transaction`, `TouchedVolume` and `Posting`. If you
renumber any field of those messages, update the corresponding `case num == N` arm in the same
commit.

## vtprotobuf (Fast Serialization)

The project uses [vtprotobuf](https://github.com/planetscale/vtprotobuf) to generate reflection-free protobuf methods (~2-3x faster, fewer allocations).

**Generated methods**: `MarshalVT()`, `UnmarshalVT()`, `SizeVT()`, `CloneVT()`, `EqualVT()`, `ResetVT()`, `ReturnToVTPool()`

The `pool` feature is also enabled (generation command uses `features=marshal+unmarshal+size+clone+equal+pool`), which generates `ResetVT()` for zeroing a message in place and `ReturnToVTPool()` for returning it to a `sync.Pool`, reducing GC pressure on hot paths.

**How it works**:
- `*_vtproto.pb.go` files are generated alongside standard `*.pb.go` files
- Wire format is identical to standard protobuf (no compatibility impact)
- Server-side gRPC codec registered in `internal/adapter/grpc/server.go` via `init()`
- Client (`cmd/ledgerctl/`) uses standard protobuf (no codec import needed)

**Hot-path usage** (direct VT method calls):

| File | Usage |
|------|-------|
| `internal/application/admission/admission.go` | Proposal marshal (`vtmarshal.MarshalCopy`) |
| `internal/infra/state/machine.go` | Proposal unmarshal, snapshot marshal/unmarshal |
| `internal/infra/attributes/attributes.go` | Attribute value marshal/unmarshal |
| `internal/storage/dal/batch.go` | Batch size estimation (`SizeVT`) |
| `internal/storage/dal/store.go` | Value unmarshal (`UnmarshalVT`) |
| `internal/domain/processing/processor.go` | Order hash (`CloneVT`) |
| `internal/infra/state/write_set_counters.go`, `internal/infra/state/registry_derived.go` | Clone functions (`CloneVT` references) |

## Uint256 Wire Format

All monetary amounts use the `Uint256` protobuf message - a fixed-size 4 x `fixed64` representation mapping directly to `holiman/uint256.Int`'s `[4]uint64` layout.

**Why not BigInt (bytes)?**
- Zero allocation: converting between proto and `uint256.Int` is just 4 `uint64` assignments
- All amounts are non-negative: the sign byte in BigInt was wasted
- 2^256 range (1.16 x 10^77) covers any real-world monetary quantity

**Key file**: `internal/proto/commonpb/uint256.go` (`IntoUint256()`, `SetFromUint256()`, `ToBigInt()`, `IsZero()`, `Dec()`)

See [architecture/uint256-wire-format.md](../architecture/primitives/uint256-wire-format.md) for the full design rationale.

## Mirror-Related Proto Types

Mirror mode introduces several protobuf types across multiple files:

**`common.proto`:**
- `LedgerMode` enum — `LEDGER_MODE_NORMAL`, `LEDGER_MODE_MIRROR`
- `MirrorSourceConfig` — oneof with `HttpMirrorSourceConfig` and `PostgresMirrorSourceConfig`
- `LedgerInfo.mode` and `LedgerInfo.mirror_source` fields

**`raft_cmd.proto`:**
- `MirrorIngestOrder` — Raft command to ingest a translated v2 log entry
- `MirrorLogEntry` — Wrapper for a single v2 log entry, including its source date (oneof: `CreatedTransaction`, `SavedMetadata`, `DeletedMetadata`, `RevertedTransaction`, `FillGap`)
- `PromoteLedgerOrder` — Raft command to promote a mirror ledger to normal mode
- `MirrorSyncUpdate` — Streaming update from the mirror worker (progress reporting)

**`bucket.proto`:**
- `CreateLedgerRequest.mode` and `CreateLedgerRequest.mirror_source` fields
- `PromoteLedgerRequest` — gRPC request to promote a mirror ledger

## Adding New Command Models

1. Add the message definition to `misc/proto/raft_cmd.proto`
2. Run `just generate-proto`
3. Use the generic `NewCommand` function in `internal/pkg/commands/command.go`, which accepts variadic `*raftcmdpb.Order` args, to build proposals containing the new order type
4. Command data is unmarshaled via vtprotobuf's generated `UnmarshalVT` on the `Proposal` type, then dispatched through the `Order.Type` oneof in the FSM
5. Add a handler method in `internal/infra/state/machine.go`
6. Rebuild and test: `go build ./... && go test ./...`

### Technical Update Fields (Non-Order Pattern)

Not all FSM operations are modeled as orders. Internal operations that do not produce user-visible log entries are wrapped in `TechnicalUpdate` entries under `Proposal.technical_updates`. Each update has its own coverage bitset and one payload selected by the `TechnicalUpdate.kind` oneof. This avoids polluting the `Order`/`LedgerApplyOrder` oneofs while preserving the same preload-coverage isolation.

Examples of this pattern:
- `MirrorSyncUpdate`
- `EventsSinkUpdate`
- `BackupOrder` / `IncrementalBackupOrder`
- `common.ClusterConfig`

These updates are processed by the FSM alongside orders but do not produce ledger `Log` entries and do not carry client idempotency keys.
