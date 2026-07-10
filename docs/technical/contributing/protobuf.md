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
- **`protoc-gen-queryfilter-validity`** — emits `common_queryfilter_validity.pb.go`, the single source of truth for per-target `QueryFilter` condition validity (EN-1504). It reads the `common.allowed_query_targets` field-option extension annotating each arm of the `QueryFilter.filter` oneof with the `QueryTarget`s the condition is valid on, and generates the `ConditionKind` enum, `ConditionKindOf`, and the `ConditionValidForTarget` table. Both `internal/query` (compile + audit compilers) and `internal/adapter/http` (REST decode) consume the generated table, so validity rules cannot drift. To change what a condition is valid on, edit the annotation in `misc/proto/common.proto` and re-run `just generate-proto` — never edit the generated file. An arm left unannotated maps to "valid on no target" (the fail-safe default), so a forgotten annotation rejects the condition everywhere rather than silently widening results.

### Prerequisites

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@v0.6.1-0.20240319094008-0393e58bdf10
```

## Modifying Protocol Definitions

1. Edit the `.proto` file in `misc/proto/`
2. **Realign field numbers sequentially** when adding/removing fields (no gaps, remove obsolete `reserved` entries)
3. Run `just generate-proto` **immediately**
4. Update Go code that uses the generated types
5. Rebuild: `go build ./...`

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
- `MirrorLogEntry` — Wrapper for a single v2 log entry (oneof: `CreatedTransaction`, `SavedMetadata`, `DeletedMetadata`, `RevertedTransaction`, `FillGap`)
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

### Technical Proposal Fields (Non-Order Pattern)

Not all FSM operations are modeled as orders. Internal, background operations that do not produce log entries can be added as **direct repeated fields on the `Proposal` message** instead. This avoids polluting the `Order`/`LedgerApplyOrder` oneofs with types that have no user-visible log semantics.

Examples of this pattern:
- `repeated MetadataConversionBatch metadata_conversion_batches` -- background metadata value conversion batches
- `repeated MetadataConversionCompletion metadata_conversions_complete` -- signals that a metadata conversion is done
- `repeated IndexReadyUpdate index_ready_updates` -- signals from the index builder that a new index is queryable

These fields are processed by the FSM alongside orders but do not produce `Log` entries and do not carry idempotency keys.
