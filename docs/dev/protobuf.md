# Protocol Buffers and gRPC

The Raft transport layer and ledger service use gRPC for communication. Protocol buffer definitions are stored in `misc/proto/`, and generated Go code is placed in internal packages.

## File Locations

### Protocol Definitions (`misc/proto/`)

| File | Contents |
|------|----------|
| `raft_transport.proto` | Raft transport messages |
| `common.proto` | Common types (Posting, Transaction, Log, Uint256, etc.) |
| `raftcmd.proto` | FSM command types (CreateLedger, DeleteLedger, CreateLog, etc.) |
| `service.proto` | gRPC service definitions (LedgerService) |
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

This reads `.proto` files, generates Go code using `protoc-gen-go`, `protoc-gen-go-grpc`, and `protoc-gen-go-vtproto`, and places files according to the `go_package` option.

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

**Generated methods**: `MarshalVT()`, `UnmarshalVT()`, `SizeVT()`, `CloneVT()`, `EqualVT()`

**How it works**:
- `*_vtproto.pb.go` files are generated alongside standard `*.pb.go` files
- Wire format is identical to standard protobuf (no compatibility impact)
- Server-side gRPC codec registered in `internal/application/grpc_server.go` via `init()`
- Client (`cmd/ledgerctl/`) uses standard protobuf (no codec import needed)

**Hot-path usage** (direct VT method calls):

| File | Usage |
|------|-------|
| `internal/service/admission/admission.go` | Proposal marshal (`SizeVT` + `MarshalToVT`) |
| `internal/service/state/machine.go` | Proposal unmarshal, snapshot marshal/unmarshal |
| `internal/service/attributes/attributes.go` | Attribute value marshal/unmarshal |
| `internal/storage/dal/batch.go` | Batch marshal |
| `internal/service/processing/processor.go` | Order hash (`CloneVT` + `MarshalVT`) |
| `internal/service/state/buffer.go` | Clone functions (`CloneVT` references) |

## Uint256 Wire Format

All monetary amounts use the `Uint256` protobuf message - a fixed-size 4 x `fixed64` representation mapping directly to `holiman/uint256.Int`'s `[4]uint64` layout.

**Why not BigInt (bytes)?**
- Zero allocation: converting between proto and `uint256.Int` is just 4 `uint64` assignments
- All amounts are non-negative: the sign byte in BigInt was wasted
- 2^256 range (1.16 x 10^77) covers any real-world monetary quantity

**Key file**: `internal/proto/commonpb/uint256.go` (`IntoUint256()`, `SetFromUint256()`, `ToBigInt()`, `IsZero()`, `Dec()`)

See [architecture/uint256-wire-format.md](./architecture/uint256-wire-format.md) for the full design rationale.

## Adding New Command Models

1. Add the message definition to `misc/proto/raftcmd.proto`
2. Run `just generate-proto`
3. Create a `NewXxxCommand` function in `internal/service/commands/command.go`
4. Update `UnmarshalCommandData` in `internal/service/commands/command.go`
5. Add a handler method in `internal/service/state/machine.go`
6. Rebuild and test: `go build ./... && go test ./...`
