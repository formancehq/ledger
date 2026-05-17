# Maintenance Mode

The ledger supports a **cluster-wide maintenance mode** that blocks all write operations while allowing reads to continue. This is useful for planned maintenance, upgrades, or emergency situations.

## Quick Start

```bash
# Enable maintenance mode
ledgerctl cluster maintenance enable

# Disable maintenance mode
ledgerctl cluster maintenance disable

# With request signing
ledgerctl cluster maintenance enable --signing-key /path/to/seed
```

## Behavior

### Allowed Operations in Maintenance Mode

- **Blocked**: All write operations (create ledger, create transaction, save metadata, delete ledger, etc.)
- **Allowed**: `SetMaintenanceMode` requests (to disable maintenance mode)
- **Allowed**: All read operations (get ledger, list accounts, get transaction, cluster status, etc.)

### Error Responses

When a write operation is attempted during maintenance mode:
- **gRPC**: `codes.Unavailable`
- **HTTP**: `503 Service Unavailable`

## Architecture

### Persistence and Replication

- The maintenance mode flag is stored in Pebble (key prefix `0x0B`) and cached in-memory in `SharedState`
- The flag is replicated through Raft consensus (same path as signing config)
- Changes take effect when the FSM applies the corresponding log entry

### Dual Enforcement

Write operations are rejected at **two levels** for safety:

1. **Admission layer**: Rejects write requests before they enter the Raft pipeline. This is the primary gate and prevents unnecessary Raft round-trips.

2. **FSM level**: Rejects proposals containing non-maintenance orders during `applyProposal()`. This catches the Raft batching race condition where entries admitted before maintenance mode was enabled can be batched into a Raft entry applied after the maintenance mode flag is set by a preceding entry in the same batch.

### Data Flow

```
Client -> gRPC Apply() -> Admission (check maintenance mode) -> Proposal -> Raft consensus
  -> FSM Apply (check maintenance mode again) -> WriteSet.Merge() -> Pebble + KeyStore
```

## Key Files

| File | Role |
|------|------|
| `internal/infra/state/shared_state.go` | In-memory flag (`MaintenanceMode()`, `SetMaintenanceMode()`) |
| `internal/query/config.go` | Pebble persistence (`ReadMaintenanceMode()`) |
| `internal/infra/state/batch.go` | Pebble write (`SaveMaintenanceMode()`) |
| `internal/application/admission/admission.go` | Admission-level check |
| `internal/infra/state/machine.go` | FSM-level check (`authorizedInMaintenanceMode`) |
| `internal/infra/state/write_set.go` | Atomic merge (persist + update SharedState) |
| `internal/domain/processing/processor.go` | `processSetMaintenanceMode()` |
| `internal/domain/errors.go` | `ErrMaintenanceMode` sentinel error |
| `cmd/ledgerctl/cluster/maintenance.go` | CLI command |
