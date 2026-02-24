# Numscript Library

The numscript library is a global repository for storing, retrieving, and managing reusable numscript programs with semantic versioning. Scripts stored in the library can be referenced when creating transactions, avoiding the need to inline the script content in every request.

## Concepts

### Versioning Model

Each numscript is identified by a **name** (e.g. `payment-with-fees`) and can have multiple **versions**:

| Version type | Format | Mutability | Example |
|---|---|---|---|
| **Latest** | `"latest"` (or empty) | Overwritable | Always points to the most recently saved "latest" content |
| **Semver** | `major.minor.patch` | Immutable | `1.0.0`, `2.3.1` |

Key rules:
- **"latest" is its own version slot**, independent from semver versions. Saving to "latest" does not overwrite any semver version, and vice versa.
- **Semver versions are immutable.** Once `payment-with-fees` v`1.0.0` is created, it cannot be overwritten. Attempting to do so returns `NUMSCRIPT_VERSION_ALREADY_EXISTS`.
- **Saving with an empty version** defaults to the `"latest"` slot.
- **Deletion is a soft delete.** Deleting a numscript clears its latest-version pointer but preserves all versioned entries in storage. Explicit version lookups (e.g. `?version=1.0.0`) continue to work after deletion.

### Syntax Validation

Scripts are parsed and validated **at save time**, not at transaction creation time. This catches syntax errors early. The parser uses the same Numscript interpreter as transaction execution, with all experimental features enabled.

## API

### HTTP Endpoints

| Method | Path | Description |
|---|---|---|
| `PUT` | `/numscripts/{name}` | Save a numscript (create or update latest) |
| `GET` | `/numscripts/{name}?version=` | Get a numscript (empty version = latest) |
| `GET` | `/numscripts` | List all numscripts (latest version of each) |
| `DELETE` | `/numscripts/{name}` | Soft-delete a numscript |

#### Save Numscript

```
PUT /numscripts/payment-with-fees
Content-Type: application/json

{
  "content": "vars { monetary $amount } send $amount ( source = @treasury destination = @merchant )",
  "version": "1.0.0"
}
```

Response: `201 Created` with a log entry containing the `NumscriptInfo`.

#### Get Numscript

```
GET /numscripts/payment-with-fees?version=1.0.0
```

Response: `200 OK` with `NumscriptInfo` (name, content, version, createdAt).
Returns `404` if the numscript or version does not exist.

#### List Numscripts

```
GET /numscripts
```

Response: `200 OK` with an array of `NumscriptInfo` (latest version of each script).

#### Delete Numscript

```
DELETE /numscripts/payment-with-fees
```

Response: `204 No Content`. Returns `404` if the numscript does not exist.

### gRPC

The `BucketService` exposes:
- `SaveNumscript` / `DeleteNumscript` via the `Apply` RPC (write operations go through Raft)
- `GetNumscript(GetNumscriptRequest) returns (NumscriptInfo)` (read)
- `ListNumscripts(ListNumscriptsRequest) returns (stream NumscriptInfo)` (read, streaming)

### CLI

```bash
# Save from a file
ledgerctl numscripts save payment-with-fees --file script.num --version 1.0.0

# Save from stdin
cat script.num | ledgerctl numscripts save payment-with-fees

# Get latest version
ledgerctl numscripts get payment-with-fees

# Get specific version
ledgerctl numscripts get payment-with-fees --version 1.0.0

# List all
ledgerctl numscripts list

# Delete
ledgerctl numscripts delete payment-with-fees
```

## Error Handling

| Error | Reason code | HTTP | gRPC | When |
|---|---|---|---|---|
| Name required | — | 400 | INVALID_ARGUMENT | Empty name |
| Content required | — | 400 | INVALID_ARGUMENT | Empty content |
| Parse error | `NUMSCRIPT_PARSE_ERROR` | 400 | INVALID_ARGUMENT | Invalid Numscript syntax |
| Invalid version | `NUMSCRIPT_INVALID_VERSION` | 400 | INVALID_ARGUMENT | Version is not "latest", empty, or valid semver |
| Version exists | `NUMSCRIPT_VERSION_ALREADY_EXISTS` | 409 | ALREADY_EXISTS | Immutable semver already saved |
| Not found | `NUMSCRIPT_NOT_FOUND` | 404 | NOT_FOUND | Get/delete non-existent numscript |

## Storage

### Pebble Keys

Two key prefixes store numscript data:

| Prefix | Key format | Value |
|---|---|---|
| `KeyPrefixNumscript` | `[prefix][name]\x00[version]` | Protobuf-encoded `NumscriptInfo` |
| `KeyPrefixNumscriptLatest` | `[prefix][name]` | Version string (UTF-8 bytes) |

The latest-version pointer is a simple string that tells the system which version to resolve when no explicit version is requested. On soft delete, this key is written with an empty value (not deleted from Pebble), preserving the key's existence for the storage layer.

### Attribute Caches

Numscript data uses the same preloading pattern as other system attributes (see [Deterministic FSM](./deterministic-fsm.md)):

| Cache | Key type | Value type | Purpose |
|---|---|---|---|
| `NumscriptVersions` | `NumscriptVersionKey{Name}` | `string` | Latest version pointer per name |
| `NumscriptEntries` | `NumscriptEntryKey{Name, Version}` | `bool` | Per-version existence (for semver immutability checks) |

Both use `KeyStore` (Machine) + `DerivedKeyStore` (Buffered) + `AttributeCache` (Cache) + `AttributeLoader` (Admission).

### Admission Preloading

Before a `SaveNumscript` or `DeleteNumscript` order enters Raft, the admission layer preloads:

1. **NumscriptVersions** — the current latest-version pointer for the script name. Always preloaded (both existing and empty values).
2. **NumscriptEntries** — for semver saves only, whether the specific `(name, version)` entry already exists. This is skipped for `"latest"` saves since they are always overwritable.

Preload messages sent in the Raft proposal:

```protobuf
message PreloadNumscriptVersion {
  AttributeID id = 1;
  string version = 2;    // "" means not found
}

message PreloadNumscriptEntry {
  AttributeID id = 1;
  bool exists = 2;       // true if version already stored
}
```

### Intra-Batch Propagation

Multiple orders in a single Raft proposal share the same `Buffered` state. The `DerivedKeyStore` overlay ensures later orders see writes from earlier orders in the same batch. For example:

1. Order 1: `SaveNumscript("transfer", "1.0.0")` — succeeds, entry cached
2. Order 2: `SaveNumscript("transfer", "1.0.0")` — fails with `NUMSCRIPT_VERSION_ALREADY_EXISTS` (sees Order 1's write)

## Architecture

### Write Path

```
HTTP PUT /numscripts/{name}
    │
    ▼
HTTP Handler → backend.Apply(SaveNumscriptRequest)
    │
    ▼
Controller → Raft Propose(SaveNumscriptOrder)
    │
    ▼
Admission: extractPreloadNeeds()
    ├── NumscriptVersions: preload latest pointer from Pebble
    └── NumscriptEntries: preload version existence from Pebble (semver only)
    │
    ▼
Raft replication (all nodes)
    │
    ▼
FSM Apply: processSaveNumscript()
    ├── Validate name, content, syntax
    ├── Resolve version ("latest" or semver immutability check)
    └── PutNumscript(info) → Buffered state
    │
    ▼
Buffered.Merge()
    ├── SaveNumscript → Pebble (versioned entry + latest pointer)
    └── DerivedKeyStore.Merge() → update Machine KeyStore
```

### Read Path

```
HTTP GET /numscripts/{name}?version=
    │
    ▼
HTTP Handler → backend.GetNumscript(name, version)
    │
    ▼
Controller → ReadNumscript(store, name, version)
    ├── If version == "": resolve latest pointer first
    └── Read specific version entry from Pebble
```

### Delete Path

```
HTTP DELETE /numscripts/{name}
    │
    ▼
Admission → preload NumscriptVersions
    │
    ▼
FSM Apply: processDeleteNumscript()
    ├── Check latest version exists (from cache, not Pebble)
    └── DeleteNumscriptLatest(name) → clear latest pointer (soft delete)
    │
    ▼
Buffered.Merge()
    └── ClearNumscriptLatestVersion → write empty value to Pebble
```

## File Map

| Layer | File | Contents |
|---|---|---|
| HTTP | `internal/compat/http/handlers_save_numscript.go` | PUT handler |
| HTTP | `internal/compat/http/handlers_get_numscript.go` | GET handler |
| HTTP | `internal/compat/http/handlers_list_numscripts.go` | List handler |
| HTTP | `internal/compat/http/handlers_delete_numscript.go` | DELETE handler |
| CLI | `cmd/ledgerctl/numscripts/` | One file per subcommand |
| Business logic | `internal/service/processing/processor_numscript_library.go` | Save/delete processors |
| Errors | `internal/service/processing/errors.go` | Error types and reason codes |
| Store interface | `internal/service/processing/store.go` | `InMemoryStore` numscript methods |
| State buffer | `internal/service/state/buffer.go` | `Buffered` numscript operations |
| State machine | `internal/service/state/machine.go` | `Machine` numscript KeyStores |
| Pebble batch | `internal/service/state/batch.go` | `SaveNumscript`, `ClearNumscriptLatestVersion` |
| Pebble read | `internal/service/state/store.go` | `ReadNumscript`, `ReadNumscriptLatestVersion`, `ReadAllNumscripts` |
| Cache | `internal/service/cache/cache.go` | `NumscriptVersions`, `NumscriptEntries` caches |
| Admission | `internal/service/admission/admission.go` | Preload phases 5-6 |
| Loaders | `internal/service/admission/loader.go` | `NumscriptVersions`, `NumscriptEntries` loaders |
| DAL types | `internal/storage/dal/types.go` | `NumscriptVersionKey`, `NumscriptEntryKey` |
| Proto | `misc/proto/common.proto` | `NumscriptInfo`, log payload messages |
| Proto | `misc/proto/raft_cmd.proto` | `SaveNumscriptOrder`, `DeleteNumscriptOrder`, preload messages |
| Proto | `misc/proto/bucket.proto` | gRPC service methods |
| gRPC errors | `internal/application/grpc_errors.go` | Error-to-gRPC-status mapping |
| E2E tests | `tests/e2e/numscript_library_test.go` | Library CRUD and versioning tests |

## Related Documentation

- [Numscript Language](../numscript.md) — DSL syntax, features, and usage in transactions
- [Deterministic FSM](./deterministic-fsm.md) — Cache, preloading, and generation-based architecture
- [System Attributes](./attributes.md) — Attribute types, caching, and compaction
- [API Comparison](../api-comparison.md) — Feature parity tracking
