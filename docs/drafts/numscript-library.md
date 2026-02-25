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

### Version Resolution

When reading a numscript (via `GetNumscript` or a `ScriptReference` in a transaction), the `version` field supports several formats. The system resolves them in order:

| Input | Strategy | Resolved to |
|---|---|---|
| `""` (empty) | Read the latest-version pointer, then recursively resolve that version | Whatever the pointer holds (e.g. `"2.0.0"` or `"latest"`) |
| `"latest"` | Direct lookup on the `latest` slot | The `latest` slot content |
| `"1.0.0"` (full semver) | Direct lookup on the exact semver key | Exact match or `NOT_FOUND` |
| `"1.0"` (major.minor) | Range scan `[1.0.0, 1.1.0)`, take the **highest** | Highest `1.0.x` patch |
| `"1"` (major only) | Range scan `[1.0.0, 2.0.0)`, take the **highest** | Highest `1.x.y` minor+patch |

Partial versions are parsed by `semver.ParsePartial()` (`internal/semver/`). Range scans exploit the big-endian uint32 encoding of semver components in Pebble keys, which ensures lexicographic order matches semantic order.

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

## Script References in Transactions

Instead of inlining a numscript in every `CreateTransaction` request, clients can pass a `ScriptReference` that points to a script stored in the library. The version resolution described in [Version Resolution](#version-resolution) applies here.

### Protobuf

```protobuf
message ScriptReference {
  string name = 1;
  string version = 2; // "" = latest pointer
  map<string, string> vars = 3;
}

message CreateTransactionPayload {
  // ...
  ScriptReference script_reference = 8;
}
```

### Resolution Flow

Version resolution happens in the **admission layer** (`admission.go`), before the Raft proposal is built. This means the script content is resolved at request time from Pebble, not from the FSM caches.

```
POST /{ledger}/transactions  { scriptReference: { name: "payment", version: "1" } }
    │
    ▼
Admission: resolveScriptReference()
    │
    ├── Validate: script and scriptReference are mutually exclusive
    │   (both set → INVALID_ARGUMENT)
    │
    ├── ReadNumscript(store, name="payment", version="1")
    │       │
    │       ▼
    │   semver.ParsePartial("1") → major=1, depth=1
    │       │
    │       ▼
    │   resolvePartialVersion(): range scan [1.0.0, 2.0.0)
    │       │
    │       ▼
    │   iter.Last() → NumscriptInfo { version: "1.3.0", content: "..." }
    │
    ├── If nil → NUMSCRIPT_NOT_FOUND
    │
    └── Build Script { plain: info.Content, vars: scriptReference.Vars }
        │
        ▼
    Normal transaction processing (parse, execute, postings...)
```

### Version Pinning Examples

Given a library with versions `1.0.0`, `1.0.5`, `1.2.0`, `2.0.0`:

| `scriptReference.version` | Resolved version | Behavior |
|---|---|---|
| `""` | Latest pointer (e.g. `"2.0.0"`) | Follows whatever version was last saved |
| `"1.0.0"` | `1.0.0` | Exact pin, never changes |
| `"1.0"` | `1.0.5` | Picks highest patch in `1.0.x` — auto-updates when `1.0.6` is saved |
| `"1"` | `1.2.0` | Picks highest minor+patch in `1.x.y` — auto-updates within major |
| `"2.0.0"` | `2.0.0` | Exact pin |
| `"3"` | `NOT_FOUND` | No version in the `3.x.y` range |

### Error Cases

| Condition | gRPC code | Reason |
|---|---|---|
| Both `script` and `scriptReference` set | `INVALID_ARGUMENT` | `SCRIPT_AND_REFERENCE_CONFLICT` |
| Script name not found / version not found | `NOT_FOUND` | `NUMSCRIPT_NOT_FOUND` |

## Storage

### Pebble Keys

Three key formats store numscript data:

| Prefix | Key format | Value |
|---|---|---|
| `KeyPrefixNumscript` (semver) | `[prefix][name]\x00\x00[major_u32BE][minor_u32BE][patch_u32BE]` | Protobuf `NumscriptInfo` |
| `KeyPrefixNumscript` (latest) | `[prefix][name]\x00\x01` | Protobuf `NumscriptInfo` |
| `KeyPrefixNumscriptLatest` | `[prefix][name]` | Version string (UTF-8 bytes) |

The `\x00` byte after the name is a separator. The next byte is a **tag**: `0x00` = semver entry, `0x01` = latest slot (constants `NumscriptVersionTagSemver` / `NumscriptVersionTagLatest` in `dal/types.go`). Semver components are encoded as big-endian `uint32`, which guarantees lexicographic key order matches semantic version order — enabling efficient range scans for partial version resolution.

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
    │
    ├── version == ""
    │       → ReadNumscriptLatestVersion(name) → pointer (e.g. "2.0.0")
    │       → ReadNumscript(name, "2.0.0")     (recursive)
    │
    ├── version == "latest"
    │       → readNumscriptLatestSlot(): direct Get on [prefix][name]\x00\x01
    │
    ├── depth == 3 (e.g. "1.0.0")
    │       → readNumscriptExactSemver(): direct Get on [prefix][name]\x00\x00[1][0][0]
    │
    └── depth < 3 (e.g. "1" or "1.0")
            → resolvePartialVersion(): range scan, iter.Last()
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
| Semver | `internal/semver/semver.go` | `Version`, `Parse`, `ParsePartial` |
| DAL types | `internal/storage/dal/types.go` | `NumscriptVersionKey`, `NumscriptEntryKey`, version tag constants |
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
