# Numscript Library

The numscript library is a per-ledger repository for storing, retrieving, and referencing reusable numscript programs with semantic versioning. Scripts stored in the library can be referenced when creating transactions, avoiding the need to inline the script content in every request.

## Concepts

### Versioning Model

Each numscript is identified by a **name** (e.g. `payment-with-fees`) scoped to a ledger, and holds one or more **versions**. The library is **immutable and append-only**:

- **Every version is an explicit full semver** (`major.minor.patch`). Saving with an empty, `"latest"`, or partial version is rejected with `NUMSCRIPT_INVALID_VERSION`.
- **Content entries are immutable.** Once `payment-with-fees` `1.0.0` is stored it can never be overwritten or removed. Re-saving the same `(name, version)` returns `NUMSCRIPT_VERSION_ALREADY_EXISTS`.
- **There is no delete and no restore.** Nothing tombstones or clears a version; the only mutation is appending a new one.
- **Each name has a `latest` pointer equal to the greatest stored semver.** Saving advances the pointer to `max(current, saved)`. Versions may be saved out of order — saving `1.0.0` after `2.0.0` leaves the pointer at `2.0.0`.

Because there is no soft-delete state, a stored version has no derived status: it simply exists, and exactly one of them (the greatest) is what the latest pointer resolves to.

`ListNumscriptVersions` returns the current latest pointer plus every stored version, ordered highest semver first. `ListNumscripts` returns the greatest version of each named script in the ledger.

### Version Resolution

When reading a numscript (via `GetNumscript` or a `ScriptReference` in a transaction), the `version` selector is resolved as follows:

| Input | Strategy | Resolved to |
|---|---|---|
| `""` (empty) or `"latest"` | Read the latest pointer (greatest stored semver), then fetch that exact version | Greatest stored semver, or `NOT_FOUND` if the name has no versions |
| `"1.0.0"` (full semver) | Direct lookup on the exact semver key | Exact match or `NOT_FOUND` |
| `"1.0"` (major.minor) | Range scan `[1.0.0, 1.1.0)`, take the **highest** | Highest `1.0.x` patch |
| `"1"` (major only) | Range scan `[1.0.0, 2.0.0)`, take the **highest** | Highest `1.x.y` minor+patch |

Partial selectors (`"1"`, `"1.0"`) are a **read-only** convenience parsed by `semver.ParsePartial()` (`internal/pkg/semver/`). They are valid for `GetNumscript` and transaction references but rejected on save (only full semver is storable).

### Syntax Validation

Scripts are parsed and validated **at save time**, not at transaction-creation time. This catches syntax errors early. The parser uses the same Numscript interpreter as transaction execution, with all experimental features enabled.

## API

### HTTP Endpoints

| Method | Path | Description |
|---|---|---|
| `PUT` | `/numscripts/{name}` | Save a new immutable version (explicit full semver) |
| `GET` | `/numscripts/{name}?version=` | Get a numscript (empty/`latest` = greatest semver) |
| `GET` | `/numscripts/{name}/versions` | List the latest pointer and every stored version |
| `GET` | `/numscripts` | List the greatest version of each named script |

#### Save Numscript

```
PUT /numscripts/payment-with-fees
Content-Type: application/json

{
  "content": "vars { monetary $amount } send $amount ( source = @treasury destination = @merchant )",
  "version": "1.0.0"
}
```

Response: `201 Created` with a log entry containing the `NumscriptInfo`. Returns `409` if the version already exists, `400` if the version is not a full semver or the content fails to parse.

#### Get Numscript

```
GET /numscripts/payment-with-fees?version=1.0.0
```

Response: `200 OK` with `NumscriptInfo` (name, content, version, createdAt). Returns `404` if the numscript or version does not exist.

#### List Numscript Versions

```
GET /numscripts/payment-with-fees/versions
```

Response: `200 OK` with the current latest version and every stored version (highest semver first).

#### List Numscripts

```
GET /numscripts
```

Response: `200 OK` with an array of `NumscriptInfo` (greatest version of each named script).

### gRPC

The `BucketService` exposes:
- `SaveNumscript` via the `Apply` RPC (the only write; goes through Raft)
- `GetNumscript(GetNumscriptRequest) returns (NumscriptInfo)` (read)
- `ListNumscripts(ListNumscriptsRequest) returns (stream NumscriptInfo)` (read, streaming)
- `ListNumscriptVersions(ListNumscriptVersionsRequest) returns (ListNumscriptVersionsResponse)` (read; response carries `latest_version` + `versions`)

### CLI

```bash
# Save a version (explicit full semver, required)
ledgerctl numscripts save payment-with-fees --file script.num --version 1.0.0

# Save from stdin
cat script.num | ledgerctl numscripts save payment-with-fees --version 1.0.0

# Get latest version (greatest stored semver)
ledgerctl numscripts get payment-with-fees

# Get specific version
ledgerctl numscripts get payment-with-fees --version 1.0.0

# List the greatest version of each script
ledgerctl numscripts list

# List the latest pointer and every stored version
ledgerctl numscripts versions payment-with-fees
```

## Error Handling

| Error | Reason code | HTTP | gRPC | When |
|---|---|---|---|---|
| Name required | — | 400 | INVALID_ARGUMENT | Empty name |
| Content required | — | 400 | INVALID_ARGUMENT | Empty content |
| Parse error | `NUMSCRIPT_PARSE_ERROR` | 400 | INVALID_ARGUMENT | Invalid Numscript syntax |
| Invalid version | `NUMSCRIPT_INVALID_VERSION` | 400 | INVALID_ARGUMENT | Save version is not a full semver |
| Version exists | `NUMSCRIPT_VERSION_ALREADY_EXISTS` | 409 | ALREADY_EXISTS | The `(name, version)` is already stored (immutable) |
| Not found | `NUMSCRIPT_NOT_FOUND` | 404 | NOT_FOUND | Get a non-existent numscript or version |

## Script References in Transactions

Instead of inlining a numscript in every `CreateTransaction` request, clients can pass a `ScriptReference` that points to a script stored in the library. The [version resolution](#version-resolution) rules apply to the reference's `version` selector.

### Protobuf

```protobuf
message ScriptReference {
  string name = 1;
  string version = 2; // "" / "latest" = greatest stored semver
  map<string, string> vars = 3;
}

message CreateTransactionPayload {
  // ...
  ScriptReference script_reference = 9;
}
```

### Resolution Flow — admission plans, the FSM resolves

Numscript content is a Pebble-backed projection, and the FSM apply path must never read Pebble (invariant #3) — so a transaction that references `"latest"` cannot resolve the pointer at apply time by itself. Resolution is split between admission (which reads Pebble but must not mutate the audited order) and the FSM (which is deterministic but reads only the cache through the coverage gate):

1. **Admission does not rewrite the reference.** The `"latest"` (or exact) selector is carried into the `raftcmdpb.Order` verbatim so the audited command matches what the client sent. Admission's only job is to *plan* the reads the FSM will make.
2. **Admission declares the coverage the FSM needs.** For a `"latest"` reference it declares the per-name latest-pointer key (`SubAttrNumscriptVersion`) and, having discovered the current greatest semver, the corresponding content key (`SubAttrNumscriptContent`). The greatest is computed as `max(intra-bulk overlay, persisted)` so a save earlier in the same bulk is visible.
3. **The FSM resolves at apply time.** `processCreateTransaction` reads the latest pointer through the gated `Scope`, then checks that the greatest version's content was actually preloaded:

   ```go
   greatest, _ := s.GetNumscriptLatestVersion(ledger, name)
   if s.CheckCoverage(dal.SubAttrNumscriptContent,
       domain.NumscriptEntryKey{LedgerName: ledger, Name: name, Version: greatest}) != nil {
       return domain.ErrStaleProposal   // retryable
   }
   info, _ := s.ResolveNumscriptContent(ledger, name, greatest)
   ```

4. **Skew is handled by stale-retry.** If another proposal advanced the latest pointer between admission's read and this apply, the content the FSM now needs was never preloaded, the coverage check misses, and the order is rejected with `ErrStaleProposal` (`KindUnavailable`, retryable). Re-admission observes the new greatest and declares the right content key. This is the same class of backstop as `PredictedIndex`: a no-op on the happy path, a bounded retry on genuine cross-proposal races.

```
POST /{ledger}/transactions  { scriptReference: { name: "payment", version: "latest" } }
    │
    ▼
Admission
    ├── keep the "latest" selector in the Order (no mutation)
    ├── discover greatest = max(overlay, Pebble)
    └── declare needs: SubAttrNumscriptVersion{name} + SubAttrNumscriptContent{name, greatest}
    │
    ▼
Raft replication (all nodes)
    │
    ▼
FSM Apply: processCreateTransaction
    ├── GetNumscriptLatestVersion → greatest
    ├── CheckCoverage(SubAttrNumscriptContent, {name, greatest})
    │        └── miss → ErrStaleProposal (retry)
    └── ResolveNumscriptContent(name, greatest) → content
    │
    ▼
Normal transaction processing (parse, execute, postings...)
```

### Version Pinning Examples

Given a library with versions `1.0.0`, `1.0.5`, `1.2.0`, `2.0.0`:

| `scriptReference.version` | Resolved version | Behavior |
|---|---|---|
| `""` / `"latest"` | `2.0.0` (greatest) | Follows the greatest stored semver |
| `"1.0.0"` | `1.0.0` | Exact pin, never changes |
| `"1.0"` | `1.0.5` | Highest patch in `1.0.x` — auto-updates when `1.0.6` is saved |
| `"1"` | `1.2.0` | Highest minor+patch in `1.x.y` — auto-updates within major |
| `"2.0.0"` | `2.0.0` | Exact pin |
| `"3"` | `NOT_FOUND` | No version in the `3.x.y` range |

### Error Cases

| Condition | gRPC code | Reason |
|---|---|---|
| Both `script` and `scriptReference` set | `INVALID_ARGUMENT` | `SCRIPT_AND_REFERENCE_CONFLICT` |
| Script name not found / version not found | `NOT_FOUND` | `NUMSCRIPT_NOT_FOUND` |
| Latest advanced between admission and apply | `UNAVAILABLE` | `ErrStaleProposal` (client retries) |

## Storage

Numscript data lives in the attributes zone as two projections, both keyed by ledger-scoped keys (`internal/domain/keys.go`) and codified by sub-attribute codes (`internal/storage/dal/store.go`):

| Sub-attribute | Key | Value | Purpose |
|---|---|---|---|
| `SubAttrNumscriptContent` (`0x0A`) | `NumscriptEntryKey{LedgerName, Name, Version}` | `NumscriptInfo` | Immutable per-version content entry |
| `SubAttrNumscriptVersion` (`0x09`) | `NumscriptVersionKey{LedgerName, Name}` | `NumscriptVersionValue` | Per-name latest pointer (greatest stored semver) |

`NumscriptEntryKey` encodes `[ledger padded 64B][name]\x00[version]`; the version is stored as a plain semver string. `NumscriptVersionKey` encodes `[ledger padded 64B][name]`. Partial-version range scans on the read path exploit the ordering of the semver-string component within the content key range.

Both projections are audit-log derivable and are re-verified by the checker — see [Checker: `compareNumscripts`](../checker/checker.md).

### Attribute Caches

Numscript data uses the same preloading pattern as other system attributes (see [Deterministic FSM](../fsm/deterministic-fsm.md)):

| Cache | Key type | Value type | Purpose |
|---|---|---|---|
| `NumscriptVersions` | `NumscriptVersionKey{LedgerName, Name}` | `NumscriptVersionValue` | Latest pointer per name |
| `NumscriptContents` | `NumscriptEntryKey{LedgerName, Name, Version}` | `NumscriptInfo` | Per-version content |

The FSM reads both only through the gated `Scope` (`GetNumscriptLatestVersion`, `ResolveNumscriptContent`, `CheckCoverage`), so every read is admitted by the per-order coverage bits (invariant #9).

### Admission Preloading

- **Save.** Admission declares both the latest pointer (`SubAttrNumscriptVersion`) and the target content key (`SubAttrNumscriptContent`) for the `(name, version)` being written, so the FSM can enforce immutability (duplicate → `NUMSCRIPT_VERSION_ALREADY_EXISTS`) and advance the pointer to the greatest semver.
- **Reference.** For a `"latest"`/empty reference, admission declares the latest pointer plus the content key for the discovered greatest semver (see [Resolution Flow](#resolution-flow--admission-plans-the-fsm-resolves)). For an exact reference, it declares just that content key. Absent keys are declared with a `Declare` plan so a never-recorded script surfaces as `ErrNotFound` (→ `NUMSCRIPT_NOT_FOUND`) rather than a coverage fault.

### Intra-Batch Propagation

Multiple orders in a single Raft proposal share the same `WriteSet` state; the `Derived` overlay ensures later orders see earlier writes. Admission plans a bulk sequentially with a greatest-wins overlay so, e.g.:

1. Order 1: `SaveNumscript("transfer", "1.0.0")` — succeeds, pointer → `1.0.0`
2. Order 2: `SaveNumscript("transfer", "2.0.0")` — succeeds, pointer → `2.0.0`
3. Order 3: transaction referencing `"latest"` — resolves to `2.0.0` (sees Orders 1–2)

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
Admission: extractLedgerScopedNeeds()
    ├── SubAttrNumscriptVersion: latest pointer from Pebble
    └── SubAttrNumscriptContent: target (name, version) entry from Pebble
    │
    ▼
Raft replication (all nodes)
    │
    ▼
FSM Apply: processSaveNumscript()
    ├── semver.Parse(version) — reject non-full-semver
    ├── duplicate content → NUMSCRIPT_VERSION_ALREADY_EXISTS
    ├── read current greatest (before write)
    ├── PutNumscript(info)
    └── if greatest > saved: keep pointer; else advance to saved
    │
    ▼
WriteSet.Merge()
    ├── content entry → Pebble (SubAttrNumscriptContent)
    └── latest pointer → Pebble (SubAttrNumscriptVersion)
```

### Read Path

```
HTTP GET /numscripts/{name}?version=
    │
    ▼
HTTP Handler → backend.GetNumscript(name, version)
    │
    ▼
Controller → query.ReadNumscript(versionAttr, contentAttr, reader, ledger, name, version)
    │
    ├── version == "" / "latest"
    │       → ReadNumscriptLatestVersion(name) → greatest (e.g. "2.0.0")
    │       → readNumscriptExact(name, "2.0.0")
    │
    ├── depth == 3 (e.g. "1.0.0")
    │       → readNumscriptExact(name, "1.0.0")
    │
    └── depth < 3 (e.g. "1" or "1.0")
            → resolvePartialVersion(): range scan, highest match
```

## File Map

| Layer | File | Contents |
|---|---|---|
| HTTP | `internal/adapter/http/handlers_save_numscript.go` | PUT handler |
| HTTP | `internal/adapter/http/handlers_get_numscript.go` | GET handler |
| HTTP | `internal/adapter/http/handlers_list_numscripts.go` | List handler |
| HTTP | `internal/adapter/http/handlers_list_numscript_versions.go` | List-versions handler |
| CLI | `cmd/ledgerctl/numscripts/` | One file per subcommand (`save`, `get`, `list`, `versions`) |
| Business logic | `internal/domain/processing/processor_numscript_library.go` | `processSaveNumscript` |
| FSM reference resolution | `internal/domain/processing/processor_transaction.go` | Latest-pointer resolution + coverage check |
| Errors | `internal/domain/errors.go`, `reason.go` | Error types and reason codes |
| Scope interface | `internal/domain/processing/store.go` | `Scope` numscript methods |
| State buffer | `internal/infra/state/write_set.go` | `PutNumscript`, `SetNumscriptLatestVersion`, `GetNumscriptLatestVersion`, `ResolveNumscriptContent` |
| Gated scope | `internal/infra/state/scope.go` | Coverage-gated numscript reads |
| Query | `internal/query/numscript.go` | `ReadNumscript`, `ReadNumscriptLatestVersion`, `ReadAllNumscripts`, `ReadAllNumscriptVersions` |
| Admission | `internal/application/admission/admission.go` | Needs declaration + script reference planning |
| Admission overlay | `internal/application/admission/overlay.go` | Intra-bulk greatest-wins overlay |
| Checker | `internal/application/check/checker.go` | `compareNumscripts` projection verification |
| Rebuild | `internal/infra/backup/rebuild.go` | Projection rebuild from the audit chain |
| Semver | `internal/pkg/semver/semver.go` | `Version`, `Parse`, `ParsePartial`, `Compare` |
| DAL keys | `internal/domain/keys.go` | `NumscriptVersionKey`, `NumscriptEntryKey` |
| DAL codes | `internal/storage/dal/store.go` | `SubAttrNumscriptVersion`, `SubAttrNumscriptContent` |
| Proto | `misc/proto/common.proto` | `NumscriptInfo`, `NumscriptVersionValue`, `NumscriptVersionEntry` |
| Proto | `misc/proto/raft_cmd.proto` | `SaveNumscriptOrder` |
| Proto | `misc/proto/bucket.proto` | gRPC service methods, `ScriptReference` |
| gRPC errors | `internal/adapter/grpc/errors.go` | Error-to-gRPC-status mapping |
| E2E tests | `tests/e2e/business/numscript_library_test.go` | Library and versioning tests |

## Related Documentation

- [Numscript Language](../../../contributing/numscript.md) — DSL syntax, features, and usage in transactions
- [Deterministic FSM](../fsm/deterministic-fsm.md) — Cache, preloading, and generation-based architecture
- [Checker](../checker/checker.md) — `compareNumscripts` projection verification
- [System Attributes](../attributes/attributes.md) — Attribute types, caching, and compaction
- [API Comparison](../../../contributing/api-comparison.md) — Feature parity tracking
