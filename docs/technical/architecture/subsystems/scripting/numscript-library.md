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

The read-only APIs (`GetNumscript`) resolve the `version` selector as follows:

| Input | Strategy | Resolved to |
|---|---|---|
| `""` (empty) or `"latest"` | Read the latest pointer (greatest stored semver), then fetch that exact version | Greatest stored semver, or `NOT_FOUND` if the name has no versions |
| `"1.0.0"` (full semver) | Direct lookup on the exact semver key | Exact match or `NOT_FOUND` |
| `"1.0"` (major.minor) | Range scan `[1.0.0, 1.1.0)`, take the **highest** | Highest `1.0.x` patch |
| `"1"` (major only) | Range scan `[1.0.0, 2.0.0)`, take the **highest** | Highest `1.x.y` minor+patch |

Empty and partial (`"1"`, `"1.0"`) selectors are a **read-only** convenience, the partial ones parsed by `semver.ParsePartial()` (`internal/pkg/semver/`). They resolve outside the FSM, where a Pebble scan is allowed, and always return a concrete version. Save rejects them: only a full semver is storable.

An **executable** `ScriptReference` in a transaction is stricter — `version` is required and accepts only the literal `"latest"` or an exact full semver; empty and partial selectors are rejected at admission with `NUMSCRIPT_INVALID_VERSION`. The selector is state-independent, so this is a structural gate (`validateOrderContent`) that runs on every accepted order — including one the FSM goes on to skip for a reference conflict, whose script is never resolved. The two forms are rejected for different reasons:

- An **empty** selector is resolvable (it would follow the same latest pointer as `"latest"`), but the selector a caller submits is what the audit chain records, so an executable reference must state the version it intends explicitly rather than leave it implicit.
- A **partial** selector resolves through a Pebble range scan, which the FSM apply path cannot make (invariant #3). `"latest"` stays legal because the FSM resolves it through the covered latest-pointer attribute instead — see [Resolution Flow](#resolution-flow--admission-plans-the-fsm-resolves).

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
| Not runnable on the VM | `NUMSCRIPT_COMPILE_ERROR` | 400 | INVALID_ARGUMENT | A transaction's script parses and resolves but does not compile (static-semantics error caught by the compiler's typechecker, feature used without its flag, unsupported construct, VM capacity exceeded, var value that does not bind) — `ErrNumscriptCompile` |

## Script References in Transactions

Instead of inlining a numscript in every `CreateTransaction` request, clients can pass a `ScriptReference` that points to a script stored in the library. The reference's `version` is required and accepts only the literal `"latest"` or an exact full semver — the executable subset of the [version resolution](#version-resolution) rules.

### Protobuf

```protobuf
message ScriptReference {
  string name = 1;
  string version = 2; // required: "latest" or an exact full semver
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

### Execution — admission compiles, the FSM executes on the VM

The Numscript VM is the only engine that executes a script. The library's
tree-walking interpreter code is still used, but only to analyze scripts, never
to execute them: dependency resolution at admission and the FSM's stale-inputs
re-resolution walk the parsed AST (`numscript.SafeResolveDependencies`).

Admission compiles each script it resolved to VM bytecode
(`numscript.compileScript`, on the leader's parallel path) — once per cached
script: the compile and its bytecode encoding hang off the script's
`NumscriptCache` parse entry (`lruEntry.compileParsed`), so every order of a
script shares one program and only its vars are encoded per order. It binds
the artifact to the order's technical sub-message: `compiled_program`,
`compiled_vars` (the order's vars encoded against that program's variable
layout) and `compiled_script_hash` (XXH3-128 of the exact text compiled). The same
16-byte hash keys the parse and compiled caches. Like the attribute keys, it is
not collision-resistant against chosen inputs; that is acceptable because write
scopes are cluster-wide, so a writer able to craft a collision can already
write any ledger directly. Per-ledger write isolation would invalidate that
premise and require comparing the script text on a cache hit.
Admission also runs that artifact on a fresh VM instance to predict the
script's effects for later orders in the same atomic batch.

A script the VM cannot run is rejected at admission with
`ErrNumscriptCompile` (`NUMSCRIPT_COMPILE_ERROR`, `KindValidation`, freezable,
detail in the `details` metadata key): the compiler's typechecker rejects the
script (`Parse` checks syntax only), a feature is used without its flag, the
compiler does not support a construct, the program exceeds the VM's capacity (register banks, program
size), or a var value does not bind to the program's variable layout. Compile
runs after dependency resolution, so a script resolution already rejects keeps
its specific error — asset scaling, for instance, still fails with
`ErrNumscriptScalingUnsupported`. A compiler panic surfaces loudly as
`ErrNumscriptRuntime`.

The FSM decodes and verifies the program once per artifact — `NumscriptCache`
keeps one warm VM instance per script, keyed by `compiled_script_hash`
(already checked against the resolved text), and serves it only for the
bytes the order commits to: bytes identical to the ones it verified when the
program travels by value, bytes with the hash the order names when it travels
by reference (below). Compiling the same text under the same bundled library
is deterministic — byte-identical, always — but a hit still compares bytes
because this binary is not the only one that could have produced the
committed bytes: a mismatch means two different library versions compiled the
same text (a straddled mixed-binary window, not ordinary operation — see
"Upgrading across the Numscript VM execution change" below). On a mismatch
the order's bytes are decoded, verified and replace the entry, so a node
always runs the committed bytes and only redoes that work when the bytes
change. The leader compiles a script once and reuses it, so in steady state
every order of a script hits. A rejected artifact is never cached, and the
cache is in-memory, so an upgrade restarts it empty. It then executes the
artifact per apply (`numscript.SafeExecCompiled`). The apply store reaches
the proposal's Scope and coverage plan; the library's `Exec` releases its
store on every exit (success, error or panic), so a cached instance never
pins an old proposal. The verifier is what entitles the VM to run
wire-supplied bytecode without per-instruction checks.

Every scripted order admission proposes carries `compiled_vars`,
`compiled_script_hash` and exactly one of `compiled_program` and
`compiled_program_hash`; an order it forwards without them is marked
`preload_unavailable` and rejected before any read. The bytecode travels by
value once per script per admission instance — on the first order whose
script that instance's own compile cache had not compiled before
(`CompiledScript.AlreadyCompiled`, backed by `lruEntry.compileParsed`) — and
by reference, as the XXH3-128 of the bytes (`numscript.HashProgram`), on
every later order of the script. The signal describes what this instance has
sent, never a peek at what any replica cached: admission and the FSM apply
path each construct their own `NumscriptCache` instance and share no state,
and the FSM tolerates the signal being wrong either way (service protocol
revision 24; see
[Omitting already-cached Numscript bytecode](../../../../ops/deployment.md#omitting-already-cached-numscript-bytecode-revision-24)).
The FSM runs the committed artifact when its own library can use it, and
otherwise derives program and vars from the script text with that library
(`numscript.SafeExecCommitted`, built on `numscript.SafeExecFromText`, the
same compile admission runs). A by-value artifact is usable when the library
reads its bytecode version. A by-reference one is usable when bytes with the
committed program hash are at hand: the node's own cache entry for the script
hash when its bytes have that hash — the steady state, since applying the
earlier by-value order warmed every replica — or, after a restart, an LRU
eviction, or on a replica that joined later, this binary's compile of the
script text when it reproduces the hash, then decoded, verified and cached
like a by-value program (one compile per script per cache lifetime, never one
per order). Within one library version the same text compiles to the same
bytes, so a hit and a miss run the same bytes with the same committed vars
and the outcome never depends on a replica's cache (invariant #2). A library
that cannot read the artifact's version, or a compile that does not reproduce
the referenced hash, means another library version produced the artifact — a
replica mid rolling upgrade — and is never a failure: the replica derives
program and vars from the text, and the committed vars are never run against
a program they were not encoded for (equal pool sizes with a different
variable layout would post wrong amounts with no error at all). Agreement
across library versions then rests on the library keeping a script's
semantics stable, the contract audit replay (below) already relies on. The
artifact as a whole is an optimization derivable from the script text, not
part of the order's meaning: an order with none of the four fields takes the
same text path, which outside audit replay is an admission bug flagged with
an Antithesis `assert.Unreachable` (invariant #7) that never feeds the
outcome (invariant #2). Any other combination of the four fields is a corrupt
state admission never produces and fails the order loudly before any cache
access.

What the FSM does reject — failing the order with `ErrNumscriptRuntime`
(invariant #7), identically on every node running the binary — is corruption,
not a version difference: a half whose header does not parse (truncated, bad
magic), whatever bytecode version the other half carries — the headers of
both halves are inspected before the version decides anything, so a foreign
version never masks a corrupt half, and the producer inspects them next to
the shape classification, ahead of the stale-inputs re-resolution, so changed
inputs never turn a corrupt header into a retryable stale rejection
(`numscript.CommittedArtifact.CheckHeaders`); a half whose header this library reads
but which does not decode; a program that fails verification; committed vars a cached
program's layout does not cover; or a `compiled_script_hash` that does not
match the resolved text. Readability is the library's own rule
(`numscriptlib.CurrentBytecodeVersion.CanRead`): for a stable major, the same
major and a minor no newer than the bundled one, since a minor bump is
additive by the library's contract; an unstable `0.x` version, which the
library uses today, reads only itself. The ledger never encodes that rule
itself — it peeks the version header to choose the text path, and the
decoders enforce it. None of the corruption failures can happen by
construction: admission produces the whole artifact with its own library,
inline scripts travel in the order, exact library versions are immutable, an
advanced `"latest"` is stale-rejected first, and our own compiler produced
the bytecode (see
[Upgrading across the Numscript VM execution change](../../../../ops/deployment.md#upgrading-across-the-numscript-vm-execution-change-revision-23)
for the library-semantics side of an upgrade).

The outcome is a function of the committed entry and the running binary
alone, so every replica on one binary applies the entry identically
(invariant #2). Technical fields are excluded from business-intent hashing
(invariant #10), so the artifact never reaches the audit chain. Client
requests cannot carry technical fields: `ApplyBatch` is made of `Request`
messages and admission builds the `raftcmdpb.Order` itself.

Because the audit never holds the artifact, the store checker, which re-runs
audited orders to rebuild state, takes the same recompile path for every
scripted order. Under the same bundled library, every compilation of a script
means the same thing, so this gives the order its original outcome. Across a
library change that alters execution semantics it does not: replaying history
applied by another library can rebuild different bytes or reject an order
that committed, as with the interpreter-to-VM change (see
[Upgrading across the Numscript VM execution change](../../../../ops/deployment.md#upgrading-across-the-numscript-vm-execution-change-revision-23)).
Missing artifacts are expected there, so
`state.AuditReplayer` turns on `RequestProcessor.CompileMissingNumscript`,
which only skips the `assert.Unreachable`; the cluster's own processor never
calls it.

### Version Pinning Examples

Given a library with versions `1.0.0`, `1.0.5`, `1.2.0`, `2.0.0`:

| `scriptReference.version` | Resolved version | Behavior |
|---|---|---|
| `"latest"` | `2.0.0` (greatest) | Follows the greatest stored semver |
| `"1.0.0"` | `1.0.0` | Exact pin, never changes |
| `"2.0.0"` | `2.0.0` | Exact pin |
| `"3.0.0"` | `NOT_FOUND` | No such stored version |
| `""` | `INVALID_ARGUMENT` | The selector is required |
| `"1"` / `"1.0"` | `INVALID_ARGUMENT` | Partial selectors are read-only |

### Error Cases

| Condition | gRPC code | Reason |
|---|---|---|
| Both `script` and `scriptReference` set | `INVALID_ARGUMENT` | `SCRIPT_AND_REFERENCE_CONFLICT` |
| `version` empty, partial, or not a semver | `INVALID_ARGUMENT` | `NUMSCRIPT_INVALID_VERSION` |
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
- **Reference.** For a `"latest"` reference, admission declares the latest pointer plus the content key for the discovered greatest semver (see [Resolution Flow](#resolution-flow--admission-plans-the-fsm-resolves)). For an exact reference, it declares just that content key. Absent keys are declared with a `Declare` plan so a never-recorded script surfaces as `ErrNotFound` (→ `NUMSCRIPT_NOT_FOUND`) rather than a coverage fault.

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
| Admission | `internal/application/admission/admission.go` | `plan.Coverage` declaration + script reference planning |
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
- [Idempotency Keys](../admission/idempotency.md#numscript-dependency-resolution-failures) — how admission classifies a dependency-*discovery* failure (distinct from the `ScriptReference` version resolution above) as terminal vs. forwardable, by selector mutability and read-attempt provenance
- [API Comparison](../../../contributing/api-comparison.md) — Feature parity tracking
