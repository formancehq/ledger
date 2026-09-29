# Ledger v3 plugin — mapping

**Plugin:** `ledger-v3` · **Product major:** 3 (only) · **Signing capability:** `sign.ledger.apply-batch`

**Inventory basis:** `cmd/ledgerctl` at `bb0297cce39ccd1eb45eba695e9aa10374d5865d` (`release/v3.0`)
**Contract basis:** `misc/proto/bucket.proto`, generated into
`github.com/formancehq/ledger/v3/internal/proto/servicepb`

## Enumeration method (reproducible)

`ledgerctl` was built from the pinned commit with Go 1.26.5 and its command
tree walked through cobra help output. The walk found **136** nodes, of which
**113** are runnable. Excluding cobra's five auto-added built-ins — `help` and
`completion {bash,zsh,fish,powershell}` — leaves **108** ledgerctl-authored
executable commands.

That reconciles exactly with the programme plan's stated 108. `ledgers
configuration` is counted as executable: it is runnable *and* has two
subcommands, so it is not grouping-only.

## Classification (108, complete)

| Bucket | Count | Disposition |
| --- | --- | --- |
| Ordinary product commands | **54** | included in the v3 plugin by programme completion |
| fctl host/local concerns | **13** | excluded — owned by the fctl host |
| Operator / storage / cluster / restore / checkpoint / provisioning | **34** | excluded from the business facet; any operator facet is a separate grant |
| Signing and event-sink control plane | **7** | deferred sensitive control-plane decision |

Reproduced by `internal/audit`. Per-command rows, aliases, source files and
RPCs are in `inventory.json`.

## Included product families (54)

| Family | Commands | Count |
| --- | --- | --- |
| `ledgers` | `configuration`, `configuration apply`, `configuration export`, `create`, `delete`, `delete-metadata`, `get`, `get-schema`, `list`, `remove-metadata-type`, `set-metadata`, `set-metadata-type`, `stats` | 13 |
| `transactions` | `analyze`, `create`, `delete-metadata`, `get`, `list`, `revert`, `set-metadata` | 7 |
| `chapters` | `archive`, `close`, `delete-schedule`, `get-schedule`, `list`, `set-schedule` | 6 |
| `accounts` | `aggregate-volumes`, `analyze`, `delete-metadata`, `get`, `list`, `set-metadata` | 6 |
| `account-types` | `add`, `get`, `list`, `remove`, `set-default-enforcement` | 5 |
| `queries` | `create`, `delete`, `execute`, `list`, `update` | 5 |
| `indexes` | `create`, `drop`, `inspect`, `list` | 4 |
| `numscripts` | `get`, `list`, `save`, `versions` | 4 |
| `audit` | `get`, `list` | 2 |
| `logs` | `get`, `list` | 2 |
| **Total** | | **54** |

`ledgers promote` is *not* in the 13: it promotes a mirror ledger to normal
mode, a replication-topology action, and is classified operator
(`exclusions.md` §B).

## gRPC surface

`ledger.BucketService` declares **39** RPCs at the pin:

| Shape | Count |
| --- | --- |
| Unary | 27 |
| Server-streaming | 12 |
| Client-streaming / bidirectional | 0 |

The 12 server-streaming RPCs are `ListLedgers`, `ListTransactions`,
`ListAccounts`, `ListIndexes`, `ListChapters`, `ListLogs`, `ListAuditEntries`,
`ListSigningKeys`, `ListNumscripts`, `CheckStore`, `AnalyzeAccounts`,
`AnalyzeTransactions`.

Absence of client-streaming matters: every write, including batched writes, is
a single unary `Apply` call. No command needs bidirectional transport.

## Writes go through one RPC

```
rpc Apply(ApplyRequest) returns (ApplyResponse);

message ApplyRequest {
  oneof variant {
    ApplyBatch unsigned = 1;
    signature.SignedApplyBatch signed = 2;
  }
  common.CallerSnapshot forwarded_caller_snapshot = 3;
  bool skip_response = 4;
}

message ApplyBatch {
  repeated Request requests = 1;
  string idempotency_key = 2;
}
```

Mutating commands compose a `Request` variant into an `ApplyBatch`. This is why
`sign.ledger.apply-batch` covers all v3 writes with one capability.

## Signing bindings — verified verbatim at the pin

| Programme claim | Verified value | Evidence |
| --- | --- | --- |
| Protobuf full name `ledger.ApplyBatch` | `package ledger;` + `message ApplyBatch` | `misc/proto/bucket.proto:4`, `:220` |
| Method `/ledger.BucketService/Apply` | `BucketService_Apply_FullMethodName = "/ledger.BucketService/Apply"` | `internal/proto/servicepb/bucket_grpc.pb.go:30` |
| Logical fctl identifier `formance.ledger.v3.ApplyBatch` | binds to the above | RFC 0009 / RFC 0011 |
| Unsigned default, signed after activation | `oneof variant { unsigned, signed }` | `bucket.proto` `ApplyRequest` |
| Idempotency key covered by the signature | `idempotency_key` is a field **of** `ApplyBatch`, so it is inside the signed bytes | `bucket.proto:220` |

### Correction to the recorded contract

The envelope's Protobuf full name is **`signature.SignedApplyBatch`**, not
`ledger.SignedApplyBatch`. It lives in a different proto package
(`misc/proto/signature.proto:17`, Go package `…/internal/proto/signaturepb`):

```protobuf
message SignedApplyBatch {
  string key_id = 1;    // ID of the public key used to sign
  bytes signature = 2;  // Ed25519 signature (64 bytes)
  bytes payload = 3;    // Exact serialized ApplyBatch bytes signed by the client
}
```

The programme plan names the type but not its package. Any binding table must
record `signature.SignedApplyBatch`; assuming the `ledger` package would fail
resolution.

The proto comment confirms RFC 0009's invariants independently: the client
signs the exact serialized `ApplyBatch` bytes, the server verifies against
`payload` and **never re-serializes**, and signing the whole batch covers
composition and ordering.

## Pagination

Streaming reads paginate through a gRPC **trailer**: `x-next-cursor`, readable
only after draining the stream to clean EOF (`cmd/ledgerctl/logs/list.go:72`,
`cmd/ledgerctl/audit/list.go:108`). Some unary responses instead carry a
`next_cursor` field (`bucket.proto:1322`, `:1333`).

This matches fctl RFC 0011, which specifies `Trailer()` exposing exactly one
validated `x-next-cursor` after clean EOF. No contract change is needed.

`account-types get` and `account-types list` reach `ListLedgers` indirectly
through `cmdutil.GetAllLedgersInfo` → `cmdutil.DrainAllPages`, which drains
every page. The plugin must not reuse that helper — see `auth-scopes-risks.md`
R3.

## Destructive product commands (12)

`ledgers delete`, `ledgers delete-metadata`, `ledgers remove-metadata-type`,
`transactions delete-metadata`, `transactions revert`, `accounts
delete-metadata`, `account-types remove`, `queries delete`, `indexes drop`,
`chapters archive`, `chapters close`, `chapters delete-schedule`.

## CLI grammar

ledgerctl names are source vocabulary, not a command tree to copy. Where the
existing wording already reads as intent it is retained. Three vocabulary
decisions are left open in `exclusions.md` §D: `show`/`get`,
`run`/`execute`, and `analyze`/`analyse` — ledgerctl currently ships both
spellings as aliases, and fctl must pick one canonical form globally.
