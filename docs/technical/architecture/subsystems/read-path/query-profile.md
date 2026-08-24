# Query Profile

`QueryProfile` is the per-request diagnostic record of a read. It answers two
different questions, and keeping them apart is the whole point of the design:

1. **Where did query execution go?** — the iterator tree, index and enrichment
   durations, item counts.
2. **How long did the server hold the request, and doing what?** — the phase
   breakdown (prepare / execute / barrier / deliver) and the headline
   `server_duration_us`.

Source: `internal/query/profile.go`, wire type `servicepb.QueryProfile`
(`misc/proto/bucket.proto`).

## Why the second question exists

Before EN-1859 the profile only reported execution: `index_duration_us +
enrichment_duration_us`. Measured against an 87.7M-transaction ledger over an
established gRPC connection, a client saw 56 ms end-to-end while the profile
accounted for 2.3 ms. The remaining ~54 ms — request decode, filter compilation,
row serialisation, stream writes — was invisible, so a caller could not tell a
slow network from a slow server, read-performance work had nothing to target,
and no server-side latency SLO could be defined.

## Phase model

The request clock starts when the profile is created and stops at `Finish()`.

```
|<-------------------------- wall clock -------------------------->|
[  prepare  ][  barrier  ][  execute  ][ residual ][   deliver     ]
                   ^                                      ^
        caller-requested wait                  consumer-dependent
```

| Field | Window | In `server_duration_us`? |
|---|---|---|
| `prepare_duration_us` | handler entry → executor invocation: auth, request decode/validation, filter parsing/compilation, checkpoint-store opening | yes |
| `execute_duration_us` | the executor call: snapshot setup, ledger/schema resolution, index scan, enrichment, plus lazy row pulls | yes |
| `barrier_duration_us` | Raft `ReadIndex` quorum round-trip and `ReadOptions.min_log_sequence` read-index catch-up | **no** |
| `deliver_duration_us` | row serialisation + transport hand-off | **no** |
| `first_row_duration_us` | handler entry → first row accepted by the transport (streams only) | n/a |
| `server_duration_us` | wall clock − barrier − deliver | — |

`index_duration_us` and `enrichment_duration_us` are sub-phases of
`execute_duration_us`, not peers of the total.

`prepare + execute <= server`. The difference is server work outside both phases
(response assembly, pagination trailer, profile emission); `ledgerctl` renders it
as *Other server work* so the breakdown stays honest instead of losing time
silently. A large residual is a signal that the phase boundaries need refining.

### Why `barrier` is excluded

A linearizable read pays a `ReadIndex` quorum round-trip, and a read-your-writes
read additionally waits for the read index to reach `min_log_sequence`. Both are
latency the *caller opted into*. Folding them into the server total would blame
the server for the caller's consistency requirement, and would make the number
move with cluster RTT rather than with server cost.

### Why `deliver` is excluded, and how streaming ambiguity is resolved

`ListTransactions` and `ListAccounts` are server-streaming. A naive total would
include time blocked in `stream.Send` waiting for HTTP/2 flow-control window —
that is consumer cost, and a total that grows because the client stopped reading
would mislead exactly the reader trying to decide whether the server is slow.

So the streaming loop (`internal/adapter/grpc/stream_helper.go`) splits its own
wall time in two:

- `cur.Next()` is **row production** and is charged to `execute`. It is near-free
  for an eagerly materialised local cursor, but on a follower that routed the
  read to the leader each `Next()` is an upstream stream receive — genuine
  server-side query cost that would otherwise vanish.
- `stream.Send()` is **delivery** and is charged to `deliver`, outside the server
  total.

"Time to first row" and "total to last row" are then reported as distinct
numbers: `first_row_duration_us` versus `server_duration_us` + `deliver`. A slow
consumer shows up as `deliver >> first_row` with `server_duration_us` unchanged.

## gRPC and HTTP agree on `server_duration_us`

`server_duration_us` is defined identically on both surfaces — handler entry to
response-ready, minus barrier, minus delivery — which is what makes the two
comparable. Only the *content* of `deliver_duration_us` differs, because the
transports differ:

| | gRPC | HTTP |
|---|---|---|
| `deliver_duration_us` | sum of `stream.Send()`: marshal + transport write + flow-control wait | always 0 — not measurable, see below |
| `first_row_duration_us` | populated for streaming reads | always 0 — the response is buffered, there is no per-row hand-off |
| row production charged to `execute` | per row in the send loop | once around the `drainCursor` loop |

The HTTP delivery gap is structural. The profile travels in the
`X-Query-Profile-Result` response *header*, so it must be finalised and flushed
before the body is written — which puts the body write out of reach by
construction (`finishProfile` in `internal/adapter/http/response.go`).

Only the marshal step could in principle be measured, by buffering the body. That
was deliberately rejected: the four profiled routes use three different writers
(`writeOK`, `writeOKChecked`, `writeJSONResponse`) whose sonic configurations
differ in HTML escaping and map-key ordering, so funnelling them through one
buffered marshaller would silently change their JSON output. A partial number is
not worth an unrelated wire change.

The requirement that actually matters is unaffected: `server_duration_us` excludes
row serialisation on **both** surfaces, so the headline number stays comparable.

## Transport surfaces

| Surface | Request opt-in | Response channel |
|---|---|---|
| gRPC | `x-query-profile` metadata | `x-query-profile-result-bin` trailer |
| HTTP | `X-Query-Profile` header | `X-Query-Profile-Result` header (base64 protobuf) |

Profiled endpoints: `ListTransactions`, `ListAccounts`, `AggregateVolumes`,
`ExecutePreparedQuery` — on both surfaces.

`ledgerctl` requests the trailer with `--analyze` / `--analyse` and renders both
tables (`cmd/ledgerctl/cmdutil/profile.go`).

## Cost when the profile is not requested

The profile is collected on **every** profiled read, not only when asked: the
slow-query log and the OTel span consume the same record, and a threshold that
could only see execution time would miss precisely the requests EN-1859 is
about. All per-request measurements are O(1) — a handful of `time.Now()` calls.

The one instrumentation whose cost scales with the result size — the per-row
`execute`/`deliver` split in the streaming loop — is gated on whether the caller
actually asked for the profile (`QueryProfile.Detailed()`). An unprofiled request
brackets the whole loop once and charges it to `deliver`. That direction is
deliberate: `deliver` is excluded from the total, so `server_duration_us` can
only be *under*-reported for an unprofiled request, never inflated by a slow
consumer.

## Slow-query threshold

`--query-profile-threshold` (default 10 ms) gates the trace-level profile log and
the span attributes. It is compared against `ServerDuration`, not the execution
total: thresholding on execution meant the slow requests the log exists to
surface were the ones it could not see. See `emitProfile` in
`internal/adapter/grpc/server_bucket.go`.

`LogTo` also emits `profileDetailed`. When false, `deliverDurationUs` holds the
whole streaming loop rather than just the sends — do not read the two as
equivalent across log lines.

## Known gaps

- **Leader-forwarded reads.** When a follower forwards a read to the leader
  (`ConsistencyLeader`, or the syncing-node fallback in
  `RoutedController.readCtrl`), the upstream RPC is charged to the local
  `execute` phase. The remote server's own breakdown is not nested into the
  local profile, so `execute` there conflates network hops, leader-side prepare
  and leader-side execution.
- **HTTP body write** is not measurable (see above).
- **Unary gRPC responses** (`AggregateVolumes`, `ExecutePreparedQuery`) have no
  delivery phase: the reply is marshalled by the gRPC codec after the handler
  returns, past the point where the trailer is set.
- **HTTP reads do not feed the slow-query log.** `--query-profile-threshold`
  drives `emitProfile`, which only the gRPC handlers call. An HTTP read still
  reports its profile on request, but a slow one produces no threshold-triggered
  log line or span attributes. Pre-existing, not introduced by EN-1859.
