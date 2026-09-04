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
accounted for 2.3 ms. The remaining ~54 ms — authentication, filter compilation,
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
| `prepare_duration_us` | request entry → executor invocation: auth, validation, filter parsing/compilation, checkpoint-store opening, and HTTP query/body decode | yes |
| `execute_duration_us` | the executor call: snapshot setup, ledger/schema resolution, index scan, enrichment, plus lazy row pulls | yes |
| `barrier_duration_us` | **local** Raft `ReadIndex` quorum round-trip and `ReadOptions.min_log_sequence` read-index catch-up, successful or not | **no** |
| `deliver_duration_us` | row serialisation + transport hand-off | **no** |
| `first_row_duration_us` | request entry → first row accepted by the transport | n/a |
| `server_duration_us` | wall clock − barrier − deliver | — |
| `forwarded` | the read was served by another node | n/a |

## Two totals, and what each one is blind to

Neither total is a clean latency-SLO basis on a streaming read. Say which one an
SLO uses and accept its bias:

| | `server_duration_us` | `server_duration_us + deliver_duration_us` |
|---|---|---|
| moved by a slow client? | no | **yes**, on a stream |
| includes row serialisation? | **no** | yes |
| same definition on gRPC and HTTP? | yes | no (HTTP delivery is 0) |

Row serialisation is excluded from `server_duration_us` because a gRPC
`stream.Send()` marshals and writes in one inseparable call — there is no seam to
measure between them without re-implementing the send path over
`grpc.PreparedMsg`, which would move the hot streaming path onto a
less-exercised codec/compression route for a measurement refinement. Not worth
the risk; the cost is reported in `deliver_duration_us` instead.

The **slow-query threshold uses the sum**, deliberately. `server_duration_us`
alone is blind to two real costs — serialisation, which grows with page size, and
the entire remote cost of a forwarded read, which arrives through the
row-production side of the send loop. A threshold blind to both would fail to
fire on exactly the requests it exists for. The price is that a slow consumer can
trip it; the logged breakdown then says which side was slow, which is strictly
more than a threshold that never fires says.

`first_row_duration_us` is 0 in three distinct cases: an empty streaming result,
a unary response (all HTTP routes, plus `AggregateVolumes` and
`ExecutePreparedQuery`), and a streaming read that failed before its first send.
Disambiguate with `items_collected` and the RPC status.

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

Only **local** barriers are measured. On a forwarded read the leader runs its own
`ReadIndexAndWait` (`x-consistency` is not propagated, so it defaults to
linearizable there), and that wait is invisible here: it arrives as row-production
time inside `execute_duration_us`. Always read `barrier_duration_us` together with
`forwarded`:

| `forwarded` | `barrier_duration_us` | meaning |
|---|---|---|
| false | any | the whole barrier this read paid |
| true | `0` | no local wait happened. Not "no barrier was needed": the remote node's is inside `execute_duration_us`. |
| true | non-zero | a local wait happened before the read left this node; the remote barrier is on top of it (see below) |

The last row always means that a non-stale read entered the syncing-follower
fallback after its local `ReadIndex` attempt failed. `RoutedController.readCtrl`
records that attempt before it resolves and marks the remote route. If
leadership moved local in the meantime, the router returns the failed barrier
rather than serving locally. Its magnitude differs sharply by cause:
`ErrNodeSyncing` returns before any wait, so it costs nanoseconds, whereas
leadership lost mid-`ReadIndex` resolves a pending future and can account for
the whole quorum attempt.

For handlers that still accept `min_log_sequence`, the same profile may also
contain an earlier `waitMinLogSequence` catch-up because that wait runs before
routing. A representative `forwarded=true, non-zero` request therefore uses
linearizable consistency with `min_log_sequence` on a syncing follower: the
profile combines the explicit sequence catch-up and the failed local
`ReadIndex` attempt, and does not attribute the total between them. In
contrast, `--consistency stale --min-log-sequence N` stays local; any sequence
wait it records belongs to the `forwarded=false` row.

So do not read cluster health off a non-zero forwarded barrier — the common cause
is a caller asking for read-your-writes.

Either way the wait is charged and stays excluded from `server_duration_us`, for
the same reason a successful barrier is: the caller waited for it, and an
abandoned quorum round-trip is no more server work than a completed one. Not
charging the failed attempt would leave it inside `execute_duration_us` — the
`readCtrl` call sits inside the `EnterExecute`/`LeaveExecute` bracket, so the
subtraction that removes barrier time from execution would find nothing to
subtract — and from there inside the server total, which is the distortion the
phase split exists to remove.

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

The profile is collected in full on **every** profiled read, not only when asked,
and there is exactly **one** measurement regime. The gate is the presence of a
profile in the context, never whether the caller asked to see it.

That is a correctness requirement, not a convenience. Nothing sends
`x-query-profile` in normal operation, so the slow-query log is the profile's only
production consumer. An earlier revision of this work gated the per-row
`execute`/`deliver` split on the caller having asked, and charged the whole send
loop to `deliver` otherwise — and since `deliver` is subtracted from the total,
`server_duration_us` was left blind to the entire loop in exactly the
configuration that reads it. A forwarded read spending seconds inside `cur.Next()`
reported sub-millisecond. Two conditional regimes also made gRPC and HTTP
incomparable when unprofiled, which contradicts the whole point of a shared
definition.

The cost of getting it right is two `time.Now()` per row — tens of microseconds
across a 1000-row page (`MaxPageSize`, the server-side ceiling), against a proto
marshal and a transport write per row. Unprofiled *handlers* (every read RPC other
than the four listed above) still pay nothing: `profile == nil` skips the clock
reads entirely.

## Slow-query threshold

`--query-profile-threshold` (default 10 ms) gates the trace-level profile log and
the span attributes. **0 disables it**; every duration is `>= 0`, so comparing
against 0 would otherwise log every single read.

It compares `WallDuration` — `server_duration_us + deliver_duration_us` — for the
reasons in "Two totals" above: `ServerDuration` alone cannot see row serialisation
or a forwarded read's remote cost. See `emitProfile` in
`internal/adapter/grpc/server_bucket.go`.

Two operational consequences:

- **The threshold now fires on strictly more reads than before EN-1859**, since
  `WallDuration >= ServerDuration >= TotalDuration` structurally. `EmitToSpan` is
  not behind a log level (only `span.IsRecording()`), so each qualifying read
  attaches ~14 attributes plus the rendered `query.iterator_tree` string to its
  span. On a read-heavy deployment that is a real increase in span payload
  volume — raise the threshold or set it to 0 if that matters more than the
  diagnostics.
- A slow *consumer* can trip it. That is intended; the logged
  `deliverDurationUs` / `firstRowDurationUs` / `serverDurationUs` breakdown
  identifies which side was slow.

`LogTo` also emits `forwarded`. When true, the remote node's whole cost sits
inside `executeDurationUs` and `barrierDurationUs` covers the local attempt only
(see the table above) — do not compare such a line's breakdown against a
locally-served one.

## Known gaps

- **Leader-forwarded reads report only the local hop.** When a follower falls
  back to the leader in `RoutedController.readCtrl` because it is syncing or its
  pending ReadIndex was invalidated by a leader change, the upstream RPC is
  charged to the local
  `execute` phase, so `execute` there conflates network hops, leader-side
  prepare, leader-side barrier and leader-side execution. `barrier_duration_us`
  covers only what this node attempted locally.

  The `forwarded` flag makes this **detectable** rather than silent, which is the
  part that mattered: a consumer can tell "no barrier" from "barrier not measured
  here". It does not make it *attributable*. Nesting the remote breakdown is
  deliberately deferred: the leader's `x-query-profile-result-bin` trailer is
  reachable from `BucketGrpcClient`, but consuming it means opting every routed
  read into upstream profiling, plumbing a trailer that only materialises at EOF
  out through the `cursor.Cursor` abstraction, and deciding how two phase
  breakdowns compose into one. That is a design question, not a fix.
- **HTTP body write** is not measurable (see above).
- **Unary gRPC responses** (`AggregateVolumes`, `ExecutePreparedQuery`) have no
  delivery phase: the reply is marshalled by the gRPC codec after the handler
  returns, past the point where the trailer is set.
- **HTTP reads do not feed the slow-query log.** `--query-profile-threshold`
  drives `emitProfile`, which only the gRPC handlers call. An HTTP read still
  reports its profile on request, but a slow one produces no threshold-triggered
  log line or span attributes. Pre-existing, not introduced by EN-1859.
