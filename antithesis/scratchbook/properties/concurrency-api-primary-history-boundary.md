# concurrency-api-primary-history-boundary — One request-scoped primary/history boundary with a fail-closed protocol

## Catalog entry

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | Every successful PIT aggregate binds a complete selector-specific view to the ledger incarnation and current log head read from one primary Pebble snapshot; the history view covers both that exact snapshot head and the caller's explicit minimum, and the body matches the independent monetary oracle at the returned cut. A typed PIT refusal remains an exact fail-closed protocol result, never a live or partial success. |
| **Invariant** | SUT `assert.AlwaysOrUnreachable(view != nil && requiredLogSequence == max(requestedMinLog, primarySnapshotLog) && view.LedgerID == primarySnapshotLedgerID && view.LogWatermark >= requiredLogSequence, "pit: history view covers the exact request primary snapshot", details)` immediately after the existing history `Open` succeeds. Workload `assert.AlwaysOrUnreachable(!success || (validPITProvenance(response, selector, requestedMinLog) && resultMatchesOracle(response.Body, response.View)), "pit: successful API response carries exact bound history", details)` and `assert.AlwaysOrUnreachable(success || noApplicationResponse || pitErrorMatchesExpectedContract(response, inducedState), "pit: failed API response stays typed and fail closed", details)`. `AlwaysOrUnreachable` fits an optional PIT path and fault windows with no application response; any observed success or typed response must satisfy its predicate. A uniquely named `Sometimes(success)` companion proves the successful boundary was exercised. |
| **Antithesis Angle** | Run acknowledged writes, same-name delete/recreate, stale-local reads, leader forwarding, builder lag, publication, compaction, tiering, and node/MinIO faults while requests cross the primary snapshot, history-watermark wait, immutable-view open, aggregation, and response-metadata serialization. Use one-attempt direct clients so a retry cannot pair a later body or failure with the request under test. |
| **Why It Matters** | A TOCTOU error can return plausible money for the wrong ledger incarnation or an older prefix. Missing provenance makes that answer unauditable; flattened or retried-away errors can turn repair-only corruption into silent approximation or a retry storm. |
| **Confidence** | High — all values needed for the exact internal assertion coexist in the current request without an extra read, and the success/error protocol is explicit in both adapters. The required PIT SDK assertions, raw no-retry workload clients, and monetary model are not implemented yet. |

**Open Questions:**

- None.

## Exact request-scoped oracle

The exact primary head is deliberately an in-request SUT oracle, not a second workload RPC:

1. `DefaultController.AggregateVolumes` opens one `dal.ReadHandle`. `NewReadHandle` is a Pebble snapshot, so `GetLedgerByName` and `ReadLastSequence` see one immutable primary state even while writes or ledger recreation continue.
2. Keep the existing `currentLogSequence` and computed `requiredLogSequence` locals. Immediately after `ctrl.volumeViews.Open` returns successfully, read the already-built `historicalView.Token()` and issue the SUT invariant above with the selector, requested minimum, snapshot ledger ID/head, required head, and returned ledger ID/watermarks in `details`.
3. This adds no storage read, RPC, pause, hook, request field, or response field. It cannot change which primary snapshot or history manifest the request uses. The assertion executes in the `DefaultController` that actually serves the request, including a leader reached through `RoutedController` forwarding.
4. Do not substitute a workload-side head read. A read before the PIT request is only a lower bound and a read after it is only an upper bound; concurrent commits mean neither identifies the snapshot created inside the request. A marker write supplies a useful `minLogSequence` lower bound, but it is not the exact `primarySnapshotLog` term.

The workload remains independently authoritative for public behavior. Use a bounded driver-owned ledger so no foreign driver mutates its model. Record the numeric ledger incarnation and every definitively resolved monetary effect with its returned log sequence and effective/insertion timestamp. To exercise a real race without admitting an ambiguous-write false positive, launch the PIT read beside an idempotency-keyed `Apply`, wait for both outcomes, and assert only when the write resolves definitively: include it when successful and `effect.logSequence <= view.logWatermark`, exclude it on a definitive rejection, and treat an unresolved transport outcome as inconclusive. Apply the same join-before-assert rule to delete/recreate. Replay the resulting effects for the returned `ledgerId` and selected axis/timestamp, then compare arbitrary-precision input/output/balance buckets exactly. This validates the body at the advertised immutable cut while the SUT assertion proves that cut covers the otherwise unobservable request snapshot.

## Merged successful-response provenance subchecks

Evaluation R1 removes the former standalone provenance candidate; retain its low-cost checks in every successful boundary sample:

- gRPC: capture raw metadata with `grpc.Trailer`, require exactly one `x-point-in-time-view-bin` value, and decode exactly one `PointInTimeView`.
- HTTP: require exactly one `X-Point-In-Time-View` value, standard-base64 decode it, and decode exactly one `PointInTimeView`.
- On both transports require the exact requested microsecond and axis, the expected non-zero ledger incarnation, `logWatermark >= requestedMinLog`, a non-zero manifest version after first publication, a present availability-floor timestamp, and a non-empty opaque token. Audit watermark is compared with the oracle's accepted prefix and is not assumed non-zero for an empty history.
- Require the monetary body to match the driver oracle at the returned ledger incarnation, selector, and audit/log watermarks. Never compare physical manifest versions or opaque tokens across different replicas unless a separate property first proves the responses refer to the same immutable view.
- Successful live controls must carry neither the PIT gRPC trailer nor the PIT HTTP header. Failed PIT responses must not carry a successful body or PIT provenance.

The transport's missing-view, duplicate/malformed trailer, empty-token, unknown-axis, and selector-mismatch branches already have deterministic tests. Optional SUT `Unreachable` messages at the gRPC/HTTP missing-view and selector-mismatch branches are diagnostic guidance, not substitutes for the workload predicates above.

## Merged fail-closed error subchecks

Evaluation R1 also merges the former standalone error-contract candidate. A single-target, one-attempt probe built with gRPC retries disabled and no workload retry interceptor must inspect the exact response before the ordinary workload retry interceptors can consume `Unavailable`. Treat a transport timeout, disconnect, or node-unreachable outcome as `noApplicationResponse`, not as proof of a typed server state. When the driver has induced a known PIT state and an application response arrives, require this complete mapping:

| Induced PIT state | gRPC code | `ErrorInfo.reason` and metadata | HTTP status/reason | Retry hint |
|---|---|---|---|---|
| Building | `Unavailable` | `HISTORY_BUILDING`; exact `currentLogSequence`, `targetLogSequence` | 503 / `HISTORY_BUILDING` | HTTP `Retry-After: 1` |
| Behind | `Unavailable` | `HISTORY_BEHIND`; exact `currentLogSequence`, `requiredLogSequence` | 503 / `HISTORY_BEHIND` | HTTP `Retry-After: 1` |
| Expired | `FailedPrecondition` | `HISTORY_EXPIRED`; exact requested/floor microseconds | 400 / `HISTORY_EXPIRED` | none |
| Source missing | `Internal` | `HISTORY_SOURCE_MISSING`; no diagnostic metadata | 500 / `HISTORY_SOURCE_MISSING` | none |
| Corrupt, quarantined, or unsupported history format | `Internal` | `HISTORY_CORRUPT`; no diagnostic metadata | 500 / `HISTORY_CORRUPT` | none |
| Unsupported temporal filter | `InvalidArgument` | `UNSUPPORTED_TEMPORAL_FILTER`; bounded `filterCategory` when known | 400 / `UNSUPPORTED_TEMPORAL_FILTER` | none |

These mappings are safety subchecks whenever their states occur; they are not all Antithesis reachability goals. In format version 1, `HISTORY_EXPIRED` stays in the wire-contract predicate but has no `Sometimes` assertion because a valid non-zero floor cannot be constructed. Unsupported filters are deterministic integration coverage, not an Antithesis scheduling target. Source-missing/corrupt reachability belongs to a focused campaign with an exact-object helper and durable MinIO; the main boundary driver must not broadly tolerate every gRPC `Internal` response.

## Repository evidence

### Primary/history boundary

- `internal/storage/dal/reader.go:39-70` defines `NewReadHandle` as a Pebble point-in-time snapshot held for the request lifetime; `internal/storage/dal/reader_test.go:73-99` proves later writes are invisible through it.
- `internal/application/ctrl/controller_default.go:956-993` resolves ledger name/ID and the last global log sequence through that one handle, computes `max(read.MinLogSequence, currentLogSequence)`, and passes both snapshot identity and the computed floor to the history provider.
- `internal/query/log.go:22-45` derives the primary head from the last log visible through the supplied reader.
- `internal/application/ctrl/volume_view.go:74-117` waits for the required log watermark, opens a pinned history view, and constructs the token from the caller-supplied snapshot ledger ID and the pinned manifest.
- `internal/storage/balancehistorystore/store.go:527-552` waits on failure state and manifest coverage. `internal/storage/balancehistorystore/view.go:181-220` rechecks completeness/minimum under `mutationMu`, then pins the Pebble snapshot and manifest lease before releasing the lock.
- `internal/bootstrap/controller_routed.go:50-101,274-286` selects stale-local, local-linearizable, or leader-forwarded controllers before invoking `AggregateVolumes`; the exact SUT check therefore runs where the serving primary snapshot exists.
- `misc/proto/bucket.proto:1262-1274` exposes immutable-view provenance but no primary-snapshot-head field. No existing API can reveal the controller's exact internal snapshot after the fact.

### Provenance and error protocol

- `internal/adapter/grpc/point_in_time.go:70-162` serializes, requires exactly one, decodes, and selector-validates the binary view trailer; `internal/adapter/grpc/server_bucket.go:1429-1445` fails a PIT success with absent or mismatched provenance.
- `internal/adapter/http/handlers_aggregate_volumes.go:123-166,234-263` applies retry hints only to building/behind, fails closed on absent/mismatched provenance, and emits the base64 protobuf header.
- `cmd/ledgerctl/accounts/aggregate.go:260-302` independently rejects missing, duplicate, incomplete, unknown-axis, or selector-mismatched gRPC provenance.
- `openapi.yml:980-1037` documents the success header and PIT 400/503/500 families.
- `internal/storage/balancehistorystore/errors.go:11-177` defines stable reasons and bounded metadata; internal source/corruption diagnostics expose no metadata.
- `internal/domain/reason.go:107-138`, `internal/adapter/grpc/errors.go:31-80`, and `internal/adapter/http/error_handler.go:15-43,94-101` map those semantic kinds to the gRPC and HTTP statuses above, including wrapped `domain.Describable` errors.
- `internal/storage/balancehistorystore/errors_test.go:12-109`, `internal/adapter/grpc/server_bucket_aggregate_volumes_test.go:43-271`, and `internal/adapter/http/handlers_aggregate_volumes_test.go:111-381,503-566` pin the deterministic field, absence, mismatch, reason, status, and retry behavior.
- `tests/antithesis/workload/internal/client.go:22-118,157-221` installs transparent `Unavailable` retries on ordinary clients, while `tests/antithesis/workload/internal/pernode.go:82-175` installs the same retries on direct-node clients. The boundary/error probe needs a new single-target variant with neither retry policy nor retry interceptor.

## SDK instrumentation status

- **Existing:** no PIT-specific SDK assertion; `existing-assertions.md` records zero PIT instrumentation. Deterministic gRPC/HTTP/client tests cover protocol serialization and mappings, but not the primary/history TOCTOU interval under faults.
- **Missing SUT safety assertion:** the exact `AlwaysOrUnreachable` after successful history `Open`, using only locals and token fields already produced by the request.
- **Missing SUT guidance:** `assert.Reachable("pit: API waited for history after pinning a primary snapshot", details)` when the computed required head initially exceeds the history watermark. Optional transport-specific `Unreachable` assertions may distinguish missing and selector-mismatched successful provenance.
- **Missing workload support:** a raw trailer-aware gRPC probe, a direct HTTP probe, a single-target dial variant that disables gRPC retries and installs no retry interceptor, a driver-owned ledger prefix, and an arbitrary-precision effect oracle keyed by ledger incarnation, selector axis/timestamp, and returned watermarks.
- **Missing workload assertions:** the successful-result/provenance and typed-failure assertions above, live-control metadata absence, plus `assert.Sometimes(success, "pit: request-scoped primary and history boundary is exercised successfully", details)`.

### Investigation Log

#### What workload-visible barrier exposes the exact per-node primary snapshot log head without perturbing the request?

- Examined: `DefaultController.AggregateVolumes`; `dal.NewReadHandle`; `query.GetLedgerByName` and `ReadLastSequence`; `LocalVolumeViewProvider.Open`; `RoutedController.readCtrl`; gRPC/HTTP request and response schemas; workload barrier, per-node, retry, and owned-ledger helpers; controller, adapter, DAL, forwarding, and PIT provider tests; and the implementability evaluation for this property.
- Found: ledger ID and last log sequence are read through the same immutable Pebble snapshot. The exact values coexist with the computed required head and returned history token immediately after the existing `Open` succeeds. A SUT assertion there uses no additional read and does not change request scheduling or response semantics. The public view exposes the history watermark and ledger ID but intentionally has no primary-snapshot-head field.
- Not found: any response field, diagnostic API, or workload RPC that identifies the exact snapshot created inside the request. A caller marker/minimum proves only a lower bound; separate head reads before and after the request observe different snapshots under concurrent writes.
- Conclusion: resolved. Use the in-request SUT `AlwaysOrUnreachable` as the exact head/incarnation oracle and the no-retry workload result/provenance checks as its public consequence. Do not add a test-only status surface or weaken `primarySnapshotLog` to the caller's minimum.

#### Which provenance and error checks must survive removal of the standalone protocol properties?

- Examined: `PointInTimeView` protobuf/OpenAPI definitions; controller token construction; gRPC server/client/trailer handling; HTTP header/error handling; CLI validation; typed PIT errors and status-kind mappings; deterministic adapter tests; `existing-assertions.md`; and evaluation synthesis R1 plus the Antithesis-fit assessment.
- Found: completeness, selector matching, exactly-one metadata, live-response absence, stable error codes/reasons/metadata, and HTTP retry hints are cheap public subchecks of the main monetary boundary. Most malformed/mismatch branches are already deterministic tests; their Antithesis value is preventing concurrency/forwarding from separating the body and out-of-band metadata. Ordinary workload clients hide `HISTORY_BUILDING`/`HISTORY_BEHIND` behind retries, and the global classifier rejects the repair-only `Internal` reasons.
- Not found: any existing PIT-specific SDK assertion, raw no-retry per-node PIT client, or reason-specific workload predicate. These are implementation gaps, not unresolved contract questions.
- Conclusion: resolved by merging one success/provenance predicate and one typed-failure predicate into this property. Keep deterministic shape matrices in ordinary tests and use Antithesis reachability only for fault-dependent runtime states.

#### Should a format-version-1 campaign require `HISTORY_EXPIRED` reachability?

- Examined: `internal/storage/balancehistorystore/types.go`, `publish.go`, `verify.go`, `view.go`, `floor_reserved_test.go`, every repository assignment to `EffectiveFloor` or `InsertionFloor`, the builder publication path, and the history-floor design section in `docs/technical/architecture/subsystems/read-path/point-in-time-balances.md`.
- Found: format version 1 initializes both floors to zero; normal publication rejects either floor when non-zero; the builder only carries the existing zero values forward; verification rejects a persisted non-zero floor as corruption; and a valid view cannot satisfy `requested uint64 < floor` when the floor is zero. Repository tests cover publication, immediate-read, verification, and restart rejection.
- Not found: any base-import API, migration authority, supported manifest format, configuration switch, or lifecycle path that creates a valid non-zero history floor at this commit.
- Conclusion: resolved. Retain `HISTORY_EXPIRED` in the safety mapping as a reserved compatibility contract, but do not add a permanently false format-v1 `Sometimes(observedExpired)` assertion. Add reachability only when a future supported format can create and independently verify a non-zero floor.
