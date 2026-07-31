# transient-minio-failure-recovers-without-sticky-source-loss — Transient MinIO I/O never becomes durable source loss

## Catalog candidate

| | |
|---|---|
| **Priority** | P0 — a temporary dependency fault can currently become a synchronously persisted, replica-wide PIT failure and combine with the existing non-builder repair-handoff gap. |
| **Type** | Liveness, with an exact safety transition predicate. |
| **Property** | A MinIO failure proven to be transport/checksum-read I/O, with the referenced object and checksum unchanged, never creates a new persistent `SOURCE_MISSING` or corruption marker; the direct cold-read outcome is exact success or `EXTERNAL_SERVICE_ERROR`, and after the fault heals the same Ledger process again serves exact PIT. Proven object/checksum absence remains a distinct persistent source-repair case. |
| **Invariant** | SUT safety: `AlwaysOrUnreachable(transientMinIOErrorObserved => failureStateAfter == failureStateBefore, "pit: transient MinIO I/O does not persist history source loss")`. Public mapping: `AlwaysOrUnreachable(directColdReadTransportFailure => reason == "EXTERNAL_SERVICE_ERROR", "pit: transient cold read remains an external service error")`. Recovery: `Sometimes(transientMinIOErrorObserved && sameProcessValidatedSameObjectAfterHeal && exactPITSucceeded, "pit: transient MinIO failure recovers without process restart")`. `AlwaysOrUnreachable` fits optional fault paths whose every occurrence must preserve durable state; `Sometimes` expresses required same-process progress after a reached and healed fault. |
| **Antithesis Angle** | Build and tier history across real archived primary chapters, force a primary-chapter checksum revalidation or a PIT-run cold-cache miss, then overlap only Ledger↔MinIO network/hang/throttle faults with `HeadObject`, `GetObject`, checksum metadata read, response-header wait, or body read. Keep the MinIO process and data intact, heal the link, and require the same process to validate the same object identity/checksum and serve the oracle result. Use an explicit delete/missing-metadata control only for the separate source-repair property. |
| **Why It Matters** | Dependency reachability is not proof that authoritative bytes are absent. Persisting `SOURCE_MISSING` closes every PIT read on that replica and requires full-prefix certification; when the verifier originated the marker, current builder-local state can leave it stuck until restart. |
| **Confidence** | High for the code-level misclassification and sticky same-process chain; medium for remote campaign reachability until the PIT profile and SUT fault-origin signals are implemented. |

**Open Questions:**

- None.

## Exact state machine

The property distinguishes evidence, not merely error text:

| Observed condition | Allowed immediate state/outcome | Durable transition |
|---|---|---|
| Fault injector and retained error chain prove timeout, connection/reset, S3 5xx, or other operational I/O failure; object identity was not changed | Existing exact cached result, `HISTORY_BUILDING`/`HISTORY_BEHIND` where no ready view exists, or direct-call `EXTERNAL_SERVICE_ERROR` | No new `SOURCE_MISSING`, `QUARANTINED`, or `REBUILDING` marker |
| A normal S3 response proves the exact object or its committed checksum metadata is absent | `HISTORY_SOURCE_MISSING` | Persistent `SOURCE_MISSING` is allowed; recovery belongs to `recovery-source-missing-heals-same-process` |
| Expected and calculated checksums are both read successfully and differ, or bytes fail codec/content-address validation | `HISTORY_CORRUPT` | Quarantine/rebuild is allowed |
| Transient fault heals without object mutation or Ledger restart | Exact PIT at the required watermark from the same process | Prior failure state remains unchanged; no repair certification is needed solely because transport was unavailable |

The same-process clause is not satisfied by pod readiness or a replacement with the same ordinal. The SUT assertion must correlate the failed and successful validation in one process-local record; workload evidence should also retain the pod UID and container restart count.

## Confirmed current failure chain

The property is expected to fail at this commit:

1. `HotColdSource.archiveReader` validates an archived authoritative chapter through `verifyArchiveChecksum` before acquiring or after re-fetching its cold reader.
2. `verifyArchiveChecksum` converts **every** `ExpectedChecksum` error and **every** calculated-checksum read error into application `ErrSourceMissing`, formatting the cause with `%v`. A Smithy transport/API chain is therefore erased before the gRPC external-service mapper can see it.
3. Builder-originated errors enter `Builder.handleBuildError`, which synchronously persists `SOURCE_MISSING`, sets the builder repair atomics, and schedules a genesis rebuild. It may heal in the same process, but it still violates the classification predicate and performs unnecessary destructive repair.
4. More dangerously, a full verifier replay can encounter the same error. `markReplayFailure` treats every non-cancellation, non-scratch error as source loss and calls `Store.MarkSourceMissing`.
5. `MarkSourceMissing` writes the marker with `pebble.Sync`, invalidates views, and causes subsequent requests to return `HISTORY_SOURCE_MISSING`.
6. A verifier-originated marker does not update the already-ready builder's `sourceMissing` atomic. At an unchanged head the builder can take its caught-up early return; verifier success proves content but deliberately does not clear the marker. Healing MinIO alone can therefore leave the replica fail closed until restart.

The cold PIT-run path is inconsistent in a second useful way. Initial `Exists`/`GetObject` transport failures retain the operational error and can reach `EXTERNAL_SERVICE_ERROR`, while an operational failure on the subsequent expected-checksum read is wrapped as `balancehistoryarchive.CorruptError` by `download`/`verifyRemote`, allowing a temporary failure to become quarantine instead. The property intentionally forbids either persistent failure kind when absence or byte divergence was not proven.

## Code evidence

- `internal/application/balancehistory/source_hotcold.go:753-797` identifies the checksum verification boundaries on initial and re-fetched authoritative chapter readers.
- `internal/application/balancehistory/source_hotcold.go:800-825` maps every checksum-metadata/read error to `ErrSourceMissing`, maps only a successfully calculated mismatch to `ErrSourceInvalid`, and discards the underlying error chain with `%v`.
- `internal/application/balancehistory/builder.go:610-658` documents the intended distinction between persistent proven data failures and retried unclassified operational failures, but persists any source error supplied by the source adapter.
- `internal/application/balancehistory/builder.go:556-589,705-748` shows the ready/caught-up early return and that only builder-owned `sourceMissing` state reaches `ClearFailure` after certification.
- `internal/application/balancehistory/verifier.go:535-586,732-779,868-886` replays the authoritative source and persists every non-cancellation replay failure as source missing, without classifying infrastructure errors.
- `internal/storage/balancehistorystore/store.go:303-375` writes `SOURCE_MISSING` with `pebble.Sync`, stores it in memory, advances generation, and invalidates current views.
- `internal/storage/balancehistorystore/view.go:256-268` makes the persisted marker fail all ordinary reads closed; `view.go:574-602` persists only errors mapped to store `ErrSourceMissing` and otherwise preserves operational errors.
- `internal/storage/balancehistorystore/verify.go:723-738` has the correct high-level shape: only archive `ErrMissing` becomes source missing, integrity sentinels become corrupt, and unclassified errors remain operational.
- `internal/storage/balancehistoryarchive/store.go:197-222` preserves `Exists` and `GetObject` operational errors but maps `ErrChecksumNotFound` to missing and every other expected-checksum error to `CorruptError`. `store.go:265-298` already preserves non-malformed operational checksum errors in `Available`; the inconsistent policies are not required by the archive interface.
- `internal/storage/balancehistoryarchive/store.go:326-348` similarly converts any expected/calculated checksum I/O error in remote verification to corruption, even when no mismatch was observed.
- `internal/infra/coldstorage/s3.go:113-180,183-205` distinguishes typed `NoSuchKey`/`NotFound` from other S3 errors and preserves operational causes. `ExpectedChecksum` uses `ErrChecksumNotFound` for an absent object/metadata and `ErrChecksumMalformed` for invalid metadata.
- `internal/infra/coldstorage/coldstorage.go:13-22,46-74` defines absence/malformed checksum sentinels and does not claim that arbitrary I/O errors prove missing or corrupt content.
- `internal/adapter/grpc/server.go:582-630` maps retained Smithy `APIError` and `OperationError` chains to `EXTERNAL_SERVICE_ERROR`; conversion to a domain `ErrSourceMissing`/`ErrCorrupt` wins earlier and prevents this mapping.
- `tests/antithesis/workload/internal/client.go:407-410,462-482` already recognizes `EXTERNAL_SERVICE_ERROR` and treats it as retry-safe, while `existing-assertions.md` confirms there are no PIT SUT assertions or no-retry reason-specific PIT probes.
- `docs/technical/architecture/subsystems/read-path/point-in-time-balances.md:512-519,718-733,880-882` reserves persistent source loss for an absent required range and corruption for checksum-invalid content; neither statement equates dependency transport failure with missing bytes.
- `internal/application/balancehistory/source_hotcold_test.go:122-183` covers actual missing checksum/archive and calculated mismatch, but no test injects an operational `ExpectedChecksum` or `Checksum` error. Verifier tests cover explicit source gaps, not transport classification.

## Required instrumentation and workload oracle

Existing status: **missing**. No PIT SDK assertion records the raw MinIO failure class, the marker kind before/after an operation, or recovery against the same object in one process.

1. At the common cold-storage classification boundary, record a bounded class: `absent`, `integrity`, `operational`, or `cancelled`, plus operation (`head`, `expected-checksum`, `checksum-body`, `fetch`) and origin (`builder-source`, `verifier-source`, `pit-cold-view`). Preserve error unwrapping so Smithy operational errors remain distinguishable.
2. Snapshot the store failure kind before an operational error is returned and assert after classification that it did not advance. Emit `pit: transient MinIO I/O does not persist history source loss` from one callsite only.
3. Keep a process-local, bounded recovery token containing origin, object identity (chapter ID or content digest), and pre-fault expected checksum. When the same operation validates the same identity/checksum later in that process, emit `pit: transient MinIO failure recovers without process restart` and clear the token.
4. Add a no-retry, direct-per-node PIT probe. During a direct cold-view operational failure it must assert the exact `EXTERNAL_SERVICE_ERROR`; after heal it compares the successful result and trailer to the independent oracle at the required watermark.
5. Treat attempts as inconclusive unless the SUT operational-failure signal was reached. Do not infer the premise from `HISTORY_SOURCE_MISSING`, pod health, a generic gRPC error, or MinIO unavailability alone.
6. Run this in the `pit-core-cold` profile with MinIO termination and object mutation disabled. Churn the byte-bounded PIT archive cache for query coverage; for authoritative source coverage, use a fresh reader or exceed the primary `ColdReader`'s eight-entry cache so checksum revalidation is actually reached.

No new public diagnostic is required. SUT SDK details plus the existing per-node client and exact `ErrorInfo.reason` decoder are sufficient; the public contract remains the typed error and exact monetary result.

## Relationship to existing properties

- This property prevents an operational error from entering durable repair state.
- `recovery-source-missing-heals-same-process` begins only after source absence has been proven and a valid `SOURCE_MISSING` marker exists; it remains necessary for deletion/restoration and for any legitimate non-builder marker origin.
- `integrity-cold-content-verified` governs actual missing or divergent bytes and forbids partial success.
- The error-contract predicates merged into
  `concurrency-api-primary-history-boundary` pin public wire mappings; this
  property supplies the fault classification that selects
  `EXTERNAL_SERVICE_ERROR` rather than a repair-only PIT reason.

### Investigation Log

#### Which MinIO failures prove source absence, and which are merely operational?

- **Examined:** `ColdStorage` sentinels/interface, S3 `HeadObject`/`ExpectedChecksum`/`Checksum`/`Fetch`, the PIT archive `download`/`Available`/`verifyRemote` paths, `mapArchiveError`, authoritative `verifyArchiveChecksum`, and the architecture error contract.
- **Found:** a typed negative object lookup or absent committed checksum metadata provides absence evidence; a successfully read but malformed/mismatched checksum or invalid content provides integrity evidence. Timeouts, connection failures, service 5xx responses, and body-read errors prove neither. The S3 adapter preserves this distinction, but the authoritative source adapter and some PIT archive checksum paths collapse it.
- **Not found:** any repository contract stating that an arbitrary checksum I/O failure is source absence/corruption, or any common classifier that intentionally authorizes those conversions.
- **Conclusion:** resolved. The property guard uses the observed/injected operational class and unchanged object identity, not the eventual public error, to avoid circularly accepting the current misclassification.

#### Can a transient verifier checksum failure become sticky in one running process?

- **Examined:** verifier replay/error handling, store failure persistence, builder tick/error handling, builder restart-state restoration, repair certification/clearing, all production callers of `MarkSourceMissing` and `ClearFailure`, and the existing same-process source-repair evidence.
- **Found:** the verifier converts the authoritative source's transport-as-missing error into a synchronous persistent marker. It does not update builder atomics; only the builder clears the marker; a ready/caught-up builder can return before certification. Restart reloads the marker and does initiate repair.
- **Not found:** a verifier-to-builder notification, durable-marker polling by an already-ready builder, or verifier authority to clear the marker after a successful later pass.
- **Conclusion:** resolved as a current code-level failure chain. This property is known-failing until operational errors stop creating the marker; the broader existing repair-handoff property remains independently known-failing for legitimate non-builder source-loss markers.

#### How can Antithesis prove same-process recovery without confusing it with restored object loss or pod replacement?

- **Examined:** the core-cold topology/fault policy, MinIO manifest durability, per-node client, pod UID helpers/RBAC, archive caches, current assertion inventory, and the existing source-repair property.
- **Found:** the campaign can exclude MinIO termination/object mutation, guard on a SUT-observed operational error, and require later validation of the same chapter/digest and checksum. A process-local recovery token is stronger than pod UID because a container can restart inside one pod; the workload can additionally verify unchanged UID/restart count and exact result.
- **Not found:** an existing process-correlated PIT assertion or workload-visible failure-marker API.
- **Conclusion:** resolved with SDK-only SUT correlation plus the public no-retry oracle. A test-only object delete/restore helper is unnecessary and would test the different proven-loss property.
