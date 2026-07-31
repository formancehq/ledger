# out-of-order-chapters-fail-closed — Out-of-order archived chapters never yield partial PIT success

## Candidate catalog entry

- **Type:** Safety
- **Priority:** P0
- **Assertion:** `Always` — while chapter N remains hot/non-archived and N+1 is
  archived, a PIT request either returns the complete oracle result or an exact
  fail-closed state; it never interprets the routing mismatch as missing
  monetary effects or zero.
- **Confidence:** High that the topology is accepted by the FSM and rejected by
  the PIT source; runtime outcome remains to be reproduced.

## Evidence

The chapter processor accepts archive/confirm for any eligible chapter ID and
does not require an archived prefix. `HotColdSource.validateChapterTopology`
rejects an archived chapter after a non-archived chapter. Builder handling turns
source-invalid into quarantine/reset/rebuild.

Relevant paths:

- `internal/domain/processing/processor_chapter.go:96-151`
- `internal/application/balancehistory/source_hotcold.go:217-275`
- `internal/application/balancehistory/builder.go:621-676`
- `internal/application/balancehistory/source_hotcold_test.go:78`

The deterministic source tests do not cover this valid chapter-state order.

## Workload

Create and close at least three chapters, request archive of N+1 before N, and
wait for the real archiver-produced state. Query timestamps whose effects cross
both chapters. Any success is compared to the independent oracle and trailer;
`HISTORY_SOURCE_MISSING`/`HISTORY_CORRUPT` are visible failures, not silently
tolerated global `Internal` responses.

## Instrumentation status

No PIT source-topology SDK assertion exists. Add a `Reachable` signal for this
specific order and the workload `Always` for public safety. Existing chapter
assertions do not validate cold readability or PIT.

## Open questions

- None. This property deliberately permits fail-closed availability while the
  mixed topology exists; the product decision to support it efficiently is
  separate from monetary safety.
