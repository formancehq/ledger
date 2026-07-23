package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// The archival cycle drives chapter archival online, alongside the workers and
// the restore cycle: it closes the current chapter, waits for the background
// Sealer to seal it, archives it, and waits for the background Archiver to
// upload it to cold storage and confirm — which purges the chapter's logs/audit
// from hot storage. It issues plain RPCs against the live node; chapter orders
// are NOT modeled bulks (they create no transactions and touch no business state
// the oracle tracks), and the checker orders only by relative log sequence, so
// the sequence gaps these foreign orders leave in the driver's own bulks are
// harmless. The coverage this unlocks — the model's reads/commits validating a
// node whose history has been archived+purged, and (combined with the restore
// cycle) a rebuild seeded from the baseline checkpoint — is checked by the
// ordinary validation paths, so no separate comparison is needed here.

// selectArchivalInterval returns the configured archival-cycle interval, or 0
// when archival is not enabled for this run (MODEL_ARCHIVE_INTERVAL unset).
func selectArchivalInterval() time.Duration {
	raw := os.Getenv("MODEL_ARCHIVE_INTERVAL")
	if raw == "" {
		return 0
	}

	return time.Duration(envInt("MODEL_ARCHIVE_INTERVAL", defaultArchiveIntervalSecs)) * time.Second
}

// runArchivalCycle archives a chapter on a jittered interval until ctx ends. It
// parks while a restore cycle has quiesced the node so its RPCs don't race the
// node teardown/swap; any RPC that still lands mid-teardown fails transiently
// and is tolerated (logged, skipped) — only wrong state, caught by the ordinary
// checks, is a finding.
func runArchivalCycle(ctx context.Context, client servicepb.BucketServiceClient, c *Checker, interval time.Duration) {
	for {
		jitter := time.Duration(internal.Rand().Int63n(int64(interval/2) + 1))
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval + jitter):
		}

		if !c.awaitResume(ctx) {
			return
		}

		archived, err := archiveOneChapter(ctx, client)
		if err != nil {
			log.Printf("archival cycle: %v (continuing)", err)

			continue
		}

		if archived {
			// The report-visible proof that archival coverage actually ran: a
			// green run where this never fired exercised nothing (mirrors the
			// restore-cycle-completed assert).
			assert.Sometimes(true, "singleton_driver_model: chapter archived", internal.Details{})
			log.Printf("archival cycle: chapter archived + purged")
		}
	}
}

// archiveOneChapter closes the current chapter, waits for it to seal, archives
// it, and waits for it to reach ARCHIVED (confirmed + purged). archived is false
// (with no error) when a step could not complete within its window or hit a
// transient — a skipped cycle, retried next tick. A non-transient RPC failure is
// returned as an error.
func archiveOneChapter(ctx context.Context, client servicepb.BucketServiceClient) (archived bool, err error) {
	if _, applyErr := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CloseChapterAction())); applyErr != nil {
		if internal.IsTransient(applyErr) || isShutdownError(applyErr) {
			return false, nil
		}

		return false, fmt.Errorf("closing chapter: %w", applyErr)
	}

	closedID, err := waitChapterStatus(ctx, client, 0, commonpb.ChapterStatus_CHAPTER_CLOSED, archiveSealTimeout)
	if err != nil {
		return false, err
	}
	if closedID == 0 {
		return false, nil
	}

	if _, applyErr := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.ArchiveChapterAction(closedID))); applyErr != nil {
		if internal.IsTransient(applyErr) || isShutdownError(applyErr) {
			return false, nil
		}

		return false, fmt.Errorf("archiving chapter %d: %w", closedID, applyErr)
	}

	// The background Archiver auto-proposes ConfirmArchiveChapter after the cold
	// upload; wait for ARCHIVED so the coverage assert reflects a completed purge.
	confirmedID, err := waitChapterStatus(ctx, client, closedID, commonpb.ChapterStatus_CHAPTER_ARCHIVED, archiveConfirmTimeout)
	if err != nil {
		return false, err
	}

	return confirmedID != 0, nil
}

// waitChapterStatus polls until a chapter reaches want, bounded by timeout. When
// id is non-zero it watches that specific chapter; when id is zero it returns
// the first chapter found in want. Returns the chapter id on success, 0 on
// timeout/shutdown/transient (a skipped cycle). A non-transient list error is
// returned.
func waitChapterStatus(ctx context.Context, client servicepb.BucketServiceClient, id uint64, want commonpb.ChapterStatus, timeout time.Duration) (uint64, error) {
	deadline := time.Now().Add(timeout)

	for {
		chapters, err := actions.ListAllChapters(ctx, client)
		if err != nil {
			if internal.IsTransient(err) || isShutdownError(err) {
				return 0, nil
			}

			return 0, fmt.Errorf("listing chapters: %w", err)
		}

		for _, ch := range chapters {
			if id != 0 && ch.GetId() != id {
				continue
			}
			if ch.GetStatus() == want {
				return ch.GetId(), nil
			}
		}

		if time.Now().After(deadline) {
			return 0, nil
		}

		select {
		case <-ctx.Done():
			return 0, nil
		case <-time.After(archivePoll):
		}
	}
}
