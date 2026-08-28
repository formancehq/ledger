package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// checkpointRecord is one live query checkpoint under observation: the frozen
// page fingerprinted right after creation, and how many identical re-reads it
// has served since.
type checkpointRecord struct {
	id          uint64
	ledger      string
	fingerprint string
	probes      int
}

// maxTrackedCheckpoints stays below the server's hard cap (10 live
// checkpoints, EN-1501) so creation failures are the exception path, not the
// steady state.
const maxTrackedCheckpoints = 3

// runCheckpointCycle exercises the query-checkpoint lifecycle: create a
// checkpoint, fingerprint a frozen accounts page through it, then keep
// re-reading that page — a checkpoint is a fixed snapshot, so any later read
// must serve the byte-identical page until the checkpoint is deleted (or
// vanishes with a cluster restore, which reads as NotFound and retires the
// record). Serving a DIFFERENT page is the finding: live state leaked into a
// frozen read.
func runCheckpointCycle(ctx context.Context, c *Checker, client servicepb.BucketServiceClient, cluster clusterpb.ClusterServiceClient) {
	var records []*checkpointRecord

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if len(records) < maxTrackedCheckpoints && random.RandomChoice([]uint8{0, 1}) == 0 {
			if rec := createAndFingerprint(ctx, c, client, cluster); rec != nil {
				records = append(records, rec)
			}

			continue
		}

		if len(records) == 0 {
			continue
		}

		i := int(random.GetRandom() % uint64(len(records)))
		rec := records[i]

		switch probeCheckpoint(ctx, client, rec) {
		case probeGone:
			records = append(records[:i], records[i+1:]...)
		case probeOK:
			rec.probes++
			if rec.probes >= 3 {
				// Retire it. Deletion is a proposal that folds asynchronously,
				// so no read-back assertion: a serve shortly after the ACK can
				// legally still see the checkpoint.
				dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				_, _ = cluster.DeleteQueryCheckpoint(dctx, &clusterpb.DeleteQueryCheckpointRequest{CheckpointId: rec.id})
				cancel()

				records = append(records[:i], records[i+1:]...)
			}
		case probeSkip:
		}
	}
}

// createAndFingerprint creates a checkpoint and takes its frozen page. A
// creation refused by the live-checkpoint cap is legal (another creator, or
// retired records the server still holds); it retires nothing and just skips
// the round.
func createAndFingerprint(ctx context.Context, c *Checker, client servicepb.BucketServiceClient, cluster clusterpb.ClusterServiceClient) *checkpointRecord {
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	resp, err := cluster.CreateQueryCheckpoint(cctx, &clusterpb.CreateQueryCheckpointRequest{})

	cancel()

	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) || status.Code(err) == codes.FailedPrecondition || status.Code(err) == codes.ResourceExhausted {
			return nil
		}

		assert.Unreachable("singleton_driver_model: CreateQueryCheckpoint returned unexpected error", internal.Details{
			"error": err.Error(),
		})

		return nil
	}

	rec := &checkpointRecord{id: resp.GetCheckpointId(), ledger: random.RandomChoice(c.ledgerNames)}

	// The checkpoint materializes asynchronously; not-ready answers are
	// retryable until the first page comes back.
	for attempt := 0; attempt < 5; attempt++ {
		page, err := checkpointPage(ctx, client, rec.ledger, rec.id)
		if err == nil {
			rec.fingerprint = page
			// Coverage: a frozen page was captured; immutability probes follow.
			assert.Reachable("singleton_driver_model: query checkpoint fingerprinted", internal.Details{"ledger": rec.ledger})

			return rec
		}

		if status.Code(err) == codes.NotFound {
			return nil // wiped under us (restore); nothing to observe
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}
	}

	return nil
}

type probeResult int

const (
	probeOK probeResult = iota
	probeGone
	probeSkip
)

// probeCheckpoint re-reads the record's frozen page. Identical → OK; NotFound
// → the checkpoint is gone (deletion elsewhere or a cluster restore), which
// retires the record; any other error is a skipped round. A page that differs
// from the fingerprint is the invariant violation.
func probeCheckpoint(ctx context.Context, client servicepb.BucketServiceClient, rec *checkpointRecord) probeResult {
	page, err := checkpointPage(ctx, client, rec.ledger, rec.id)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return probeGone
		}

		return probeSkip
	}

	if page != rec.fingerprint {
		assert.Unreachable("singleton_driver_model: query checkpoint served a mutated page", internal.Details{
			"ledger":       rec.ledger,
			"checkpointId": rec.id,
			"probes":       rec.probes,
			"fingerprint":  rec.fingerprint,
			"page":         page,
		})

		return probeGone
	}

	// Coverage: a frozen page re-read identically.
	assert.Reachable("singleton_driver_model: query checkpoint immutability probe passed", internal.Details{"ledger": rec.ledger})

	return probeOK
}

// checkpointPage reads one deterministic accounts page through the
// checkpoint: unfiltered, forward, fixed size, rendered with sorted volumes
// and metadata so identical states render identically.
func checkpointPage(ctx context.Context, client servicepb.BucketServiceClient, ledger string, checkpointID uint64) (string, error) {
	rctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	stream, err := client.ListAccounts(rctx, &servicepb.ListAccountsRequest{
		Ledger: ledger,
		Options: &commonpb.ListOptions{
			PageSize: 50,
			Read:     &commonpb.ReadOptions{CheckpointId: checkpointID},
		},
	})
	if err != nil {
		return "", err
	}

	accts, err := drainStream(stream)
	if err != nil {
		return "", err
	}

	return renderAccountsPage(accts), nil
}

// renderAccountsPage renders a page deterministically: addresses in stream
// order, each with its uncolored volumes and metadata keys sorted.
func renderAccountsPage(accts []*commonpb.Account) string {
	var b strings.Builder

	for _, a := range accts {
		b.WriteString(a.GetAddress())

		vols := make([]string, 0, len(a.GetVolumes()))
		for _, av := range a.GetVolumes() {
			if av.GetColor() != "" {
				continue
			}

			vols = append(vols, fmt.Sprintf("%s=in:%s,out:%s", av.GetAsset(), av.GetVolumes().GetInput(), av.GetVolumes().GetOutput()))
		}

		sort.Strings(vols)
		b.WriteString("{" + strings.Join(vols, " ") + "}")

		metas := make([]string, 0, len(a.GetMetadata()))
		for k, v := range a.GetMetadata() {
			metas = append(metas, k+"="+v.String())
		}

		sort.Strings(metas)
		b.WriteString("[" + strings.Join(metas, " ") + "]\n")
	}

	return b.String()
}
