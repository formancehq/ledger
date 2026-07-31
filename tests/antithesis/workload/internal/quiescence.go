package internal

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// WaitForQuiescentCommitIndex returns B when two consecutive barriers commit
// at B-1 and B. The second barrier is then the only proposal between samples.
// Transient barrier failures are retried; an exhausted search is inconclusive
// and returns (0, nil), while an unexpected RPC outcome is returned loudly to
// the caller so it can attach its property-specific assertion.
func WaitForQuiescentCommitIndex(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	attempts int,
) (uint64, error) {
	if attempts <= 0 {
		return 0, fmt.Errorf("quiescence attempts must be positive")
	}

	var last uint64
	for attempt := 1; attempt <= attempts; attempt++ {
		current, err := BarrierCommitIndex(ctx, client)
		if err != nil {
			if IsTransient(err) {
				continue
			}

			return 0, fmt.Errorf("quiescence barrier attempt %d: %w", attempt, err)
		}
		if last > 0 && current == last+1 {
			return current, nil
		}

		last = current
	}

	return 0, nil
}

// BarrierCommitIndex issues one barrier and returns its committed Raft index.
func BarrierCommitIndex(
	ctx context.Context,
	client servicepb.BucketServiceClient,
) (uint64, error) {
	response, err := client.Barrier(ctx, &servicepb.BarrierRequest{})
	if err != nil {
		return 0, err
	}

	return response.GetCommitIndex(), nil
}
