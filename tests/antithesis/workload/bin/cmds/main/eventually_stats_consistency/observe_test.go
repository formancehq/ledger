package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestAwaitUsageIndependentWitnessAndExactCounters(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		marker   uint64
		postings uint64
		reverts  uint64
		converge bool
	}{
		{name: "exact", marker: 1, postings: 3, reverts: 1, converge: true},
		{name: "matching counters without witness", postings: 3, reverts: 1},
		{name: "under-counted postings", marker: 1, postings: 2, reverts: 1},
		{name: "over-counted postings", marker: 1, postings: 4, reverts: 1},
		{name: "under-counted reverts", marker: 1, postings: 3},
		{name: "over-counted reverts", marker: 1, postings: 3, reverts: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var targetReads atomic.Int64
			var wrongConsistency atomic.Bool
			server := &oracleTestServer{
				getLedgerFn: func(ctx context.Context, req *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
					md, _ := metadata.FromIncomingContext(ctx)
					if values := md.Get("x-consistency"); len(values) != 1 || values[0] != "stale" {
						wrongConsistency.Store(true)
					}
					return &commonpb.LedgerInfo{Name: req.GetLedger(), Id: 7}, nil
				},
				statsFn: func(ctx context.Context, req *servicepb.GetLedgerStatsRequest) (*commonpb.LedgerStats, error) {
					if req.GetLedger() == "witness" {
						return &commonpb.LedgerStats{ReferenceCount: tt.marker}, nil
					}
					targetReads.Add(1)
					return &commonpb.LedgerStats{LogCount: 2, PostingCount: tt.postings, RevertCount: tt.reverts}, nil
				},
			}
			client := newSourceTestClient(t, server)
			var waits int
			result := awaitUsage(context.Background(), client, &commonpb.LedgerInfo{Name: "witness", Id: 7}, map[string]expectedLedger{"target": {ID: 7, Counts: counts{Logs: 2, Postings: 3, Reverts: 1}}}, func(context.Context) error {
				waits++
				if waits == 3 {
					return context.DeadlineExceeded
				}
				return nil
			})
			require.Equal(t, tt.converge, result.Converged)
			require.False(t, wrongConsistency.Load())
			if tt.converge {
				require.Equal(t, 1, result.Attempts)
			} else {
				require.Equal(t, 3, result.Attempts)
				require.Contains(t, result.Error, "usage convergence did not complete")
				if tt.marker == 1 {
					require.Equal(t, tt.postings, result.Mismatches["target"].Postings)
					require.Equal(t, tt.reverts, result.Mismatches["target"].Reverts)
				}
			}
			if tt.marker == 0 {
				require.Zero(t, targetReads.Load(), "matching counters cannot certify their own progress")
			}
		})
	}
}

func TestAwaitUsageRecoversFromLagAndTransientError(t *testing.T) {
	t.Parallel()
	for _, transient := range []bool{false, true} {
		t.Run(map[bool]string{false: "lag", true: "transport failure"}[transient], func(t *testing.T) {
			t.Parallel()
			var released atomic.Bool
			var targetReads atomic.Int64
			client := newSourceTestClient(t, &oracleTestServer{
				getLedgerFn: func(_ context.Context, req *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
					return &commonpb.LedgerInfo{Name: req.GetLedger(), Id: 1}, nil
				},
				statsFn: func(_ context.Context, req *servicepb.GetLedgerStatsRequest) (*commonpb.LedgerStats, error) {
					if !released.Load() {
						if transient {
							return nil, status.Error(codes.Unavailable, "replica restarting")
						}
						return &commonpb.LedgerStats{}, nil
					}
					if req.GetLedger() == "witness" {
						return &commonpb.LedgerStats{ReferenceCount: 1}, nil
					}
					targetReads.Add(1)
					return &commonpb.LedgerStats{LogCount: 1, PostingCount: 1}, nil
				},
			})
			result := awaitUsage(context.Background(), client, &commonpb.LedgerInfo{Name: "witness", Id: 1}, map[string]expectedLedger{"target": {ID: 1, Counts: counts{Logs: 1, Postings: 1}}}, func(context.Context) error { released.Store(true); return nil })
			require.True(t, result.Converged, result.Error)
			require.Equal(t, 2, result.Attempts)
			require.Equal(t, int64(1), targetReads.Load())
		})
	}
}

func TestAwaitUsageRejectsDisappearingWitness(t *testing.T) {
	t.Parallel()
	var markerReads atomic.Int64
	client := newSourceTestClient(t, &oracleTestServer{
		getLedgerFn: func(_ context.Context, req *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: req.GetLedger(), Id: 1}, nil
		},
		statsFn: func(_ context.Context, req *servicepb.GetLedgerStatsRequest) (*commonpb.LedgerStats, error) {
			if req.GetLedger() == "witness" {
				if markerReads.Add(1)%2 == 1 {
					return &commonpb.LedgerStats{ReferenceCount: 1}, nil
				}
				return &commonpb.LedgerStats{}, nil
			}
			return &commonpb.LedgerStats{LogCount: 1, PostingCount: 1}, nil
		},
	})
	result := awaitUsage(context.Background(), client, &commonpb.LedgerInfo{Name: "witness", Id: 1}, map[string]expectedLedger{"target": {ID: 1, Counts: counts{Logs: 1, Postings: 1}}}, func(context.Context) error { return context.DeadlineExceeded })
	require.False(t, result.Converged)
	require.Zero(t, result.WitnessReferences)
	require.Equal(t, int64(2), markerReads.Load())
}

func TestAwaitUsageRetainsErrorsAndIncarnationMismatch(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"unexpected error", "target incarnation", "witness incarnation"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			client := newSourceTestClient(t, &oracleTestServer{
				getLedgerFn: func(_ context.Context, req *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
					id := uint32(1)
					if scenario == req.GetLedger()+" incarnation" {
						id = 2
					}
					return &commonpb.LedgerInfo{Name: req.GetLedger(), Id: id}, nil
				},
				statsFn: func(_ context.Context, req *servicepb.GetLedgerStatsRequest) (*commonpb.LedgerStats, error) {
					if req.GetLedger() == "witness" {
						return &commonpb.LedgerStats{ReferenceCount: 1}, nil
					}
					return nil, status.Error(codes.Internal, "corrupt usage record")
				},
			})
			var waited bool
			result := awaitUsage(context.Background(), client, &commonpb.LedgerInfo{Name: "witness", Id: 1}, map[string]expectedLedger{"target": {ID: 1, Counts: counts{Logs: 1, Postings: 1}}}, func(context.Context) error { waited = true; return errors.New("must not wait") })
			require.False(t, result.Converged)
			require.False(t, waited)
			if scenario == "unexpected error" {
				require.Contains(t, result.Error, "corrupt usage record")
			} else {
				require.Contains(t, result.Error, "incarnation changed")
			}
		})
	}
}

func TestAwaitUsageRetainsMismatchWhenLaterRPCFails(t *testing.T) {
	t.Parallel()
	var unavailable atomic.Bool
	client := newSourceTestClient(t, &oracleTestServer{
		getLedgerFn: func(_ context.Context, req *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
			if unavailable.Load() {
				return nil, status.Error(codes.Unavailable, "replica stopped responding")
			}
			return &commonpb.LedgerInfo{Name: req.GetLedger(), Id: 1}, nil
		},
		statsFn: func(_ context.Context, req *servicepb.GetLedgerStatsRequest) (*commonpb.LedgerStats, error) {
			if req.GetLedger() == "witness" {
				return &commonpb.LedgerStats{ReferenceCount: 1}, nil
			}
			return &commonpb.LedgerStats{LogCount: 1}, nil
		},
	})
	result := awaitUsage(context.Background(), client, &commonpb.LedgerInfo{Name: "witness", Id: 1}, map[string]expectedLedger{"target": {ID: 1, Counts: counts{Logs: 1, Postings: 1}}}, func(context.Context) error {
		if unavailable.Swap(true) {
			return context.DeadlineExceeded
		}
		return nil
	})
	require.False(t, result.Converged)
	require.Equal(t, 2, result.Attempts)
	require.Equal(t, counts{Logs: 1}, result.Mismatches["target"])
	require.Contains(t, result.Error, "replica stopped responding")
}

func TestAwaitUsageAllowsAFullSweepWithinConvergenceWindow(t *testing.T) {
	t.Parallel()
	// A large healthy collection may take longer than a short per-sample
	// deadline. Check the budget reaching the server without sleeping in tests.
	var truncatedBudget atomic.Bool
	client := newSourceTestClient(t, &oracleTestServer{
		getLedgerFn: func(ctx context.Context, req *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) < 30*time.Second {
				truncatedBudget.Store(true)
			}
			return &commonpb.LedgerInfo{Name: req.GetLedger(), Id: 1}, nil
		},
		statsFn: func(_ context.Context, req *servicepb.GetLedgerStatsRequest) (*commonpb.LedgerStats, error) {
			return &commonpb.LedgerStats{ReferenceCount: 1}, nil
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	result := awaitUsage(ctx, client, &commonpb.LedgerInfo{Name: "witness", Id: 1}, map[string]expectedLedger{"target": {ID: 1}}, func(context.Context) error { return errors.New("unexpected retry") })
	require.True(t, result.Converged, result.Error)
	require.False(t, truncatedBudget.Load(), "the full sweep must retain its convergence budget")
}
