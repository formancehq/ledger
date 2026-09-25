package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/sdktest"
)

type balanceSDKServer struct {
	servicepb.UnimplementedBucketServiceServer
	scenario string
	lists    atomic.Int32
	gets     atomic.Int32
	barriers atomic.Int32
}

func sdkAccount(balance string) *commonpb.Account {
	return &commonpb.Account{Address: "users:1", Volumes: []*commonpb.AccountVolume{{
		Asset: "COIN", Color: "blue",
		Volumes: &commonpb.VolumesWithBalance{
			Input:   commonpb.MustBigUintFromDecimal(balance),
			Output:  commonpb.MustBigUintFromDecimal("0"),
			Balance: commonpb.MustSignedBigIntFromDecimal(balance),
		},
	}}}
}

func (s *balanceSDKServer) ListAccounts(_ *servicepb.ListAccountsRequest, stream grpc.ServerStreamingServer[commonpb.Account]) error {
	s.lists.Add(1)
	if s.scenario == "empty" {
		return nil
	}
	return stream.Send(sdkAccount("10"))
}

func (s *balanceSDKServer) GetAccount(context.Context, *servicepb.GetAccountRequest) (*commonpb.Account, error) {
	s.gets.Add(1)
	switch s.scenario {
	case "transient":
		return nil, status.Error(codes.Unavailable, "read interrupted")
	case "unknown":
		return nil, status.Error(codes.Unknown, "unexpected read failure")
	case "failure_branch":
		return sdkAccount("11"), nil
	case "advanced_then_match":
		if s.lists.Load() == 1 {
			return sdkAccount("11"), nil
		}
	}
	return sdkAccount("10"), nil
}

func (s *balanceSDKServer) Barrier(context.Context, *servicepb.BarrierRequest) (*servicepb.BarrierResponse, error) {
	call := s.barriers.Add(1)
	if call > 2 {
		return nil, status.Error(codes.Internal, "unexpected extra barrier")
	}
	index := uint64(100 + call)
	if s.scenario == "failure_branch" {
		// Exercise only the existing SDK failure branch. These scripted horizons
		// are not a reproduction of the server's barrier advancement contract.
		index--
	} else if s.scenario == "advanced_then_match" {
		// Include a real extra proposal, in addition to the barriers themselves.
		index++
	}
	return &servicepb.BarrierResponse{CommitIndex: index}, nil
}

func TestCheckVolumesConsistentSDK(t *testing.T) {
	t.Parallel()
	const divergence = "list/get balance divergence persisted past quiescence"
	const success = "list/get balance pair verified matching"
	for _, scenario := range []string{"matching", "failure_branch", "empty", "transient", "unknown", "advanced_then_match"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			events := sdktest.Capture(t, func() {
				server := &balanceSDKServer{scenario: scenario}
				client := sdktest.Client(t, server)
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				checkVolumesConsistent(ctx, client, "sdk-balances", 100)
				wantLists, wantGets, wantBarriers := int32(1), int32(2), int32(0)
				switch scenario {
				case "empty":
					wantGets = 0
				case "failure_branch":
					wantBarriers = 2
				case "advanced_then_match":
					wantLists, wantGets, wantBarriers = 2, 3, 2
				}
				require.Equal(t, wantLists, server.lists.Load())
				require.Equal(t, wantGets, server.gets.Load())
				require.Equal(t, wantBarriers, server.barriers.Load())
			})
			if events == nil {
				return
			}
			switch scenario {
			case "matching", "advanced_then_match":
				event := sdktest.Find(t, events, success)
				require.Equal(t, "Reachable", event.DisplayType)
				require.True(t, event.Condition)
				require.True(t, event.MustHit)
				require.Equal(t, "10", event.Details["listBalance"])
				require.Equal(t, "10", event.Details["actualBalance"])
				sdktest.Absent(t, events, divergence)
			case "failure_branch":
				event := sdktest.Find(t, events, divergence)
				require.False(t, event.Condition)
				require.Equal(t, "sdk-balances", event.Details["ledger"])
				require.Equal(t, "users:1", event.Details["account"])
				require.Equal(t, "10", event.Details["listBalance"])
				require.Equal(t, "11", event.Details["actualBalance"])
				require.Equal(t, "Unreachable", event.DisplayType, "failure-only observation must not require a hit")
				require.False(t, event.MustHit)
				sdktest.Absent(t, events, success)
			case "empty", "transient", "unknown":
				sdktest.Absent(t, events, success)
				sdktest.Absent(t, events, divergence)
				sdktest.Find(t, events, "eventually_correct found no list/get balance pairs to compare")
				if scenario == "unknown" {
					event := sdktest.Find(t, events, "GetAccount returned unexpected error in cross-check")
					require.False(t, event.Condition)
				}
			}
		})
	}
}
