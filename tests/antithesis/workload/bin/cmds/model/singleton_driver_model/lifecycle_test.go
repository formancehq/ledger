package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type immediateApplyClient struct {
	servicepb.BucketServiceClient
}

func (immediateApplyClient) Apply(context.Context, *servicepb.ApplyRequest, ...grpc.CallOption) (*servicepb.ApplyResponse, error) {
	return &servicepb.ApplyResponse{}, nil
}

func TestDispatchMaintenanceRecoveryWaitsForObservationProcessing(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	c.mu.Lock()
	recoveryID := c.registerRead()
	c.mu.Unlock()
	done := make(chan struct{})
	go func() {
		dispatchMaintenanceRecovery(t.Context(), immediateApplyClient{}, c, recoveryID)
		close(done)
	}()

	obs := <-c.incoming
	select {
	case <-done:
		t.Fatal("recovery returned before its observation was processed")
	default:
	}
	require.NotContains(t, c.reads, recoveryID)
	require.Contains(t, c.inflight, obs.ticket)

	c.mu.Lock()
	c.removeInflight(obs.ticket)
	markObservationProcessed(obs)
	c.mu.Unlock()
	<-done
	require.NotContains(t, c.inflight, obs.ticket)
}

func TestValidateLifecycleLogCanonicalizesAccountTypeNames(t *testing.T) {
	t.Parallel()

	req := &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{
		Name:         "L",
		AccountTypes: map[string]*commonpb.AccountType{"asset": {}},
	}}}
	payload := &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{
		Id:           1,
		Name:         "L",
		CreatedAt:    &commonpb.Timestamp{Data: 1},
		AccountTypes: map[string]*commonpb.AccountType{"asset": {Name: "asset"}},
	}}}

	require.NoError(t, validateLifecycleLog(req, payload))
}

func TestGenerateLifecycleHonorsConfiguredLiveTarget(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"model-0", "model-1", "model-2", "model-3", "model-4"}, nil)
	req := generateLifecycle(c.modelState, c.ledgerNamesSnapshot(), "model-5", 6)

	require.NotNil(t, req.GetCreateLedger())
	require.Equal(t, "model-5", req.GetCreateLedger().GetName())
}

func TestNextLedgerNameContinuesInitialSequence(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"model-run-0", "model-run-1", "model-run-2"}, nil)
	require.Equal(t, "model-run-3", c.nextLedgerName())
}
