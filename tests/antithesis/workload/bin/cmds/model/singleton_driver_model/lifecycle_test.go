package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

type immediateApplyClient struct {
	servicepb.BucketServiceClient
}

func (immediateApplyClient) Apply(context.Context, *servicepb.ApplyRequest, ...grpc.CallOption) (*servicepb.ApplyResponse, error) {
	return &servicepb.ApplyResponse{}, nil
}

type scriptedApplyClient struct {
	servicepb.BucketServiceClient
	errors []error
	calls  int
}

func (c *scriptedApplyClient) Apply(context.Context, *servicepb.ApplyRequest, ...grpc.CallOption) (*servicepb.ApplyResponse, error) {
	err := c.errors[c.calls]
	c.calls++
	return nil, err
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

func TestAmbiguousEnableSchedulesRecoveryOnLaterMaintenanceRejection(t *testing.T) {
	t.Parallel()

	maintenanceStatus, err := status.New(codes.Unavailable, "maintenance").WithDetails(&errdetails.ErrorInfo{Reason: domain.ErrReasonMaintenanceMode})
	require.NoError(t, err)
	client := &scriptedApplyClient{errors: []error{
		status.Error(codes.Canceled, "response lost"),
		maintenanceStatus.Err(),
	}}
	c := NewChecker([]string{"L"}, nil)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	enable := oracle.Bulk{Requests: []*servicepb.Request{actions.SetMaintenanceModeAction(true)}}
	go func() {
		dispatchBulk(ctx, client, nil, c, enable)
		close(done)
	}()

	obs := <-c.incoming
	require.Equal(t, 2, client.calls)
	require.Equal(t, uint64(1), c.maintenanceEnableSeq)

	c.mu.Lock()
	c.removeInflight(obs.ticket)
	markObservationProcessed(obs)
	c.mu.Unlock()
	cancel()
	<-done
	c.recoveries.Wait()
}

func TestProcessorPreservesAmbiguousMaintenanceEnableAsCandidate(t *testing.T) {
	t.Parallel()

	maintenanceStatus, err := status.New(codes.Unavailable, "maintenance").WithDetails(&errdetails.ErrorInfo{Reason: domain.ErrReasonMaintenanceMode})
	require.NoError(t, err)
	c := NewChecker([]string{"L"}, nil)
	enable := oracle.Bulk{Requests: []*servicepb.Request{actions.SetMaintenanceModeAction(true)}}
	ticket := c.registerInflight(enable)

	c.handleObservation(observation{
		ticket:          ticket,
		bulk:            enable,
		err:             maintenanceStatus.Err(),
		ambiguousCommit: true,
		observeTicket:   ticket,
	})

	require.NotContains(t, c.inflight, ticket)
	require.Contains(t, c.ambiguousEnables, ticket)
	found := false
	c.candidateBases(ticket, func(state oracle.GlobalState) bool {
		found = found || state.MaintenanceMode()
		return found
	})
	require.True(t, found)
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
