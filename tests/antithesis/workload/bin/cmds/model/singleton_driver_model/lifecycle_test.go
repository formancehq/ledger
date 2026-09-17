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
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

type immediateApplyClient struct {
	servicepb.BucketServiceClient
}

func (immediateApplyClient) Apply(context.Context, *servicepb.ApplyRequest, ...grpc.CallOption) (*servicepb.ApplyResponse, error) {
	return &servicepb.ApplyResponse{}, nil
}

type scriptedApplyClient struct {
	servicepb.BucketServiceClient
	responses []*servicepb.ApplyResponse
	errors    []error
	calls     int
}

func (c *scriptedApplyClient) Apply(context.Context, *servicepb.ApplyRequest, ...grpc.CallOption) (*servicepb.ApplyResponse, error) {
	response := (*servicepb.ApplyResponse)(nil)
	if c.calls < len(c.responses) {
		response = c.responses[c.calls]
	}
	err := c.errors[c.calls]
	c.calls++
	return response, err
}

func TestDispatchMaintenanceRecoveryWaitsForObservationProcessing(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	c.mu.Lock()
	recoveryID := c.registerRead()
	c.mu.Unlock()
	done := make(chan struct{})
	go func() {
		dispatchMaintenanceRecovery(t.Context(), immediateApplyClient{}, c, recoveryID, 1)
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
	require.True(t, obs.ambiguousCommit)

	c.mu.Lock()
	c.removeInflight(obs.ticket)
	markObservationProcessed(obs)
	c.mu.Unlock()
	cancel()
	<-done
	c.recoveries.Wait()
}

func TestScheduleMaintenanceRecoveryDoesNotRefreshActiveWindow(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	c.maintenanceRecoveryActive = true
	c.maintenanceEnableSeq = 7

	scheduleMaintenanceRecovery(t.Context(), immediateApplyClient{}, c)

	require.Equal(t, uint64(7), c.maintenanceEnableSeq)
}

func TestScheduleMaintenanceRecoveryQueuesFollowUpAfterDisableDispatch(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	c.maintenanceRecoveryActive = true
	c.maintenanceRecoveryTicket = 8
	c.maintenanceEnableSeq = 7

	scheduleMaintenanceRecovery(t.Context(), immediateApplyClient{}, c)

	require.Equal(t, uint64(8), c.maintenanceEnableSeq)
}

func TestAmbiguousBusinessBulkRetriesThroughMaintenanceRecovery(t *testing.T) {
	t.Parallel()

	maintenanceStatus, err := status.New(codes.Unavailable, "maintenance").WithDetails(&errdetails.ErrorInfo{Reason: domain.ErrReasonMaintenanceMode})
	require.NoError(t, err)
	client := &scriptedApplyClient{
		responses: []*servicepb.ApplyResponse{nil, nil, {}},
		errors: []error{
			status.Error(codes.Canceled, "response lost"),
			maintenanceStatus.Err(),
			nil,
		},
	}
	c := NewChecker([]string{"L"}, nil)
	bulk := bulkOf(&servicepb.Request{Type: &servicepb.Request_SaveLedgerMetadata{SaveLedgerMetadata: &servicepb.SaveLedgerMetadataRequest{Ledger: "L"}}})
	done := make(chan struct{})
	go func() {
		dispatchBulk(t.Context(), client, nil, c, bulk)
		close(done)
	}()

	obs := <-c.incoming
	require.Equal(t, 3, client.calls)
	require.NoError(t, obs.err)
	require.False(t, obs.ambiguousCommit)
	c.mu.Lock()
	c.removeInflight(obs.ticket)
	markObservationProcessed(obs)
	c.mu.Unlock()
	<-done
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
	require.Contains(t, c.ambiguousBulks, ticket)
	found := false
	c.candidateBases(ticket, func(state oracle.GlobalState) bool {
		found = found || state.MaintenanceMode()
		return found
	})
	require.True(t, found)
}

func TestProcessorPreservesAmbiguousBusinessBulkAsCandidate(t *testing.T) {
	t.Parallel()

	maintenanceStatus, err := status.New(codes.Unavailable, "maintenance").WithDetails(&errdetails.ErrorInfo{Reason: domain.ErrReasonMaintenanceMode})
	require.NoError(t, err)
	c := NewChecker([]string{"L"}, nil)
	bulk := bulkOf(oracletest.AddTypeReq("retained"))
	ticket := c.registerInflight(bulk)

	c.handleObservation(observation{
		ticket:          ticket,
		bulk:            bulk,
		err:             maintenanceStatus.Err(),
		ambiguousCommit: true,
		observeTicket:   ticket,
	})

	require.Equal(t, bulk, c.ambiguousBulks[ticket])
	found := false
	c.candidateBases(ticket, func(state oracle.GlobalState) bool {
		result := state.Apply(bulk)
		found = !result.OK && result.Reason == domain.ErrReasonAccountTypeAlreadyExists
		return found
	})
	require.True(t, found)
}

func TestProcessorCoalescesAmbiguousMaintenanceEnables(t *testing.T) {
	t.Parallel()

	maintenanceStatus, err := status.New(codes.Unavailable, "maintenance").WithDetails(&errdetails.ErrorInfo{Reason: domain.ErrReasonMaintenanceMode})
	require.NoError(t, err)
	c := NewChecker([]string{"L"}, nil)
	enable := bulkOf(actions.SetMaintenanceModeAction(true))
	for _, ticket := range []uint64{2, 1} {
		c.inflight[ticket] = enable
		c.handleObservation(observation{
			ticket:          ticket,
			bulk:            enable,
			err:             maintenanceStatus.Err(),
			ambiguousCommit: true,
			observeTicket:   ticket,
		})
	}

	require.Len(t, c.ambiguousBulks, 1)
	retainedTicket, retained := c.ambiguousMaintenanceEnableTicket()
	require.True(t, retained)
	require.Equal(t, uint64(1), retainedTicket)
}

func TestAmbiguousEnableClearsOnlyAfterScheduledRecoveryGeneration(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	c.ambiguousBulks[1] = bulkOf(actions.SetMaintenanceModeAction(true))
	c.ambiguousEnableClearSeq = 2

	c.clearAmbiguousMaintenanceEnable(1)
	require.Contains(t, c.ambiguousBulks, uint64(1))

	c.clearAmbiguousMaintenanceEnable(2)
	require.NotContains(t, c.ambiguousBulks, uint64(1))
	require.Zero(t, c.ambiguousEnableClearSeq)
}

func TestResponseHighWaterExcludesWriterBlockedBeforeRegistration(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	responseFrontier := c.beginResponseFrontier()
	registered := make(chan struct{})
	go func() {
		c.mu.Lock()
		c.dispatchMu.Lock()
		c.registerInflight(bulkOf(oracletest.AddTypeReq("later")))
		c.dispatchMu.Unlock()
		c.mu.Unlock()
		close(registered)
	}()

	require.Zero(t, responseFrontier())
	<-registered
	require.Equal(t, uint64(1), c.ticketSeq.Load())
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

func TestReserveLedgerCreateCountsOutstandingCreates(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"model-0"}, nil)
	c.liveTarget = 2
	createOne := oracle.Bulk{Requests: []*servicepb.Request{{Type: &servicepb.Request_CreateLedger{
		CreateLedger: &servicepb.CreateLedgerRequest{Name: "model-1"},
	}}}}
	createTwo := oracle.Bulk{Requests: []*servicepb.Request{{Type: &servicepb.Request_CreateLedger{
		CreateLedger: &servicepb.CreateLedgerRequest{Name: "model-2"},
	}}}}

	require.True(t, c.reserveLedgerCreate(createOne))
	require.False(t, c.reserveLedgerCreate(createTwo), "the outstanding create must consume the remaining live slot")
	c.releaseLedgerCreate(createOne)
}

func TestReserveLedgerCreateExemptsTombstoneProbe(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"model-0"}, nil)
	deleted := c.modelState.Apply(bulkOf(&servicepb.Request{Type: &servicepb.Request_DeleteLedger{
		DeleteLedger: &servicepb.DeleteLedgerRequest{Name: "model-0"},
	}}))
	require.True(t, deleted.OK)
	c.modelState = deleted.State
	c.liveTarget = 0
	probe := bulkOf(&servicepb.Request{Type: &servicepb.Request_CreateLedger{
		CreateLedger: &servicepb.CreateLedgerRequest{Name: "model-0"},
	}})

	require.True(t, c.reserveLedgerCreate(probe))
	require.Zero(t, c.pendingLedgerCreates)
	c.releaseLedgerCreate(probe)
	require.Zero(t, c.pendingLedgerCreates)
}

func TestNextLedgerNameContinuesInitialSequence(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"model-run-0", "model-run-1", "model-run-2"}, nil)
	require.Equal(t, "model-run-3", c.nextLedgerName())
}
