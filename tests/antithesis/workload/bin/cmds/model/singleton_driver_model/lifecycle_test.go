package main

import (
	"context"
	"fmt"
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
	if c.calls >= len(c.errors) {
		// Silently answering an unscripted call would let a stray Apply pass as
		// a success in every test sharing this client.
		panic(fmt.Sprintf("scriptedApplyClient: unscripted Apply #%d (script has %d)", c.calls+1, len(c.errors)))
	}

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

// A write whose response was lost must survive the maintenance window its
// retry lands in. The gate sits at admission, ahead of the FSM's idempotency
// replay, so breaking on the rejection would record a committed bulk as one
// that never happened — the bug that dropped DeleteQueryCheckpoint(48).
func TestBulkRetriesThroughMaintenanceAfterLostResponse(t *testing.T) {
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
	require.Equal(t, 3, client.calls, "the maintenance rejection is retried, not taken as an answer")
	require.NoError(t, obs.err, "the committed bulk must be observed as the success it was")
	c.mu.Lock()
	c.removeInflight(obs.ticket)
	markObservationProcessed(obs)
	c.mu.Unlock()
	<-done
}

// Retrying through maintenance is only safe because the window always ends,
// and the disable is scheduled off the enable's OWN success. An enable whose
// response is lost must therefore still reach that success: a toggle batch is
// exempt from the admission gate, and the retry carries the same idempotency
// key, so it replays the frozen outcome rather than being refused by the
// window it opened. Without that, retry-forever would hang instead.
func TestLostEnableResponseStillSchedulesItsRecovery(t *testing.T) {
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
	ctx, cancel := context.WithCancel(t.Context())
	enable := bulkOf(actions.SetMaintenanceModeAction(true))
	done := make(chan struct{})

	go func() {
		dispatchBulk(ctx, client, nil, c, enable)
		close(done)
	}()

	obs := <-c.incoming
	require.Equal(t, 3, client.calls)
	require.NoError(t, obs.err, "the enable committed, so the driver must observe a success")

	c.mu.Lock()
	scheduled := c.maintenanceRecoveryActive
	c.removeInflight(obs.ticket)
	c.mu.Unlock()
	markObservationProcessed(obs)
	require.True(t, scheduled, "the enable's success must arm the disable that ends the window")
	<-done

	// No processor runs here, so absorb whatever the recovery publishes rather
	// than letting it block on an observation nobody drains.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			select {
			case <-ctx.Done():
				return
			case recovery := <-c.incoming:
				c.mu.Lock()
				c.removeInflight(recovery.ticket)
				c.mu.Unlock()
				markObservationProcessed(recovery)
			}
		}
	}()

	cancel()
	c.recoveries.Wait()
	<-drained
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

	require.NoError(t, validateLifecycleLog(req, nil, payload))
}

func TestGenerateLifecycleHonorsConfiguredLiveTarget(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"model-0", "model-1", "model-2", "model-3", "model-4"}, nil)
	req := generateLifecycle(c.modelState, c.ledgerNamesSnapshot(), "model-5", 6)

	require.NotNil(t, req.GetCreateLedger())
	require.Equal(t, "model-5", req.GetCreateLedger().GetName())
	require.NotEmpty(t, req.GetCreateLedger().GetMetadata())
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
