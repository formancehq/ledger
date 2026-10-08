package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/health"
)

func TestCheckQueryCheckpointProjectionReady(t *testing.T) {
	t.Parallel()

	checkpoint := &ledgerpb.Request{Type: &ledgerpb.Request_CreateQueryCheckpoint{
		CreateQueryCheckpoint: &ledgerpb.CreateQueryCheckpointRequest{},
	}}
	nonCheckpoint := &ledgerpb.Request{Type: &ledgerpb.Request_CreateLedger{
		CreateLedger: &ledgerpb.CreateLedgerRequest{Name: "ledger"},
	}}

	for _, tc := range []struct {
		name       string
		reqs       []*ledgerpb.Request
		disabled   bool
		rebuilding bool
		wantReason string
		wantKind   domain.ErrorKind
	}{
		{name: "ready", reqs: []*ledgerpb.Request{checkpoint}},
		{name: "disabled", reqs: []*ledgerpb.Request{checkpoint}, disabled: true, wantReason: domain.ErrReasonAuditDisabled, wantKind: domain.KindPrecondition},
		{name: "rebuilding", reqs: []*ledgerpb.Request{checkpoint}, rebuilding: true, wantReason: domain.ErrReasonIndexBuilding, wantKind: domain.KindUnavailable},
		{name: "mixed batch", reqs: []*ledgerpb.Request{nonCheckpoint, checkpoint}, disabled: true, wantReason: domain.ErrReasonAuditDisabled, wantKind: domain.KindPrecondition},
		{name: "unrelated request", reqs: []*ledgerpb.Request{nonCheckpoint}, disabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := &Admission{auditProjectionState: func() (bool, bool) {
				return tc.disabled, tc.rebuilding
			}}
			err := a.checkQueryCheckpointProjectionReady(tc.reqs)
			if tc.wantReason == "" {
				require.NoError(t, err)

				return
			}

			var describable domain.Describable
			require.ErrorAs(t, err, &describable)
			require.Equal(t, tc.wantReason, describable.Reason())
			require.Equal(t, tc.wantKind, describable.Kind())
		})
	}
}

func TestWithAuditProjectionStateConfiguresAdmission(t *testing.T) {
	t.Parallel()

	a := &Admission{}
	WithAuditProjectionState(func() (bool, bool) { return true, false })(a)
	disabled, rebuilding := a.auditProjectionState()
	require.True(t, disabled)
	require.False(t, rebuilding)
}

func TestAdmitRejectsCheckpointWhenAuditProjectionIsUnavailable(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	a, _ := createTestAdmission(t, store)
	writeGate := health.NewMockWriteGate(gomock.NewController(t))
	writeGate.EXPECT().CheckWritesAllowed().Return(nil)
	a.writeGate = writeGate
	WithAuditProjectionState(func() (bool, bool) { return false, true })(a)

	_, err := a.Admit(attributedTestContext(context.Background()), &ledgerpb.ApplyRequest{
		Variant: &ledgerpb.ApplyRequest_Unsigned{Unsigned: &ledgerpb.ApplyBatch{Requests: []*ledgerpb.Request{{
			Type: &ledgerpb.Request_CreateQueryCheckpoint{
				CreateQueryCheckpoint: &ledgerpb.CreateQueryCheckpointRequest{},
			},
		}}}},
	})

	var building *domain.ErrIndexBuilding
	require.ErrorAs(t, err, &building)
	require.Contains(t, building.Index, "audit")
}
