package admission

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestCheckQueryCheckpointProjectionReady(t *testing.T) {
	t.Parallel()

	checkpoint := &servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{
		CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{},
	}}
	nonCheckpoint := &servicepb.Request{Type: &servicepb.Request_CreateLedger{
		CreateLedger: &servicepb.CreateLedgerRequest{Name: "ledger"},
	}}

	for _, tc := range []struct {
		name       string
		reqs       []*servicepb.Request
		disabled   bool
		rebuilding bool
		wantErr    bool
	}{
		{name: "ready", reqs: []*servicepb.Request{checkpoint}},
		{name: "disabled", reqs: []*servicepb.Request{checkpoint}, disabled: true, wantErr: true},
		{name: "rebuilding", reqs: []*servicepb.Request{checkpoint}, rebuilding: true, wantErr: true},
		{name: "mixed batch", reqs: []*servicepb.Request{nonCheckpoint, checkpoint}, disabled: true, wantErr: true},
		{name: "unrelated request", reqs: []*servicepb.Request{nonCheckpoint}, disabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a := &Admission{auditProjectionState: func() (bool, bool) {
				return tc.disabled, tc.rebuilding
			}}
			err := a.checkQueryCheckpointProjectionReady(tc.reqs)
			if !tc.wantErr {
				require.NoError(t, err)

				return
			}

			var building *domain.ErrIndexBuilding
			require.ErrorAs(t, err, &building)
			require.Contains(t, building.Index, "audit")
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
