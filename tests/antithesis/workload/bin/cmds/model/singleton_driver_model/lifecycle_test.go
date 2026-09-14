package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"google.golang.org/grpc"
)

func TestLifecycleResponseRejectsMissingOrWrongMaintenanceLog(t *testing.T) {
	t.Parallel()
	req := &servicepb.Request{Type: &servicepb.Request_SetMaintenanceMode{
		SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{Enabled: false},
	}}
	for _, tt := range []struct {
		name    string
		payload *commonpb.LogPayload
		valid   bool
	}{
		{name: "missing payload"},
		{name: "wrong payload", payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_DeleteLedger{DeleteLedger: &commonpb.DeletedLedgerLog{Name: "L"}}}},
		{name: "wrong mode", payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_SetMaintenanceMode{SetMaintenanceMode: &commonpb.SetMaintenanceModeLog{Enabled: true}}}},
		{name: "disabled", payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_SetMaintenanceMode{SetMaintenanceMode: &commonpb.SetMaintenanceModeLog{Enabled: false}}}, valid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validateLifecycleLog(req, tt.payload)
			if tt.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// The episode must stop before reads or maintenance if the service acknowledges
// setup with no committed logs. A successful RPC alone is not oracle evidence.
func TestLifecycleEpisodeRejectsEmptySuccess(t *testing.T) {
	t.Parallel()
	c := NewChecker(nil, nil)
	driver := &lifecycleDriver{client: emptyLifecycleClient{}, checker: c}
	err := driver.runEpisode(context.Background(), "lifecycle")
	require.ErrorContains(t, err, "logs, got 0")
	require.Empty(t, c.modelState.Ledgers())
}

type emptyLifecycleClient struct{ servicepb.BucketServiceClient }

func (emptyLifecycleClient) Apply(context.Context, *servicepb.ApplyRequest, ...grpc.CallOption) (*servicepb.ApplyResponse, error) {
	return &servicepb.ApplyResponse{}, nil
}

func TestLifecycleResponseChecksLedgerIdentity(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		req     *servicepb.Request
		payload *commonpb.LogPayload
	}{
		{"create", &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: "wanted"}}}, &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "wrong"}}}},
		{"delete", &servicepb.Request{Type: &servicepb.Request_DeleteLedger{DeleteLedger: &servicepb.DeleteLedgerRequest{Name: "wanted"}}}, &commonpb.LogPayload{Type: &commonpb.LogPayload_DeleteLedger{DeleteLedger: &commonpb.DeletedLedgerLog{Name: "wrong"}}}},
		{"promote", &servicepb.Request{Type: &servicepb.Request_PromoteLedger{PromoteLedger: &servicepb.PromoteLedgerRequest{Ledger: "wanted"}}}, &commonpb.LogPayload{Type: &commonpb.LogPayload_PromoteLedger{PromoteLedger: &commonpb.PromotedLedgerLog{Name: "wrong"}}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, validateLifecycleLog(tt.req, nil))
			require.Error(t, validateLifecycleLog(tt.req, tt.payload))
		})
	}
}
