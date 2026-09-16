package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

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
