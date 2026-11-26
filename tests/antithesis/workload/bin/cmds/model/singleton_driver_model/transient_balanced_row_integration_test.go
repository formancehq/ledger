package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// TestTransientBalancedPersistedRowAgainstServer pins that a persisted volume
// row at zero balance is not grandfathered once its account becomes TRANSIENT:
// a bulk leaving it non-zero is rejected by the server and the model alike.
func TestTransientBalancedPersistedRowAgainstServer(t *testing.T) {
	t.Parallel()

	for name, last := range map[string]*servicepb.Request{
		"tx":     oracletest.TxReq("world", "g:1", "USD", 5),
		"revert": oracletest.RevertReqL("L", 2, false),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, client := skippableTestServer(t)

			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("L", nil)))
			require.NoError(t, err)

			state := oracle.NewGlobalState()
			for _, req := range []*servicepb.Request{
				oracletest.TxReq("world", "g:1", "USD", 5),
				oracletest.TxReq("g:1", "world", "USD", 5),
				oracletest.AddTypeReqP("g", commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT),
			} {
				res := state.Apply(oracle.Bulk{Requests: []*servicepb.Request{req}})
				require.True(t, res.OK)
				state = res.State

				_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", req))
				require.NoError(t, err)
			}

			predicted := state.Apply(oracle.Bulk{Requests: []*servicepb.Request{last}})
			require.False(t, predicted.OK)
			require.Equal(t, domain.ErrReasonTransientAccountNonZero, predicted.Reason)

			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", last))
			require.Error(t, err)
			require.Equal(t, predicted.Reason, internal.ErrorReason(err))
		})
	}
}
