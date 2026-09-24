package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/drivertest"
)

func TestDriverQueryOracles(t *testing.T) {
	drivertest.CheckDriver(t, main, "definitively rejected write never appears in the ledger")
}

func TestRejectedWriteOracleDetectsCommittedTransaction(t *testing.T) {
	const ledger, reference = "deferr-sensitivity", "sensitivity-rejected-reference"
	const injection = "acknowledged transaction deliberately presented as a rejected write"
	drivertest.CheckEmissions(t, func() {
		ctx, client := drivertest.StartServer(t)
		require.NoError(t, internal.CreateQueryOracleLedger(ctx, client, ledger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE))
		request := actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{{
			Source: "world", Destination: "deferr-sensitivity-destination",
			Amount: commonpb.NewUint256FromUint64(1), Asset: "USD/2",
		}}, nil)
		request.GetApply().GetAction().GetCreateTransaction().Reference = reference
		resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("sensitivity-seed", request))
		require.NoError(t, err)
		txID, ok := actions.GetCreatedTransactionID(resp)
		require.True(t, ok)

		// The acknowledged seed is visible through the actual reference index.
		// Its synthetic rejection below tests the oracle, not server rejection.
		ids, err := internal.ReadOracleTransactions(ctx, client, ledger, actions.ReferenceFilter(reference))
		require.NoError(t, err)
		require.Equal(t, []uint64{txID}, ids)
		assertRejectedWritesAbsent(ctx, client, ledger, []rejection{{
			reference: reference, code: codes.FailedPrecondition, errMsg: injection,
		}}, internal.Details{"ledger": ledger, "sensitivityInjection": injection, "expectedTxId": txID})
	}, func(records []drivertest.Assertion) {
		var hits []drivertest.Assertion
		for _, record := range records {
			if record.Hit && record.Message == "definitively rejected write never appears in the ledger" {
				hits = append(hits, record)
			}
		}
		require.Len(t, hits, 1, "the original rejected-write oracle must execute")
		require.False(t, hits[0].Condition)
		require.Equal(t, ledger, hits[0].Details["ledger"])
		require.Equal(t, reference, hits[0].Details["reference"])
		require.Equal(t, codes.FailedPrecondition.String(), hits[0].Details["code"])
		require.Equal(t, injection, hits[0].Details["sensitivityInjection"])
		require.Equal(t, injection, hits[0].Details["error"])
		require.NotNil(t, hits[0].Details["expectedTxId"])
		require.Equal(t, hits[0].Details["expectedTxId"], hits[0].Details["foundTxId"])
	})
}
