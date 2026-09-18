package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/drivertest"
)

func TestDriverQueryOracles(t *testing.T) {
	drivertest.CheckDriver(t, main,
		"failed atomic bulk leaves no partial transaction effects",
		"failed atomic bulk leaves no partial account activity",
		"failed atomic bulk yields no success audit entry beyond ledger creation",
	)
}

func TestBulkAbsenceOraclesDetectCommittedActivity(t *testing.T) {
	const ledger, reference, account = "bulkatom-sensitivity", "sensitivity-bulk-reference", "bulkatom-sensitivity-destination"
	const injection = "acknowledged transaction deliberately presented as a forbidden partial bulk effect"
	drivertest.CheckEmissions(t, func() {
		ctx, client := drivertest.StartServer(t)
		require.NoError(t, internal.CreateQueryOracleLedger(ctx, client, ledger,
			commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE,
			commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS,
		))
		request := actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{{
			Source: "world", Destination: account,
			Amount: commonpb.NewUint256FromUint64(1), Asset: "USD/2",
		}}, nil)
		request.GetApply().GetAction().GetCreateTransaction().Reference = reference
		resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("sensitivity-seed", request))
		require.NoError(t, err)
		txID, ok := actions.GetCreatedTransactionID(resp)
		require.True(t, ok)

		// Both real query paths must see the acknowledged seed. This deliberately
		// seeded effect tests sensitivity; it is not a failed bulk that committed.
		for _, filter := range []*commonpb.QueryFilter{actions.ReferenceFilter(reference), actions.AddressExactFilter(account)} {
			ids, err := internal.ReadOracleTransactions(ctx, client, ledger, filter)
			require.NoError(t, err)
			require.Equal(t, []uint64{txID}, ids)
		}
		assertBulkEffectsAbsent(ctx, client, ledger, []string{reference}, []string{account}, internal.Details{
			"ledger": ledger, "sensitivityInjection": injection, "expectedTxIds": fmt.Sprint([]uint64{txID}),
		})
	}, func(records []drivertest.Assertion) {
		for _, expected := range []struct {
			message, detailKey, detailValue string
		}{
			{"failed atomic bulk leaves no partial transaction effects", "reference", reference},
			{"failed atomic bulk leaves no partial account activity", "account", account},
		} {
			var hits []drivertest.Assertion
			for _, record := range records {
				if record.Hit && record.Message == expected.message {
					hits = append(hits, record)
				}
			}
			require.Len(t, hits, 1, "the original bulk oracle must execute: %s", expected.message)
			require.False(t, hits[0].Condition)
			require.Equal(t, ledger, hits[0].Details["ledger"])
			require.Equal(t, expected.detailValue, hits[0].Details[expected.detailKey])
			require.Equal(t, injection, hits[0].Details["sensitivityInjection"])
			require.NotEmpty(t, hits[0].Details["expectedTxIds"])
			require.Equal(t, hits[0].Details["expectedTxIds"], hits[0].Details["txIds"])
		}
	})
}
