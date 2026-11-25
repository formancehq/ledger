package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestApplyEntriesSelectsTransactionAccountMetadataCapability(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		marked bool
	}{
		{name: "marked transaction rejects untyped metadata", marked: true},
		{name: "historical unmarked transaction keeps legacy behavior"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			machine, store, attrs := newTestMachine(t)
			const ledger = "strict-metadata"
			const account = "rogue:1"

			create := createLedgerOrderWithAccountTypes(ledger, map[string]*commonpb.AccountType{
				"users": {Pattern: "users:{id}"},
			})
			create.GetLedgerScoped().GetCreateLedger().DefaultEnforcementMode = commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT
			createProposal := makeProposal(1, create)
			_, err := capabilities.PrepareProposal(createProposal)
			require.NoError(t, err)
			created, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, createProposal))
			require.NoError(t, err)
			require.NoError(t, created.Results[0].Error)

			transaction := createTransactionOrder(ledger, true, newPosting("world", "users:1", "USD/2", 100))
			transaction.GetLedgerScoped().GetApply().GetCreateTransaction().AccountMetadata = map[string]*commonpb.MetadataMap{
				account: {Values: map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue("v")}},
			}
			proposal := makeProposal(2, transaction)
			metadataKey := domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: ledger, Account: account}, Key: "k"}
			metadataID, _ := attributes.MakeKey(metadataKey.Bytes())
			proposal.ExecutionPlan.Attributes = append(proposal.ExecutionPlan.Attributes, declareTestPlan(metadataID, dal.SubAttrMetadata))
			if tc.marked {
				prepared, err := capabilities.PrepareProposal(proposal)
				require.NoError(t, err)
				require.Equal(t, []capabilities.CapabilityID{capabilities.StrictAccountTypes}, prepared.RequiredCapabilities())
			} else {
				require.Empty(t, proposal.GetRequiredCapabilities(), "historical entries carry no capability marker")
			}

			before := readBusinessProjections(t, store, attrs)
			value, err := attrs.Metadata.Get(store, metadataKey.Bytes())
			require.NoError(t, err)
			require.Nil(t, value)
			applied, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 2, proposal))
			require.NoError(t, err)
			require.Len(t, applied.Results, 1)
			result := applied.Results[0]
			require.True(t, result.AuditEntryWritten)

			value, err = attrs.Metadata.Get(store, metadataKey.Bytes())
			require.NoError(t, err)
			if tc.marked {
				var mismatch *domain.ErrAccountNotMatchingType
				require.ErrorAs(t, result.Error, &mismatch)
				require.Equal(t, account, mismatch.Address)
				require.Empty(t, result.Logs)
				require.Nil(t, value, "rejected metadata must not reach the persisted store")
				require.Equal(t, before, readBusinessProjections(t, store, attrs), "rejection must preserve all business projections")
			} else {
				require.NoError(t, result.Error)
				require.Len(t, result.Logs, 1)
				require.NotNil(t, value, "the unmarked control must execute the metadata write")
				require.Equal(t, "v", value.GetStringValue())
			}
		})
	}
}
