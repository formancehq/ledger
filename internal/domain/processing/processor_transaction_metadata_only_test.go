package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestProcessCreateTransaction_MetadataOnly_TransactionFields(t *testing.T) {
	t.Parallel()
	for _, collision := range []bool{false, true} {
		name := "new reference and transaction metadata"
		if collision {
			name = "existing monetary reference"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			mockStore := NewMockScope(gomock.NewController(t))
			expectDefaultMetadataLimits(mockStore)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)

			boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 7, NextLogId: 3}
			expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
			expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), nil).AnyTimes()
			mockStore.EXPECT().GetDate().Return((&commonpb.Timestamp{Data: 1234567890}).AsReader()).AnyTimes()
			refs, _ := stubsFor(mockStore).transactionReferencesStubFor(mockStore)
			refs.putHook = func(domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue) {
				t.Fatal("metadata-only execution must not reserve a transaction reference")
			}
			refKey := domain.TransactionReferenceKey{LedgerName: "test-ledger", Reference: "wallet-snapshot"}
			if collision {
				refs.expectGet(refKey, (&commonpb.TransactionReferenceValue{TransactionId: 5}).AsReader(), nil)
			} else {
				refs.expectGet(refKey, nil, domain.ErrNotFound)
				expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil, func(_ string, written *raftcmdpb.LedgerBoundaries) {
					require.Equal(t, uint64(7), written.GetNextTransactionId())
					require.Equal(t, uint64(4), written.GetNextLogId())
				})
				expectPutAccountMetadata(t, mockStore, domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"}, Key: "tier"}, commonpb.NewStringValue("gold"))
			}

			request := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: &servicepb.CreateTransactionPayload{
					Reference: "wallet-snapshot",
					Metadata:  map[string]*commonpb.MetadataValue{"source": commonpb.NewStringValue("fixture")},
					Script: &commonpb.Script{Plain: `set_account_meta(@users:alice, "tier", "gold")
set_tx_meta("observation", "wallet")`},
				}}},
			}}}
			order := requestToOrder(t, request)
			before := proto.Clone(order)
			result, err := processor.ProcessOrder(order, mockStore)
			require.True(t, proto.Equal(before, order), "accepted order must remain immutable for audit capture")
			if collision {
				var conflict *domain.ErrTransactionReferenceConflict
				require.ErrorAs(t, err, &conflict)
				require.Equal(t, uint64(5), conflict.ExistingTransactionID)
				require.Equal(t, "wallet-snapshot", conflict.Reference)

				return
			}
			require.NoError(t, err)
			log := result.GetApply().GetLog()
			require.Equal(t, uint64(3), log.GetId())
			require.Nil(t, log.GetData().GetCreatedTransaction())
			require.Equal(t, map[string]*commonpb.MetadataMap{"users:alice": {Values: map[string]*commonpb.MetadataValue{"tier": commonpb.NewStringValue("gold")}}}, log.GetData().GetAccountMetadataUpdated().GetAccountMetadata())
		})
	}
}
