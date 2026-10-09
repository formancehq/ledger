package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func numscriptSendRequest(script string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
		Ledger: "test-ledger",
		Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
			CreateTransaction: &servicepb.CreateTransactionPayload{Script: &commonpb.Script{Plain: script}},
		}},
	}}}
}

// TestProcessCreateTransaction_Numscript_TextOrderExecutes pins that a
// scripted order is compiled from its business text and applied.
func TestProcessCreateTransaction_Numscript_TextOrderExecutes(t *testing.T) {
	t.Parallel()

	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	expectDefaultMetadataLimits(mockStore)
	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}).AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return((&commonpb.Timestamp{Data: 1234567890}).AsReader()).AnyTimes()
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)
	setupNumscriptVolumeMocks(mockStore)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	expectPutTransactionState(t, mockStore, domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, nil)

	order := requestToOrderUnchecked(numscriptSendRequest(
		`send [USD/2 10000] (source = @world destination = @users:alice)`))

	result, procErr := processor.ProcessOrder(order, mockStore)
	require.Nil(t, procErr)

	postings := result.GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().GetPostings()
	require.Len(t, postings, 1)
	require.Equal(t, "world", postings[0].GetSource())
	require.Equal(t, "users:alice", postings[0].GetDestination())
	require.Equal(t, int64(10000), postings[0].GetAmount().ToBigInt().Int64())
	require.Equal(t, "USD/2", postings[0].GetAsset())
}

// TestProcessCreateTransaction_Numscript_TextOrderCompileError pins that
// a malformed scripted order surfaces the compiler's validation error.
func TestProcessCreateTransaction_Numscript_TextOrderCompileError(t *testing.T) {
	t.Parallel()

	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	expectDefaultMetadataLimits(mockStore)
	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}).AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), nil).AnyTimes()
	setupNumscriptVolumeMocks(mockStore)

	order := requestToOrderUnchecked(numscriptSendRequest(
		`send [USD/2 *] (source = @sa:credit allowing unbounded overdraft destination = @users:alice)`))

	result, procErr := processor.ProcessOrder(order, mockStore)
	require.Nil(t, result)

	var compileErr *domain.ErrNumscriptCompile
	require.ErrorAs(t, procErr, &compileErr)
}
