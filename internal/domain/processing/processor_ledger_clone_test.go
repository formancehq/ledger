package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// countingLedgerReader wraps a LedgerInfoReader and observes the clone
// contract: read-only apply orders (transactions, metadata) resolve account
// types/enforcement from the immutable reader and must never call Mutate();
// configuration-mutating handlers acquire exactly one owned clone through
// loadLedger. Mutate() also captures the clone it returns so the test can
// prove the pointer written back through Put is that same clone — an extra
// `info = info.CloneVT()` after loadLedger would swap in a second allocation
// without touching the Mutate() call count.
type countingLedgerReader struct {
	ledgerpb.LedgerInfoReader

	mutateCalls *int
	mutated     **ledgerpb.LedgerInfo
}

func (r countingLedgerReader) Mutate() *ledgerpb.LedgerInfo {
	*r.mutateCalls++

	clone := r.LedgerInfoReader.Mutate()
	if r.mutated != nil {
		*r.mutated = clone
	}

	return clone
}

// TestProcessCreateTransaction_DoesNotCloneLedgerInfo pins the EN-1968 hot
// path: a read-only ledger apply (a transaction) must not deep-clone LedgerInfo
// just to compile account types / read the default enforcement mode. The clone
// used to happen in processApply for every order; it now stays on Mutate(), and
// only configuration-mutating handlers call that.
func TestProcessCreateTransaction_DoesNotCloneLedgerInfo(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	expectDefaultMetadataLimits(mockStore)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &ledgerpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	sourceKey := domain.NewVolumeKey("test-ledger", "bank", "USD", "")
	destKey := domain.NewVolumeKey("test-ledger", "users:123", "USD", "")

	sourceVolume := &raftcmdpb.VolumePair{
		Input:  ledgerpb.NewUint256FromUint64(1000),
		Output: ledgerpb.NewUint256FromUint64(0),
	}
	destVolume := &raftcmdpb.VolumePair{
		Input:  ledgerpb.NewUint256FromUint64(0),
		Output: ledgerpb.NewUint256FromUint64(0),
	}

	var mutateCalls int
	ledgerReader := countingLedgerReader{
		mutateCalls: &mutateCalls,
		LedgerInfoReader: (&ledgerpb.LedgerInfo{
			Name:                   "test-ledger",
			Id:                     1,
			DefaultEnforcementMode: ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
			AccountTypes: map[string]*ledgerpb.AccountType{
				"user": {Name: "user", Pattern: "users:{id}"},
			},
		}).AsReader(),
	}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, ledgerReader, nil)
	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(now.AsReader()).Times(4)
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)
	expectGetVolume(mockStore, sourceKey, sourceVolume.AsReader(), nil)
	expectPutVolume(t, mockStore, sourceKey, nil)
	expectGetVolume(mockStore, destKey, destVolume.AsReader(), nil)
	expectPutVolume(t, mockStore, destKey, nil)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	expectPutTransactionState(t, mockStore, domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, nil)

	request := &ledgerpb.Request{
		Type: &ledgerpb.Request_Apply{
			Apply: &ledgerpb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_CreateTransaction{
					CreateTransaction: &ledgerpb.CreateTransactionPayload{
						Postings: []*ledgerpb.Posting{
							{
								Source:      "bank",
								Destination: "users:123",
								Amount:      ledgerpb.NewUint256FromUint64(100),
								Asset:       "USD",
							},
						},
					},
				}},
			},
		},
	}

	payload, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, payload)
	require.Zero(t, mutateCalls, "read-only apply must not deep-clone LedgerInfo")
}

// TestProcessAddAccountType_ClonesLedgerInfoOnce pins the mutation boundary:
// processAddAccountType loads an owned clone through loadLedger exactly once
// (no redundant CloneVT afterwards), then writes that same clone back.
func TestProcessAddAccountType_ClonesLedgerInfoOnce(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	var (
		mutateCalls int
		mutated     *ledgerpb.LedgerInfo
	)
	ledgerReader := countingLedgerReader{
		mutateCalls:      &mutateCalls,
		mutated:          &mutated,
		LedgerInfoReader: (&ledgerpb.LedgerInfo{Name: "l", Id: 1}).AsReader(),
	}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "l"}, ledgerReader, nil)

	var putInfo *ledgerpb.LedgerInfo
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "l"}, nil, func(_ string, info *ledgerpb.LedgerInfo) {
		putInfo = info
	})

	order := &raftcmdpb.AddAccountTypeOrder{
		AccountType: &ledgerpb.AccountType{Name: "new-type", Pattern: "users:{z}"},
	}

	payload, derr := processAddAccountType("l", order, &Context{Scope: mockStore})
	require.Nil(t, derr)
	require.NotNil(t, payload)
	require.Equal(t, 1, mutateCalls, "mutating apply must acquire exactly one owned clone")
	require.NotNil(t, mutated)
	require.Zero(t, ledgerReader.GetAccountTypes().Len(), "configuration mutation must leave the original cached reader unchanged")
	require.Contains(t, putInfo.GetAccountTypes(), "new-type")
	require.Same(t, mutated, putInfo, "mutating apply must write back the clone returned by Mutate(), not a redundant CloneVT() clone")
}
