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

// countingLedgerReader wraps a LedgerInfoReader and counts Mutate() calls. It
// pins the clone contract: read-only apply orders (transactions, metadata)
// resolve account types/enforcement from the immutable reader and must never
// call Mutate(); configuration-mutating handlers acquire exactly one owned
// clone through loadLedger.
type countingLedgerReader struct {
	commonpb.LedgerInfoReader

	mutateCalls *int
}

func (r countingLedgerReader) Mutate() *commonpb.LedgerInfo {
	*r.mutateCalls++

	return r.LedgerInfoReader.Mutate()
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
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	sourceKey := domain.NewVolumeKey("test-ledger", "bank", "USD", "")
	destKey := domain.NewVolumeKey("test-ledger", "users:123", "USD", "")

	sourceVolume := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(1000),
		Output: commonpb.NewUint256FromUint64(0),
	}
	destVolume := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	var mutateCalls int
	ledgerReader := countingLedgerReader{
		mutateCalls: &mutateCalls,
		LedgerInfoReader: (&commonpb.LedgerInfo{
			Name:                   "test-ledger",
			Id:                     1,
			DefaultEnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
			AccountTypes: map[string]*commonpb.AccountType{
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

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{
							{
								Source:      "bank",
								Destination: "users:123",
								Amount:      commonpb.NewUint256FromUint64(100),
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
// (no redundant CloneVT afterwards), then writes it back.
func TestProcessAddAccountType_ClonesLedgerInfoOnce(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)

	var mutateCalls int
	ledgerReader := countingLedgerReader{
		mutateCalls:      &mutateCalls,
		LedgerInfoReader: (&commonpb.LedgerInfo{Name: "l", Id: 1}).AsReader(),
	}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "l"}, ledgerReader, nil)

	order := &raftcmdpb.AddAccountTypeOrder{
		AccountType: &commonpb.AccountType{Name: "new-type", Pattern: "users:{z}"},
	}

	payload, derr := processAddAccountType("l", order, &Context{Scope: mockStore})
	require.Nil(t, derr)
	require.NotNil(t, payload)
	require.Equal(t, 1, mutateCalls, "mutating apply must acquire exactly one owned clone")
}
