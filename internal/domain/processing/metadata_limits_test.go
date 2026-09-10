package processing

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// tightMetadataPolicy is a committed policy whose entity ceiling is small
// enough that a caller-plus-Numscript merge can cross it while each half stays
// legal on its own.
func tightMetadataPolicy(maxEntityBytes uint64) *commonpb.ClusterPolicy {
	return &commonpb.ClusterPolicy{
		Revision:                    1,
		QueryCheckpointLimit:        1,
		MetadataMaxEntriesPerEntity: domain.DefaultMetadataMaxEntriesPerEntity,
		MetadataMaxKeyBytes:         64,
		MetadataMaxValueBytes:       maxEntityBytes,
		MetadataMaxEntityBytes:      maxEntityBytes,
		MetadataMaxCommandBytes:     maxEntityBytes,
	}
}

// The FSM bounds the MERGED transaction metadata. Admission validated the
// caller's map and the producer validated each Numscript key/value, but only
// apply sees the union — so this is the one place a legal caller map plus a
// legal script output is caught exceeding the entity ceiling.
func TestProcessCreateTransaction_NumscriptMergedMetadataOverEntityCeiling(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	// 30 bytes: the caller entry ("caller" + 20 bytes = 26) fits on its own and
	// the script's entry ("s" + 4 = 5) fits on its own, but the merge is 31.
	mockStore.EXPECT().GetClusterPolicy().Return(tightMetadataPolicy(30)).AnyTimes()

	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now.AsReader()).AnyTimes()
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1)).AnyTimes()
	setupNumscriptVolumeMocks(mockStore)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Metadata: map[string]*commonpb.MetadataValue{
							"caller": commonpb.NewStringValue(strings.Repeat("v", 20)),
						},
						Script: &commonpb.Script{
							Plain: `
								set_tx_meta("s", "abcd")
								send [USD/2 100] (
									source = @world
									destination = @users:alice
								)
							`,
						},
					},
				}},
			},
		},
	}

	_, procErr := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NotNil(t, procErr, "the merged metadata must be rejected inside apply")
	require.Equal(t, domain.ErrReasonMetadataLimitExceeded, procErr.Reason())
	require.Equal(t, domain.MetadataLimitDimensionEntity, procErr.Metadata()["dimension"])
	require.Equal(t, "31", procErr.Metadata()["actual"])
}

// Numscript-produced account metadata is bounded the same way, and the failure
// names the account.
func TestProcessCreateTransaction_NumscriptAccountMetadataOverCeiling(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	mockStore.EXPECT().GetClusterPolicy().Return(tightMetadataPolicy(4)).AnyTimes()

	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now.AsReader()).AnyTimes()
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1)).AnyTimes()
	setupNumscriptVolumeMocks(mockStore)

	// The transaction map is empty here, so apply stages the transaction state
	// before reaching the per-account check; the failed order's write set is
	// discarded, exactly as for the address validation that already sits after
	// this Put.
	expectPutTransactionState(t, mockStore, domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, nil)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: `
								set_account_meta(@users:alice, "tier", "premium")
								send [USD/2 100] (
									source = @world
									destination = @users:alice
								)
							`,
						},
					},
				}},
			},
		},
	}

	_, procErr := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NotNil(t, procErr)
	require.Equal(t, domain.ErrReasonMetadataLimitExceeded, procErr.Reason())
	require.Equal(t, "users:alice", procErr.Metadata()["account"])
}

// A committed policy must always carry usable ceilings: zero is the absence of
// configuration, and committing it would silently disable the protection on
// every write path.
func TestProcessSetClusterPolicy_RejectsMissingMetadataLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*commonpb.ClusterPolicy)
	}{
		{"no metadata limits at all", func(p *commonpb.ClusterPolicy) {
			p.MetadataMaxEntriesPerEntity = 0
			p.MetadataMaxKeyBytes = 0
			p.MetadataMaxValueBytes = 0
			p.MetadataMaxEntityBytes = 0
			p.MetadataMaxCommandBytes = 0
		}},
		{"zero entries", func(p *commonpb.ClusterPolicy) { p.MetadataMaxEntriesPerEntity = 0 }},
		{"zero key bytes", func(p *commonpb.ClusterPolicy) { p.MetadataMaxKeyBytes = 0 }},
		{"zero value bytes", func(p *commonpb.ClusterPolicy) { p.MetadataMaxValueBytes = 0 }},
		{"zero entity bytes", func(p *commonpb.ClusterPolicy) { p.MetadataMaxEntityBytes = 0 }},
		{"zero command bytes", func(p *commonpb.ClusterPolicy) { p.MetadataMaxCommandBytes = 0 }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			mockStore := NewMockScope(ctrl)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)

			policy := withMetadataLimits(&commonpb.ClusterPolicy{Revision: 3, QueryCheckpointLimit: 1})
			tc.mutate(policy)

			_, procErr := processor.ProcessOrder(clusterPolicyOrder(policy), mockStore)
			require.NotNil(t, procErr)
			require.Equal(t, domain.ErrReasonClusterPolicyInvalid, procErr.Reason())
			require.Contains(t, procErr.Error(), "metadata_max_")
		})
	}
}

// Mutually unsatisfiable ceilings are refused too: the wider bound would be
// unreachable, so an operator raising it would observe no effect.
func TestProcessSetClusterPolicy_RejectsInconsistentMetadataLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*commonpb.ClusterPolicy)
	}{
		{"key above entity", func(p *commonpb.ClusterPolicy) { p.MetadataMaxKeyBytes = p.GetMetadataMaxEntityBytes() + 1 }},
		{"value above entity", func(p *commonpb.ClusterPolicy) { p.MetadataMaxValueBytes = p.GetMetadataMaxEntityBytes() + 1 }},
		{"entity above command", func(p *commonpb.ClusterPolicy) { p.MetadataMaxEntityBytes = p.GetMetadataMaxCommandBytes() + 1 }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			mockStore := NewMockScope(ctrl)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)

			policy := withMetadataLimits(&commonpb.ClusterPolicy{Revision: 3, QueryCheckpointLimit: 1})
			tc.mutate(policy)

			_, procErr := processor.ProcessOrder(clusterPolicyOrder(policy), mockStore)
			require.NotNil(t, procErr)
			require.Equal(t, domain.ErrReasonClusterPolicyInvalid, procErr.Reason())
			require.Contains(t, procErr.Error(), "must satisfy")
		})
	}
}

// The account-scoped check reports the lexicographically smallest offending
// account rather than whichever one map iteration reaches first: this runs in
// apply, so two nodes must produce the identical rejection (invariant #2).
func TestValidateMergedAccountMetadataIsDeterministic(t *testing.T) {
	t.Parallel()

	limits := domain.MetadataLimits{
		MaxEntriesPerEntity:     10,
		MaxKeyBytes:             64,
		MaxValueBytes:           4,
		MaxTotalBytesPerEntity:  1 << 20,
		MaxTotalBytesPerCommand: 1 << 20,
	}

	tooBig := &commonpb.MetadataMap{Values: map[string]*commonpb.MetadataValue{
		"k": commonpb.NewStringValue(strings.Repeat("v", 5)),
	}}
	accountMetadata := map[string]*commonpb.MetadataMap{
		"users:zoe":   tooBig,
		"users:alice": tooBig,
		"users:mia":   tooBig,
	}

	first := validateMergedAccountMetadata(accountMetadata, limits)
	require.NotNil(t, first)

	for range 100 {
		again := validateMergedAccountMetadata(accountMetadata, limits)
		require.NotNil(t, again)
		require.Equal(t, first.Error(), again.Error())
		require.Equal(t, first.Metadata(), again.Metadata())
	}

	require.Equal(t, "users:alice", first.Metadata()["account"])
}

// Nothing to check means nothing to reject: an empty account-map set is not a
// failure, and a nil map for an account carries no metadata.
func TestValidateMergedAccountMetadataAcceptsEmpty(t *testing.T) {
	t.Parallel()

	limits := domain.MetadataLimitsFromPolicy(defaultTestClusterPolicy())

	require.Nil(t, validateMergedAccountMetadata(nil, limits))
	require.Nil(t, validateMergedAccountMetadata(map[string]*commonpb.MetadataMap{"users:alice": nil}, limits))
	require.Nil(t, validateMergedAccountMetadata(
		map[string]*commonpb.MetadataMap{"users:alice": {Values: map[string]*commonpb.MetadataValue{}}}, limits))
}
