package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestNumscriptVMProducer_SimpleSend sanity-checks the bytecode VM path end to
// end: compile (via the cache) → encode vars → NewVm → ExecVm → apply postings.
func TestNumscriptVMProducer_SimpleSend(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	setupNumscriptVolumeMocks(mockStore)

	producer := &numscriptVMPostingProducer{
		cache:      numscript.NewNumscriptCache(16),
		ledgerName: "test-ledger",
	}

	script := &commonpb.Script{Plain: `
		send [USD/2 10] (
			source = @world
			destination = @users:alice
		)
	`}

	result, derr := producer.produce(mockStore, "test-ledger", &raftcmdpb.CreateTransactionOrder{}, script)
	require.Nil(t, derr)
	require.Len(t, result.Postings, 1)
	require.Equal(t, "world", result.Postings[0].GetSource())
	require.Equal(t, "users:alice", result.Postings[0].GetDestination())
	require.Equal(t, "USD/2", result.Postings[0].GetAsset())
	require.Equal(t, int64(10), result.Postings[0].GetAmount().ToBigInt().Int64())
}

// TestNumscriptVMProducer_MatchesInterpreter pins that the VM producer yields
// the same postings as the tree-walking interpreter producer for the same
// script and store state (here an allotment split from @world).
func TestNumscriptVMProducer_MatchesInterpreter(t *testing.T) {
	t.Parallel()

	script := &commonpb.Script{Plain: `
		send [USD/2 100] (
			source = @world
			destination = {
				50% to @users:alice
				50% to @users:bob
			}
		)
	`}

	run := func(vm bool) []*commonpb.Posting {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStore := NewMockScope(ctrl)
		setupNumscriptVolumeMocks(mockStore)

		cache := numscript.NewNumscriptCache(16)

		var producer postingProducer
		if vm {
			producer = &numscriptVMPostingProducer{cache: cache, ledgerName: "test-ledger"}
		} else {
			producer = &numscriptPostingProducer{cache: cache, ledgerName: "test-ledger"}
		}

		result, derr := producer.produce(mockStore, "test-ledger", &raftcmdpb.CreateTransactionOrder{}, script)
		require.Nil(t, derr)

		return result.Postings
	}

	interpreterPostings := run(false)
	vmPostings := run(true)

	require.Len(t, vmPostings, 2)
	require.Len(t, interpreterPostings, len(vmPostings))
	for i := range vmPostings {
		require.Equal(t, interpreterPostings[i].GetSource(), vmPostings[i].GetSource())
		require.Equal(t, interpreterPostings[i].GetDestination(), vmPostings[i].GetDestination())
		require.Equal(t, interpreterPostings[i].GetAsset(), vmPostings[i].GetAsset())
		require.Equal(t,
			interpreterPostings[i].GetAmount().ToBigInt().Int64(),
			vmPostings[i].GetAmount().ToBigInt().Int64(),
		)
	}
}
