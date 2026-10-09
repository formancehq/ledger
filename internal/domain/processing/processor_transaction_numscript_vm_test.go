package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

const vmScript = `vars {
  monetary $amt
}

send $amt (
  source = @wallet
  destination = @out
)

set_tx_meta("kind", "vm-test")`

var vmScriptVars = map[string]string{"amt": "USD/2 100"}

func vmTestScope(t *testing.T) *MockScope {
	t.Helper()

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	volumes := setupVolumesStub(mockStore)
	walletVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(1000),
		Output: commonpb.NewUint256FromUint64(0),
	}
	volumes.expectGet(domain.NewVolumeKey("test", "wallet", "USD/2", ""), walletVol.AsReader(), nil)

	return mockStore
}

func produceTextScript(t *testing.T, cache *numscript.NumscriptCache, vars map[string]string) (*produceResult, domain.SerializableError) {
	t.Helper()
	producer := &numscriptPostingProducer{
		cache:      cache,
		ledgerName: "test",
		assetCache: map[string]cachedAssetPrecision{},
	}

	return producer.produce(vmTestScope(t), "test", &raftcmdpb.CreateTransactionOrder{}, &commonpb.Script{Plain: vmScript, Vars: vars})
}

func TestProduce_TextScriptColdAndWarmAgree(t *testing.T) {
	t.Parallel()
	cache := numscript.NewNumscriptCache(16)
	cold, err := produceTextScript(t, cache, vmScriptVars)
	require.Nil(t, err)
	warm, err := produceTextScript(t, cache, vmScriptVars)
	require.Nil(t, err)
	require.Equal(t, cold, warm)
	require.Len(t, warm.Postings, 1)
	require.Equal(t, "wallet", warm.Postings[0].GetSource())
	require.Equal(t, "out", warm.Postings[0].GetDestination())
	require.Equal(t, "USD/2", warm.Postings[0].GetAsset())
	require.Equal(t, uint64(100), warm.Postings[0].GetAmount().ToBigInt().Uint64())
	require.Equal(t, "vm-test", commonpb.MetadataValueToString(warm.TransactionMetadata["kind"]))
}

func TestProduce_TextScriptUsesEachOrdersVars(t *testing.T) {
	t.Parallel()
	cache := numscript.NewNumscriptCache(16)
	_, err := produceTextScript(t, cache, vmScriptVars)
	require.Nil(t, err)
	result, err := produceTextScript(t, cache, map[string]string{"amt": "USD/2 200"})
	require.Nil(t, err)
	require.Equal(t, uint64(200), result.Postings[0].GetAmount().ToBigInt().Uint64())
}

func TestProduce_VMMissingFundsIsInsufficientFunds(t *testing.T) {
	t.Parallel()
	_, err := produceTextScript(t, numscript.NewNumscriptCache(16), map[string]string{"amt": "USD/2 5000"})
	require.NotNil(t, err)
	var insufficientFunds *domain.ErrInsufficientFunds
	require.ErrorAs(t, err, &insufficientFunds)
	require.Equal(t, "USD/2", insufficientFunds.Asset)
}

// TestProduce_ForcedPostingWiderThan256BitsIsExecutionError: under force every
// balance() reads 2^256, so sending it produces a posting no uint256 volume can
// hold. The script caused it, so it is a client error, not NUMSCRIPT_RUNTIME.
func TestProduce_ForcedPostingWiderThan256BitsIsExecutionError(t *testing.T) {
	t.Parallel()

	const script = `vars {
  monetary $all = balance(@wallet, USD/2)
}

send $all (
  source = @wallet
  destination = @out
)`

	producer := &numscriptPostingProducer{
		cache:      numscript.NewNumscriptCache(16),
		ledgerName: "test",
		assetCache: map[string]cachedAssetPrecision{},
	}

	_, err := producer.produce(vmTestScope(t), "test",
		&raftcmdpb.CreateTransactionOrder{Force: true},
		&commonpb.Script{Plain: script})
	require.NotNil(t, err)

	var execErr *domain.ErrNumscriptExecution
	require.ErrorAs(t, err, &execErr)
	require.Contains(t, execErr.Detail, "exceeds 256 bits")
	require.Equal(t, domain.KindPrecondition, err.Kind())
}
