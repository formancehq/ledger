package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	numscriptlib "github.com/formancehq/numscript"

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

// compileArtifactForTest builds the (program, vars, script hash) triple the way
// admission's compileScript does, so producer tests can stage it on the
// producer exactly as the dispatcher would from OrderTechnical.
func compileArtifactForTest(t *testing.T, script string, vars map[string]string) (programBytes, varsBytes, scriptHash []byte) {
	t.Helper()

	varsEncoder, program, err := numscriptlib.Compile(script)
	require.NoError(t, err)

	encodedVars, err := varsEncoder.Encode(vars)
	require.NoError(t, err)

	hash := numscript.HashScript(script)

	return program.Encode(), encodedVars.Encode(), hash[:]
}

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

// TestProduce_CompiledArtifactExecutesOnTheVM: an order carrying the
// admission-compiled artifact executes on the VM and produces the same result
// the interpreter produces for the same script and state.
func TestProduce_CompiledArtifactExecutesOnTheVM(t *testing.T) {
	t.Parallel()

	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, vmScriptVars)

	runProduce := func(t *testing.T, withArtifact bool) *produceResult {
		t.Helper()

		producer := &numscriptPostingProducer{
			cache:      numscript.NewNumscriptCache(16),
			ledgerName: "test",
			assetCache: map[string]cachedAssetPrecision{},
		}
		if withArtifact {
			producer.compiledProgram = programBytes
			producer.compiledVars = varsBytes
			producer.compiledScriptHash = scriptHash
		}

		result, err := producer.produce(vmTestScope(t), "test",
			&raftcmdpb.CreateTransactionOrder{},
			&commonpb.Script{Plain: vmScript, Vars: vmScriptVars})
		require.Nil(t, err)

		return result
	}

	vmResult := runProduce(t, true)
	interpreterResult := runProduce(t, false)

	require.Len(t, vmResult.Postings, 1)
	require.Equal(t, "wallet", vmResult.Postings[0].GetSource())
	require.Equal(t, "out", vmResult.Postings[0].GetDestination())
	require.Equal(t, "USD/2", vmResult.Postings[0].GetAsset())
	require.Equal(t, uint64(100), vmResult.Postings[0].GetAmount().ToBigInt().Uint64())
	require.Equal(t, "vm-test", commonpb.MetadataValueToString(vmResult.TransactionMetadata["kind"]))

	// Engine parity on the same state: the two paths must be indistinguishable.
	require.Equal(t, len(interpreterResult.Postings), len(vmResult.Postings))
	for i := range vmResult.Postings {
		require.True(t, interpreterResult.Postings[i].EqualVT(vmResult.Postings[i]))
	}
	require.Equal(t, len(interpreterResult.TransactionMetadata), len(vmResult.TransactionMetadata))
}

// TestProduce_CompiledArtifactHashMismatchIsLoud: an artifact bound to a
// different script text must surface loudly (invariant #7), never execute and
// never fall back silently.
func TestProduce_CompiledArtifactHashMismatchIsLoud(t *testing.T) {
	t.Parallel()

	programBytes, varsBytes, _ := compileArtifactForTest(t, vmScript, vmScriptVars)
	otherHash := numscript.HashScript("send [USD/2 1] (source = @a destination = @b)")

	producer := &numscriptPostingProducer{
		cache:              numscript.NewNumscriptCache(16),
		ledgerName:         "test",
		assetCache:         map[string]cachedAssetPrecision{},
		compiledProgram:    programBytes,
		compiledVars:       varsBytes,
		compiledScriptHash: otherHash[:],
	}

	_, err := producer.produce(vmTestScope(t), "test",
		&raftcmdpb.CreateTransactionOrder{},
		&commonpb.Script{Plain: vmScript, Vars: vmScriptVars})

	require.NotNil(t, err)

	var runtimeErr *domain.ErrNumscriptRuntime
	require.ErrorAs(t, err, &runtimeErr)
}

// TestProduce_CorruptedArtifactIsLoud: bytes that fail to decode or verify are
// an internal error (our compiler produced them), not a panic and not a client
// error.
func TestProduce_CorruptedArtifactIsLoud(t *testing.T) {
	t.Parallel()

	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, vmScriptVars)

	corrupted := append([]byte(nil), programBytes...)
	for i := range corrupted {
		corrupted[i] ^= 0xA5
	}

	producer := &numscriptPostingProducer{
		cache:              numscript.NewNumscriptCache(16),
		ledgerName:         "test",
		assetCache:         map[string]cachedAssetPrecision{},
		compiledProgram:    corrupted,
		compiledVars:       varsBytes,
		compiledScriptHash: scriptHash,
	}

	_, err := producer.produce(vmTestScope(t), "test",
		&raftcmdpb.CreateTransactionOrder{},
		&commonpb.Script{Plain: vmScript, Vars: vmScriptVars})

	require.NotNil(t, err)

	var runtimeErr *domain.ErrNumscriptRuntime
	require.ErrorAs(t, err, &runtimeErr)
}

// TestProduce_VMMissingFundsMatchesInterpreterClassification: the VM's
// missing-funds failure maps to the same domain error the interpreter path
// raises, so the client-facing classification does not depend on the engine.
func TestProduce_VMMissingFundsMatchesInterpreterClassification(t *testing.T) {
	t.Parallel()

	shortVars := map[string]string{"amt": "USD/2 5000"}
	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, shortVars)

	producer := &numscriptPostingProducer{
		cache:              numscript.NewNumscriptCache(16),
		ledgerName:         "test",
		assetCache:         map[string]cachedAssetPrecision{},
		compiledProgram:    programBytes,
		compiledVars:       varsBytes,
		compiledScriptHash: scriptHash,
	}

	_, err := producer.produce(vmTestScope(t), "test",
		&raftcmdpb.CreateTransactionOrder{},
		&commonpb.Script{Plain: vmScript, Vars: shortVars})

	require.NotNil(t, err)

	var insufficientFunds *domain.ErrInsufficientFunds
	require.ErrorAs(t, err, &insufficientFunds)
	require.Equal(t, "USD/2", insufficientFunds.Asset)
}
