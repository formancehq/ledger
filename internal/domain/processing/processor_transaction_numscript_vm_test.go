package processing

import (
	"bytes"
	"encoding/binary"
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

// withArtifactVersion returns a copy of an encoded program or vars blob with
// its bytecode version header rewritten: a 4-byte magic, then major and minor
// as two little-endian uint16, the one part of the library's layout that has
// to stay put for any versioning to work at all.
func withArtifactVersion(t *testing.T, encoded []byte, version numscriptlib.BytecodeVersion) []byte {
	t.Helper()

	require.GreaterOrEqual(t, len(encoded), 8)

	patched := bytes.Clone(encoded)
	binary.LittleEndian.PutUint16(patched[4:], version.Major)
	binary.LittleEndian.PutUint16(patched[6:], version.Minor)

	return patched
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

// produceVMScript runs the producer on vmScript against a fresh test scope,
// staging the given artifact (all nil for the plain interpreter path) the way
// the dispatcher would from OrderTechnical.
func produceVMScript(t *testing.T, vars map[string]string, programBytes, varsBytes, scriptHash []byte) (*produceResult, domain.Describable) {
	t.Helper()

	producer := &numscriptPostingProducer{
		cache:              numscript.NewNumscriptCache(16),
		ledgerName:         "test",
		assetCache:         map[string]cachedAssetPrecision{},
		compiledProgram:    programBytes,
		compiledVars:       varsBytes,
		compiledScriptHash: scriptHash,
	}

	return producer.produce(vmTestScope(t), "test",
		&raftcmdpb.CreateTransactionOrder{},
		&commonpb.Script{Plain: vmScript, Vars: vars})
}

// requireNumscriptRuntimeError asserts the producer failed with the loud
// internal classification every artifact defect maps to — not a panic, not a
// client error — and that its detail names the intended branch.
func requireNumscriptRuntimeError(t *testing.T, err domain.Describable, detail string) {
	t.Helper()

	require.NotNil(t, err)
	require.False(t, numscript.IsPanic(err))

	var runtimeErr *domain.ErrNumscriptRuntime
	require.ErrorAs(t, err, &runtimeErr)
	require.Contains(t, runtimeErr.Detail, detail)
}

// TestProduce_CompiledArtifactExecutesOnTheVM: an order carrying the
// admission-compiled artifact executes on the VM and produces the same result
// the interpreter produces for the same script and state.
func TestProduce_CompiledArtifactExecutesOnTheVM(t *testing.T) {
	t.Parallel()

	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, vmScriptVars)

	vmResult, err := produceVMScript(t, vmScriptVars, programBytes, varsBytes, scriptHash)
	require.Nil(t, err)

	interpreterResult, err := produceVMScript(t, vmScriptVars, nil, nil, nil)
	require.Nil(t, err)

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

	_, err := produceVMScript(t, vmScriptVars, programBytes, varsBytes, otherHash[:])
	requireNumscriptRuntimeError(t, err, "does not match the resolved script text")
}

// TestProduce_ForeignBytecodeVersionIsLoud: an artifact carrying a bytecode
// version other than the bundled library's — a Raft log replayed across a
// library upgrade, or a rollback — fails the order loudly; the text is never
// interpreted in its place, even though it would run. Another major or a
// newer minor the library refuses at decode; an older minor of the same major
// the library would read and the ledger's exact-match check refuses (exercised
// as soon as the bundled version's minor is above zero). Either half.
func TestProduce_ForeignBytecodeVersionIsLoud(t *testing.T) {
	t.Parallel()

	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, vmScriptVars)

	current := numscriptlib.CurrentBytecodeVersion
	require.Positive(t, current.Major)

	const (
		libraryRefusal = "not readable by this build"
		ledgerRefusal  = "encoded with bytecode version"
	)

	versions := map[string]struct {
		v      numscriptlib.BytecodeVersion
		detail string
	}{
		"older major": {numscriptlib.BytecodeVersion{Major: current.Major - 1, Minor: current.Minor}, libraryRefusal},
		"newer major": {numscriptlib.BytecodeVersion{Major: current.Major + 1}, libraryRefusal},
		"newer minor": {numscriptlib.BytecodeVersion{Major: current.Major, Minor: current.Minor + 1}, libraryRefusal},
	}
	if current.Minor > 0 {
		versions["older minor"] = struct {
			v      numscriptlib.BytecodeVersion
			detail string
		}{numscriptlib.BytecodeVersion{Major: current.Major, Minor: current.Minor - 1}, ledgerRefusal}
	}

	for name, tc := range versions {
		for _, half := range []string{"program", "vars"} {
			t.Run(name+" "+half, func(t *testing.T) {
				t.Parallel()

				program, vars := programBytes, varsBytes
				if half == "program" {
					program = withArtifactVersion(t, program, tc.v)
				} else {
					vars = withArtifactVersion(t, vars, tc.v)
				}

				_, err := produceVMScript(t, vmScriptVars, program, vars, scriptHash)
				requireNumscriptRuntimeError(t, err, "compiled numscript "+half)
				require.Contains(t, err.Error(), tc.detail)
			})
		}
	}
}

// TestProduce_CorruptedArtifactIsLoud: bytes the bundled library cannot decode
// at all are an internal error (our own codec wrote them), not a panic, not a
// client error and never an interpreter fallback.
func TestProduce_CorruptedArtifactIsLoud(t *testing.T) {
	t.Parallel()

	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, vmScriptVars)

	corrupted := bytes.Clone(programBytes)
	for i := range corrupted {
		corrupted[i] ^= 0xA5
	}

	_, err := produceVMScript(t, vmScriptVars, corrupted, varsBytes, scriptHash)
	requireNumscriptRuntimeError(t, err, "decoding compiled numscript program")
}

// TestProduce_UnverifiableArtifactIsLoud: bytecode in the bundled format that
// decodes but fails verification is an internal error (our own compiler
// produced it in this very format), not a panic and not a client error.
func TestProduce_UnverifiableArtifactIsLoud(t *testing.T) {
	t.Parallel()

	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, vmScriptVars)

	program, decErr := numscriptlib.DecodeCompiledProgram(programBytes)
	require.NoError(t, decErr)
	require.NotEmpty(t, program.Instructions)

	program.Instructions[0].Opcode = 0xFF // no such opcode

	_, err := produceVMScript(t, vmScriptVars, program.Encode(), varsBytes, scriptHash)
	requireNumscriptRuntimeError(t, err, "verifying compiled numscript program")
}

// TestProduce_VMMissingFundsMatchesInterpreterClassification: the VM's
// missing-funds failure maps to the same domain error the interpreter path
// raises, so the client-facing classification does not depend on the engine.
func TestProduce_VMMissingFundsMatchesInterpreterClassification(t *testing.T) {
	t.Parallel()

	shortVars := map[string]string{"amt": "USD/2 5000"}
	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, shortVars)

	_, err := produceVMScript(t, shortVars, programBytes, varsBytes, scriptHash)
	require.NotNil(t, err)

	var insufficientFunds *domain.ErrInsufficientFunds
	require.ErrorAs(t, err, &insufficientFunds)
	require.Equal(t, "USD/2", insufficientFunds.Asset)
}
