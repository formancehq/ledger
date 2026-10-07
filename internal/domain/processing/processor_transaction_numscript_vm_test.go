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

// produceVMScript runs the producer on vmScript against a fresh test scope and
// a fresh cache, staging the given artifact (all nil for an order without
// one) the way the dispatcher would from OrderTechnical.
func produceVMScript(t *testing.T, vars map[string]string, programBytes, varsBytes, scriptHash []byte) (*produceResult, domain.SerializableError) {
	t.Helper()

	return produceVMScriptWithCache(t, numscript.NewNumscriptCache(16), vars, programBytes, varsBytes, scriptHash)
}

// produceVMScriptWithCache is produceVMScript against a caller-supplied cache,
// for tests that need to observe behavior across more than one order sharing
// the same node-local cache (e.g. a program omitted because a prior order on
// this cache already warmed it).
func produceVMScriptWithCache(t *testing.T, cache *numscript.NumscriptCache, vars map[string]string, programBytes, varsBytes, scriptHash []byte) (*produceResult, domain.SerializableError) {
	t.Helper()

	producer := &numscriptPostingProducer{
		cache:              cache,
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
func requireNumscriptRuntimeError(t *testing.T, err domain.SerializableError, detail string) {
	t.Helper()

	require.NotNil(t, err)
	require.False(t, numscript.IsPanic(err))

	var runtimeErr *domain.ErrNumscriptRuntime
	require.ErrorAs(t, err, &runtimeErr)
	require.Contains(t, runtimeErr.Detail, detail)
}

// TestProduce_CompiledArtifactExecutesOnTheVM: an order carrying the
// admission-compiled artifact executes on the VM and produces the expected
// postings and transaction metadata.
func TestProduce_CompiledArtifactExecutesOnTheVM(t *testing.T) {
	t.Parallel()

	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, vmScriptVars)

	vmResult, err := produceVMScript(t, vmScriptVars, programBytes, varsBytes, scriptHash)
	require.Nil(t, err)

	require.Len(t, vmResult.Postings, 1)
	require.Equal(t, "wallet", vmResult.Postings[0].GetSource())
	require.Equal(t, "out", vmResult.Postings[0].GetDestination())
	require.Equal(t, "USD/2", vmResult.Postings[0].GetAsset())
	require.Equal(t, uint64(100), vmResult.Postings[0].GetAmount().ToBigInt().Uint64())
	require.Equal(t, "vm-test", commonpb.MetadataValueToString(vmResult.TransactionMetadata["kind"]))
}

// TestProduce_MissingArtifactRecompilesToTheSameOutcome: the artifact is
// derivable from the script text, so an order without one is recompiled and
// produces exactly what the admission-compiled artifact produces — still on
// the VM, the only engine.
func TestProduce_MissingArtifactRecompilesToTheSameOutcome(t *testing.T) {
	t.Parallel()

	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, vmScriptVars)

	withArtifact, err := produceVMScript(t, vmScriptVars, programBytes, varsBytes, scriptHash)
	require.Nil(t, err)

	recompiled, err := produceVMScript(t, vmScriptVars, nil, nil, nil)
	require.Nil(t, err)

	require.Equal(t, withArtifact, recompiled)
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

// unreadableBytecodeVersions returns the bytecode versions around the bundled
// one that it cannot read: another major or a newer minor always, and under an
// unstable (0.x) bundled version an older minor too.
func unreadableBytecodeVersions(t *testing.T) map[string]numscriptlib.BytecodeVersion {
	t.Helper()

	current := numscriptlib.CurrentBytecodeVersion
	candidates := map[string]numscriptlib.BytecodeVersion{
		"newer major": {Major: current.Major + 1},
		"newer minor": {Major: current.Major, Minor: current.Minor + 1},
	}

	if current.Major > 0 {
		candidates["older major"] = numscriptlib.BytecodeVersion{Major: current.Major - 1, Minor: current.Minor}
	}

	if current.Minor > 0 {
		candidates["older minor"] = numscriptlib.BytecodeVersion{Major: current.Major, Minor: current.Minor - 1}
	}

	for name, v := range candidates {
		if current.CanRead(v) {
			delete(candidates, name)
		}
	}

	require.Contains(t, candidates, "newer major")
	require.Contains(t, candidates, "newer minor")

	return candidates
}

// TestProduce_UnreadableBytecodeVersionIsLoud: an artifact carrying a
// bytecode version the bundled library cannot read fails the order loudly, for
// either half. Admission produces the artifact with this very library and v3
// has no cross-version replay contract, so a present artifact is never
// repaired from the script text.
func TestProduce_UnreadableBytecodeVersionIsLoud(t *testing.T) {
	t.Parallel()

	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, vmScriptVars)

	for name, v := range unreadableBytecodeVersions(t) {
		for _, half := range []string{"program", "vars"} {
			t.Run(name+" "+half, func(t *testing.T) {
				t.Parallel()

				program, vars := programBytes, varsBytes
				if half == "program" {
					program = withArtifactVersion(t, program, v)
				} else {
					vars = withArtifactVersion(t, vars, v)
				}

				_, err := produceVMScript(t, vmScriptVars, program, vars, scriptHash)
				requireNumscriptRuntimeError(t, err, "decoding compiled numscript "+half)
				require.Contains(t, err.Error(), "not readable by this build")
			})
		}
	}
}

// artifactHeaderLen is the library's fixed header: a 4-byte magic, major and
// minor as two little-endian uint16, then the uint16 section count.
const artifactHeaderLen = 10

// TestProduce_CorruptedArtifactIsLoud: a program whose header this build reads
// but whose body the bundled library cannot decode is an internal error (our
// own codec wrote it), not a panic, not a client error and never a recompile.
func TestProduce_CorruptedArtifactIsLoud(t *testing.T) {
	t.Parallel()

	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, vmScriptVars)

	corrupted := bytes.Clone(programBytes)
	for i := artifactHeaderLen; i < len(corrupted); i++ {
		corrupted[i] ^= 0xA5
	}

	version, peekErr := numscriptlib.PeekCompiledProgramVersion(corrupted)
	require.NoError(t, peekErr)
	require.True(t, numscriptlib.CurrentBytecodeVersion.CanRead(version))

	_, err := produceVMScript(t, vmScriptVars, corrupted, varsBytes, scriptHash)
	requireNumscriptRuntimeError(t, err, "decoding compiled numscript program")
}

// TestProduce_InvalidArtifactHeaderIsLoud: a half without a valid header is a
// corrupt artifact, not a missing one: it fails the order loudly and is never
// recompiled from the script text, for either half.
func TestProduce_InvalidArtifactHeaderIsLoud(t *testing.T) {
	t.Parallel()

	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, vmScriptVars)

	for _, half := range []string{"program", "vars"} {
		t.Run(half, func(t *testing.T) {
			t.Parallel()

			program, vars := bytes.Clone(programBytes), bytes.Clone(varsBytes)
			if half == "program" {
				program[0] ^= 0xFF
			} else {
				vars[0] ^= 0xFF
			}

			_, err := produceVMScript(t, vmScriptVars, program, vars, scriptHash)
			requireNumscriptRuntimeError(t, err, "decoding compiled numscript "+half)
			require.Contains(t, err.Error(), "bad magic")
		})
	}
}

// TestProduce_PartialArtifactIsLoud: a program alone may be legitimately
// absent (vars and hash present) — see
// TestProduce_OmittedProgramRecompilesToTheSameOutcome — but every other
// partial shape is a corrupt artifact admission never produces, and fails the
// order loudly.
func TestProduce_PartialArtifactIsLoud(t *testing.T) {
	t.Parallel()

	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, vmScriptVars)

	for name, tc := range map[string]struct {
		program, vars, hash []byte
		detail              string
	}{
		"no vars":                {programBytes, nil, scriptHash, "decoding compiled numscript vars"},
		"no hash":                {programBytes, varsBytes, nil, "does not match the resolved script text"},
		"hash only":              {nil, nil, scriptHash, "decoding compiled numscript vars"},
		"no program, short hash": {nil, varsBytes, scriptHash[:8], "script hash has 8 bytes, want 16"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := produceVMScript(t, vmScriptVars, tc.program, tc.vars, tc.hash)
			requireNumscriptRuntimeError(t, err, tc.detail)
		})
	}
}

// TestProduce_OmittedProgramRecompilesToTheSameOutcome: admission may omit
// compiled_program alone (keeping vars and hash) once it has already attached
// these exact bytes to an earlier proposal for this hash. On a cache miss —
// the case here, a fresh cache, which is also the ordinary case since
// nothing admission does ever warms this node's apply-side cache — this node
// recompiles from the script text and produces exactly what the full
// artifact produces.
func TestProduce_OmittedProgramRecompilesToTheSameOutcome(t *testing.T) {
	t.Parallel()

	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, vmScriptVars)

	withArtifact, err := produceVMScript(t, vmScriptVars, programBytes, varsBytes, scriptHash)
	require.Nil(t, err)

	omittedProgram, err := produceVMScript(t, vmScriptVars, nil, varsBytes, scriptHash)
	require.Nil(t, err)

	require.Equal(t, withArtifact, omittedProgram)
}

// TestProduce_OmittedProgramUsesWarmCache: when a prior order on this node's
// own apply-side cache already decoded and verified this hash's bytes (this
// node having applied an earlier committed entry that carried the program),
// a later order omitting the program serves those cached bytes directly — no
// recompile — and produces exactly the same outcome. Admission's decision to
// omit is independent of this (see admission.go's SeenCompiledProgram use,
// its own separate bookkeeping) — this test only exercises the FSM apply
// side's PeekCompiledProgram lookup.
func TestProduce_OmittedProgramUsesWarmCache(t *testing.T) {
	t.Parallel()

	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, vmScriptVars)
	cache := numscript.NewNumscriptCache(16)

	warming, err := produceVMScriptWithCache(t, cache, vmScriptVars, programBytes, varsBytes, scriptHash)
	require.Nil(t, err)

	omittedProgram, err := produceVMScriptWithCache(t, cache, vmScriptVars, nil, varsBytes, scriptHash)
	require.Nil(t, err)

	require.Equal(t, warming, omittedProgram)
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

// TestProduce_VMMissingFundsIsInsufficientFunds: the VM's missing-funds
// failure maps to the client-facing ErrInsufficientFunds, not a runtime error.
func TestProduce_VMMissingFundsIsInsufficientFunds(t *testing.T) {
	t.Parallel()

	shortVars := map[string]string{"amt": "USD/2 5000"}
	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, shortVars)

	_, err := produceVMScript(t, shortVars, programBytes, varsBytes, scriptHash)
	require.NotNil(t, err)

	var insufficientFunds *domain.ErrInsufficientFunds
	require.ErrorAs(t, err, &insufficientFunds)
	require.Equal(t, "USD/2", insufficientFunds.Asset)
}
