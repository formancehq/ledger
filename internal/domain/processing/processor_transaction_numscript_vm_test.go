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

// testArtifact is the technical artifact a producer test stages, in any
// combination of the four OrderTechnical fields — the shapes admission
// produces and the ones it never does (see classifyCompiledArtifact).
type testArtifact struct {
	program, programHash, vars, scriptHash []byte
}

// byValueArtifact is the shape of a script's first order from an admission
// instance: the bytecode itself, next to vars and script hash.
func byValueArtifact(programBytes, varsBytes, scriptHash []byte) testArtifact {
	return testArtifact{program: programBytes, vars: varsBytes, scriptHash: scriptHash}
}

// byReferenceArtifact is the shape of every later order of the script from
// that instance: the bytecode's hash in place of the bytes.
func byReferenceArtifact(programBytes, varsBytes, scriptHash []byte) testArtifact {
	programHash := numscript.HashProgram(programBytes)

	return testArtifact{programHash: programHash[:], vars: varsBytes, scriptHash: scriptHash}
}

// produceVMScript runs the producer on vmScript against a fresh test scope and
// a fresh cache, staging the given by-value artifact (all nil for an order
// without one) the way the dispatcher would from OrderTechnical.
func produceVMScript(t *testing.T, vars map[string]string, programBytes, varsBytes, scriptHash []byte) (*produceResult, domain.SerializableError) {
	t.Helper()

	return produceVMScriptArtifact(t, numscript.NewNumscriptCache(16), vars, byValueArtifact(programBytes, varsBytes, scriptHash))
}

// produceVMScriptWithCache is produceVMScript against a caller-supplied cache,
// for tests that need to observe behavior across more than one order sharing
// the same node-local cache (e.g. a by-value order warming it for a later
// by-reference one).
func produceVMScriptWithCache(t *testing.T, cache *numscript.NumscriptCache, vars map[string]string, programBytes, varsBytes, scriptHash []byte) (*produceResult, domain.SerializableError) {
	t.Helper()

	return produceVMScriptArtifact(t, cache, vars, byValueArtifact(programBytes, varsBytes, scriptHash))
}

// produceVMScriptArtifact runs the producer on vmScript against a fresh test
// scope and the given cache, staging artifact as the dispatcher would from
// OrderTechnical.
func produceVMScriptArtifact(t *testing.T, cache *numscript.NumscriptCache, vars map[string]string, artifact testArtifact) (*produceResult, domain.SerializableError) {
	t.Helper()

	producer := &numscriptPostingProducer{
		cache:               cache,
		ledgerName:          "test",
		assetCache:          map[string]cachedAssetPrecision{},
		compiledProgram:     artifact.program,
		compiledProgramHash: artifact.programHash,
		compiledVars:        artifact.vars,
		compiledScriptHash:  artifact.scriptHash,
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

// TestProduce_UnreadableBytecodeVersionDerivesFromText: an artifact carrying a
// bytecode version the bundled library cannot read, in either half, came from
// another library version — a replica mid rolling upgrade — and is not a
// failure: the order is derived from the script text with this binary's own
// library, business vars included (the committed vars say 999, the business
// vars 100), to the outcome a readable artifact produces.
func TestProduce_UnreadableBytecodeVersionDerivesFromText(t *testing.T) {
	t.Parallel()

	programBytes, committedVars, scriptHash := compileArtifactForTest(t, vmScript, map[string]string{"amt": "USD/2 999"})

	expected, err := produceVMScript(t, vmScriptVars, nil, nil, nil)
	require.Nil(t, err)

	for name, v := range unreadableBytecodeVersions(t) {
		for _, half := range []string{"program", "vars"} {
			t.Run(name+" "+half, func(t *testing.T) {
				t.Parallel()

				program, vars := programBytes, committedVars
				if half == "program" {
					program = withArtifactVersion(t, program, v)
				} else {
					vars = withArtifactVersion(t, vars, v)
				}

				derived, err := produceVMScript(t, vmScriptVars, program, vars, scriptHash)
				require.Nil(t, err)
				require.Equal(t, expected, derived)
				require.Equal(t, uint64(100), derived.Postings[0].GetAmount().ToBigInt().Uint64())
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

// TestProduce_PartialArtifactIsLoud: the artifact arrives by value, by
// reference (see TestProduce_ProgramByReference*), or not at all; every other
// combination of the four fields is a corrupt artifact admission never
// produces and fails the order loudly — identically on a cold cache and on
// one already warm for the script, since the shape is classified before any
// cache access and vars are never re-derived from the business script fields
// (invariant #2). The one by-reference shape with a malformed script hash is
// a well-formed shape that fails the hash binding instead.
func TestProduce_PartialArtifactIsLoud(t *testing.T) {
	t.Parallel()

	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, vmScriptVars)
	programHash := numscript.HashProgram(programBytes)

	for name, tc := range map[string]struct {
		artifact testArtifact
		detail   string
	}{
		"no vars":                           {testArtifact{program: programBytes, scriptHash: scriptHash}, "is partial"},
		"no script hash":                    {testArtifact{program: programBytes, vars: varsBytes}, "is partial"},
		"script hash only":                  {testArtifact{scriptHash: scriptHash}, "is partial"},
		"vars only":                         {testArtifact{vars: varsBytes}, "is partial"},
		"program by value and by reference": {testArtifact{program: programBytes, programHash: programHash[:], vars: varsBytes, scriptHash: scriptHash}, "is partial"},
		"program hash without vars":         {testArtifact{programHash: programHash[:], scriptHash: scriptHash}, "is partial"},
		"program hash alone":                {testArtifact{programHash: programHash[:]}, "is partial"},
		"short program hash":                {testArtifact{programHash: programHash[:8], vars: varsBytes, scriptHash: scriptHash}, "is partial"},
		"by reference, short script hash":   {testArtifact{programHash: programHash[:], vars: varsBytes, scriptHash: scriptHash[:8]}, "does not match the resolved script text"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, coldErr := produceVMScriptArtifact(t, numscript.NewNumscriptCache(16), vmScriptVars, tc.artifact)
			requireNumscriptRuntimeError(t, coldErr, tc.detail)

			warm := numscript.NewNumscriptCache(16)
			_, err := produceVMScriptArtifact(t, warm, vmScriptVars, byValueArtifact(programBytes, varsBytes, scriptHash))
			require.Nil(t, err)

			_, warmErr := produceVMScriptArtifact(t, warm, vmScriptVars, tc.artifact)
			require.Equal(t, coldErr, warmErr, "a corrupt shape must fail identically whatever this node's cache holds")
		})
	}
}

// TestProduce_ProgramByReferenceRecompilesToTheSameOutcome: a by-reference
// order on a cold cache (restart, eviction, late joiner) compiles the script
// text, accepts the bytes because they hash to the committed program hash,
// and produces exactly what the by-value artifact produces.
func TestProduce_ProgramByReferenceRecompilesToTheSameOutcome(t *testing.T) {
	t.Parallel()

	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, vmScriptVars)

	byValue, err := produceVMScript(t, vmScriptVars, programBytes, varsBytes, scriptHash)
	require.Nil(t, err)

	byReference, err := produceVMScriptArtifact(t, numscript.NewNumscriptCache(16), vmScriptVars, byReferenceArtifact(programBytes, varsBytes, scriptHash))
	require.Nil(t, err)

	require.Equal(t, byValue, byReference)
}

// TestProduce_ProgramByReferenceUsesWarmCache: when this node applied the
// earlier by-value order of the script (the steady state — that order warmed
// every replica), a by-reference order runs the cached bytes directly, no
// compile, to exactly the same outcome.
func TestProduce_ProgramByReferenceUsesWarmCache(t *testing.T) {
	t.Parallel()

	programBytes, varsBytes, scriptHash := compileArtifactForTest(t, vmScript, vmScriptVars)
	cache := numscript.NewNumscriptCache(16)

	byValue, err := produceVMScriptWithCache(t, cache, vmScriptVars, programBytes, varsBytes, scriptHash)
	require.Nil(t, err)

	byReference, err := produceVMScriptArtifact(t, cache, vmScriptVars, byReferenceArtifact(programBytes, varsBytes, scriptHash))
	require.Nil(t, err)

	require.Equal(t, byValue, byReference)
}

// TestProduce_ProgramByReferenceUsesCommittedVarsOnHitAndMiss: the committed
// compiledVars run on both paths and are never re-derived from the business
// script fields — a cold cache recompiles only the program and pairs it with
// the same committed vars a warm cache uses directly. compiledVars here
// intentionally differs from vmScriptVars (what a recompile that bound vars
// itself would use): the posting must reflect compiledVars either way, and
// the two outcomes must be identical (invariant #2).
func TestProduce_ProgramByReferenceUsesCommittedVarsOnHitAndMiss(t *testing.T) {
	t.Parallel()

	programBytes, differentVarsBytes, scriptHash := compileArtifactForTest(t, vmScript, map[string]string{"amt": "USD/2 999"})
	byReference := byReferenceArtifact(programBytes, differentVarsBytes, scriptHash)

	cold, err := produceVMScriptArtifact(t, numscript.NewNumscriptCache(16), vmScriptVars, byReference)
	require.Nil(t, err)
	require.Equal(t, uint64(999), cold.Postings[0].GetAmount().ToBigInt().Uint64())

	cache := numscript.NewNumscriptCache(16)
	_, err = produceVMScriptWithCache(t, cache, vmScriptVars, programBytes, differentVarsBytes, scriptHash)
	require.Nil(t, err)

	warm, err := produceVMScriptArtifact(t, cache, vmScriptVars, byReference)
	require.Nil(t, err)
	require.Equal(t, uint64(999), warm.Postings[0].GetAmount().ToBigInt().Uint64())

	require.Equal(t, cold, warm, "outcome must not depend on this node's own cache state")
}

// TestProduce_IrreproducibleReferenceDerivesFromText: a by-reference order
// whose hash this node can neither find in its cache nor reproduce by
// compiling the script text was compiled by another library version. It is
// not a failure, and the committed vars (999) are never run against a program
// they were not encoded for: program and vars are derived from the script
// text with this binary's own library (business vars, 100). Here the hash
// names another script's bytecode, standing in for a foreign compilation.
func TestProduce_IrreproducibleReferenceDerivesFromText(t *testing.T) {
	t.Parallel()

	_, committedVars, scriptHash := compileArtifactForTest(t, vmScript, map[string]string{"amt": "USD/2 999"})
	otherProgram, _, _ := compileArtifactForTest(t, `send [USD/2 1] (source = @wallet destination = @out)`, nil)

	expected, err := produceVMScript(t, vmScriptVars, nil, nil, nil)
	require.Nil(t, err)

	derived, err := produceVMScriptArtifact(t, numscript.NewNumscriptCache(16), vmScriptVars, byReferenceArtifact(otherProgram, committedVars, scriptHash))
	require.Nil(t, err)
	require.Equal(t, expected, derived)
	require.Equal(t, uint64(100), derived.Postings[0].GetAmount().ToBigInt().Uint64())
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
