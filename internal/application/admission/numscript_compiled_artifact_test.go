package admission

import (
	"testing"

	"github.com/stretchr/testify/require"

	numscriptlib "github.com/formancehq/numscript"

	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestResolveScripts_BindsCompiledArtifact: a resolvable inline script leaves
// admission with the VM artifact bound to OrderTechnical — bytecode that
// decodes and verifies, vars encoded against its layout, and the hash of the
// exact text it was compiled from. This is the leader's half of the contract;
// the FSM half (decode + verify + execute, hash guard, missing artifact)
// is covered by the processing package's producer tests.
func TestResolveScripts_BindsCompiledArtifact(t *testing.T) {
	t.Parallel()

	const script = `send [USD 10] (
	source = @world
	destination = @dst
)`

	admission, _ := createTestAdmission(t, createTestStore(t))
	order := scriptOrder(testLedgerName, script)

	require.NoError(t, runResolveProvenance(t, admission, []*raftcmdpb.Order{order}, false))

	technical := order.GetTechnical()
	require.NotEmpty(t, technical.GetCompiledProgram(), "the artifact must be bound to the order")
	require.Empty(t, technical.GetCompiledProgramHash(), "bytes sent by value carry no reference")

	hash := numscript.HashScript(script)
	require.Equal(t, hash[:], technical.GetCompiledScriptHash(),
		"the artifact must be bound to the exact compiled text")

	program, err := numscriptlib.DecodeCompiledProgram(technical.GetCompiledProgram())
	require.NoError(t, err)

	vars, err := numscriptlib.DecodeVars(technical.GetCompiledVars())
	require.NoError(t, err)

	_, err = numscriptlib.VerifyCompiledProgramWithVars(program, &vars)
	require.NoError(t, err, "the bound artifact must pass the same verification the FSM runs")
}

// TestResolveScripts_SendsBytecodeByReferenceOnceSent: once this admission
// instance's own compile cache has compiled a script (from an earlier order),
// a later order using the identical script carries the bytecode by reference
// — compiled_program_hash, the hash of exactly the bytes the first order
// carried — instead of by value, keeping vars and script hash. The signal is
// CompiledScript.AlreadyCompiled (see lruEntry.compileParsed), independent of
// whether the earlier proposal ever committed or of anything the FSM apply
// path cached on its own, separate NumscriptCache instance; the FSM side
// (serve from cache, or recompile and check the hash) is covered by the
// processing package's producer tests.
func TestResolveScripts_SendsBytecodeByReferenceOnceSent(t *testing.T) {
	t.Parallel()

	const script = `send [USD 10] (
	source = @world
	destination = @dst
)`

	admission, _ := createTestAdmission(t, createTestStore(t))

	first := scriptOrder(testLedgerName, script)
	require.NoError(t, runResolveProvenance(t, admission, []*raftcmdpb.Order{first}, false))

	firstTechnical := first.GetTechnical()
	require.NotEmpty(t, firstTechnical.GetCompiledProgram(), "the first use of a script must carry the bytecode")
	require.Empty(t, firstTechnical.GetCompiledProgramHash())

	second := scriptOrder(testLedgerName, script)
	require.NoError(t, runResolveProvenance(t, admission, []*raftcmdpb.Order{second}, false))

	secondTechnical := second.GetTechnical()
	require.Empty(t, secondTechnical.GetCompiledProgram(),
		"a script this admission instance has already sent must not carry the bytecode again")

	programHash := numscript.HashProgram(firstTechnical.GetCompiledProgram())
	require.Equal(t, programHash[:], secondTechnical.GetCompiledProgramHash(),
		"the reference must name exactly the bytes the first order carried")
	require.Equal(t, firstTechnical.GetCompiledVars(), secondTechnical.GetCompiledVars(),
		"vars are still bound per order when the program travels by reference")
	require.Equal(t, firstTechnical.GetCompiledScriptHash(), secondTechnical.GetCompiledScriptHash())
}

// TestResolveScripts_DifferentAdmissionInstancesEachSendBytecodeOnce: two
// independent admission instances (e.g. across a leadership change) each
// attach the bytecode on their own first use of a script — one instance
// having already sent it carries no information for another.
func TestResolveScripts_DifferentAdmissionInstancesEachSendBytecodeOnce(t *testing.T) {
	t.Parallel()

	const script = `send [USD 10] (
	source = @world
	destination = @dst
)`

	store := createTestStore(t)

	firstAdmission, _ := createTestAdmission(t, store)
	first := scriptOrder(testLedgerName, script)
	require.NoError(t, runResolveProvenance(t, firstAdmission, []*raftcmdpb.Order{first}, false))
	require.NotEmpty(t, first.GetTechnical().GetCompiledProgram())
	require.Empty(t, first.GetTechnical().GetCompiledProgramHash())

	secondAdmission, _ := createTestAdmission(t, store)
	second := scriptOrder(testLedgerName, script)
	require.NoError(t, runResolveProvenance(t, secondAdmission, []*raftcmdpb.Order{second}, false))
	require.NotEmpty(t, second.GetTechnical().GetCompiledProgram(),
		"a new admission instance's first use of a script must still carry the bytecode")
	require.Empty(t, second.GetTechnical().GetCompiledProgramHash())
}
