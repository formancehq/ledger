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
// the FSM half (decode + verify + execute, hash guard, interpreter fallback)
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

	hash := numscript.HashScript(script)
	require.Equal(t, hash[:], technical.GetCompiledScriptHash(),
		"the artifact must be bound to the exact compiled text")

	program, err := numscriptlib.DecodeCompiledProgram(technical.GetCompiledProgram())
	require.NoError(t, err)

	vars, err := numscriptlib.DecodeVars(technical.GetCompiledVars())
	require.NoError(t, err)

	require.NoError(t, numscriptlib.VerifyCompiledProgramWithVars(program, &vars),
		"the bound artifact must pass the same verification the FSM runs")
}
