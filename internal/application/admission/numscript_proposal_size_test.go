package admission

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestResolveScripts_TextOnlyOrderSize pins the proposal cost of a repeated
// inline script. Admission still validates the script and plans its reads,
// while neither the first nor later order carries VM bytecode or bound vars.
func TestResolveScripts_TextOnlyOrderSize(t *testing.T) {
	t.Parallel()
	const script = `send [USD 10] (
  source = @world
  destination = @dst
 )`
	admission, _ := createTestAdmission(t, createTestStore(t))
	first := scriptOrder(testLedgerName, script)
	require.NoError(t, runResolveProvenance(t, admission, []*raftcmdpb.Order{first}, false))
	second := scriptOrder(testLedgerName, script)
	require.NoError(t, runResolveProvenance(t, admission, []*raftcmdpb.Order{second}, false))
	require.Equal(t, first.SizeVT(), second.SizeVT())
	require.LessOrEqual(t, first.SizeVT(), 96, "technical VM fields must not inflate the Raft order")
}
