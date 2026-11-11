package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/health"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
)

type barrierTestProposer struct{}

func (barrierTestProposer) InitialIndex() uint64 { return 1 }

func (barrierTestProposer) Propose(_ context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
	proposal.Resolve(nil, nil)
	future := futures.New[state.ApplyResult]()
	future.Resolve(state.ApplyResult{AppliedIndex: 1}, nil)

	return future, nil
}

type allowWrites struct{}

func (allowWrites) CheckWritesAllowed() error { return nil }

var _ health.WriteGate = allowWrites{}

func TestBarrierStampsSemanticCapabilities(t *testing.T) {
	a, _ := createTestAdmission(t, createTestStore(t))
	a.writeGate = allowWrites{}
	a.proposer = barrierTestProposer{}

	_, err := a.Barrier(context.Background())
	require.NoError(t, err)
}
