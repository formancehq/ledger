package bootstrap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// bootstrapStaleThenSuccessProposer mirrors the events-package fixture:
// the first Propose is rejected at apply time with ErrStaleProposal and
// every later call succeeds. It exercises the bootstrap production
// trigger (proposeTechnical) rather than the shared helper directly.
type bootstrapStaleThenSuccessProposer struct {
	calls int
}

func (p *bootstrapStaleThenSuccessProposer) Propose(_ context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
	p.calls++
	proposal.Resolve(nil, nil)

	f := futures.New[state.ApplyResult]()
	if p.calls == 1 {
		f.Resolve(state.ApplyResult{Error: &domain.BusinessError{Err: domain.ErrStaleProposal}}, nil)
	} else {
		f.Resolve(state.ApplyResult{}, nil)
	}

	return f, nil
}

// TestProposeTechnical_RetriesStaleProposal pins that the bootstrap
// trigger crosses the shared retry implementation: one stale rejection
// is retried internally instead of bubbling to the caller.
func TestProposeTechnical_RetriesStaleProposal(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	store := newTestStore(t)

	c, err := cache.New(1000, noop.Meter{})
	require.NoError(t, err)

	builder := plan.NewBuilder(node.NewIndexTracker(1), c, attributes.New(), store, nil, logger, 0)
	proposer := &bootstrapStaleThenSuccessProposer{}

	cmd := commands.NewCommand()
	cmd.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentClusterConfig)
	cmd.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{
		Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{ClusterConfig: &commonpb.ClusterConfig{}},
	}}
	ops := []plan.WriteOperation{{Target: &cmd.GetTechnicalUpdates()[0].CoverageBits}}

	err = proposeTechnical(context.Background(), builder, proposer, cmd, ops)
	require.NoError(t, err, "ErrStaleProposal on the first attempt must be retried by the shared submitter")
	require.Equal(t, 2, proposer.calls, "exactly one retry was needed")
}
