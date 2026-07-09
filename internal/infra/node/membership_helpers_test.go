package node

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/membership"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// noopMemTransport / noopMemPool satisfy membership.Transport /
// membership.Pool with no side effect. Used by Node-level tests that
// construct a Membership without exercising the wiring path.
type noopMemTransport struct{}

func (noopMemTransport) AddPeer(uint64, string)             {}
func (noopMemTransport) RemovePeer(context.Context, uint64) {}

type noopMemPool struct{}

func (noopMemPool) AddPeer(uint64, string) error { return nil }
func (noopMemPool) RemovePeer(uint64) error      { return nil }

// newTestMembership returns a Membership backed by a fresh in-memory
// Pebble store and noop transport/pool, in the post-Start state. Used
// by Node-level tests that need a *membership.Membership to plug into
// a Node struct (recovery / maintenance / confstate suites).
func newTestMembership(t *testing.T) *membership.Membership {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	m, err := membership.NewMembership(
		membership.NewPeerStore(store),
		noopMemTransport{}, noopMemPool{},
		42, "self:7777", "self:8888",
		nil,
		logging.Testing(),
	)
	require.NoError(t, err)
	m.Start()

	return m
}
