package bootstrap

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

// TestShouldRunJoinPreflight covers the skip logic that decides whether the
// EN-1436 join preflight runs for a given boot.
func TestShouldRunJoinPreflight(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()

	t.Run("bootstrap node never joins", func(t *testing.T) {
		t.Parallel()

		cfg := Config{RaftConfig: node.NodeConfig{
			Bootstrap: true,
			Peers:     []node.Peer{{ID: 1, Address: "leader:7777"}},
			WalDir:    t.TempDir(),
		}}
		assert.False(t, shouldRunJoinPreflight(cfg, logger))
	})

	t.Run("no peers means no join", func(t *testing.T) {
		t.Parallel()

		cfg := Config{RaftConfig: node.NodeConfig{WalDir: t.TempDir()}}
		assert.False(t, shouldRunJoinPreflight(cfg, logger))
	})

	t.Run("already joined skips", func(t *testing.T) {
		t.Parallel()

		walDir := t.TempDir()
		require.NoError(t, wal.MarkClusterJoined(walDir))

		cfg := Config{RaftConfig: node.NodeConfig{
			Peers:  []node.Peer{{ID: 1, Address: "leader:7777"}},
			WalDir: walDir,
		}}
		assert.False(t, shouldRunJoinPreflight(cfg, logger))
	})

	t.Run("fresh joining node runs preflight", func(t *testing.T) {
		t.Parallel()

		cfg := Config{RaftConfig: node.NodeConfig{
			Peers:  []node.Peer{{ID: 1, Address: "leader:7777"}},
			WalDir: t.TempDir(),
		}}
		assert.True(t, shouldRunJoinPreflight(cfg, logger))
	})
}

// TestJoinPreflightRunsBeforeRaftTraffic proves the EN-1436 ordering invariant
// (flemzord review on #1478): the join preflight OnStart hook must run — and,
// when it fails, abort startup — BEFORE any hook that lets inbound Raft traffic
// reach rawNode.Step.
//
// It mirrors the module's registration order: the join preflight hook is
// appended first, followed by a sentinel "inbound Raft traffic" hook standing
// in for the Raft gRPC server start / membership.Start() / node.Run() hooks.
// fx runs OnStart hooks in Append order and halts the sequence on the first
// error, so if the preflight fails, the sentinel hook must never start. We make
// the preflight fail deterministically (unreachable peer + a start context that
// is cancelled almost immediately, driving tryAddLearner's backoff loop to
// return ctx.Err()) and assert the sentinel never ran.
func TestJoinPreflightRunsBeforeRaftTraffic(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()

	cfg := Config{RaftConfig: node.NodeConfig{
		NodeID: 2,
		// Unreachable peer: the outbound dial fails, tryAddLearner enters its
		// backoff select, and the cancelled context makes it return ctx.Err().
		Peers:         []node.Peer{{ID: 1, Address: "127.0.0.1:1"}},
		AdvertiseAddr: "127.0.0.1:7777",
		WalDir:        filepath.Join(t.TempDir(), "wal"),
	}}

	require.True(t, shouldRunJoinPreflight(cfg, logger),
		"test setup: this cfg must exercise the preflight path")

	var (
		preflightStarted    atomic.Bool
		raftTrafficStarted  atomic.Bool
		preflightBeforeRaft atomic.Bool
	)

	// startCtx is cancelled shortly after Start begins so the preflight's
	// backoff loop returns promptly instead of retrying the unreachable peer
	// forever. This models "the preflight did not pass" without any network.
	startCtx, cancel := context.WithCancel(context.Background())

	app := fx.New(
		fx.NopLogger,
		fx.Supply(cfg),
		fx.Provide(func() logging.Logger { return logger }),
		// Registration order MUST match module.go: preflight first, inbound
		// Raft traffic second.
		fx.Invoke(func(lc fx.Lifecycle, cfg Config, logger logging.Logger) {
			if !shouldRunJoinPreflight(cfg, logger) {
				return
			}

			hook := joinPreflightHook(cfg, logger)
			inner := hook.OnStart
			hook.OnStart = func(ctx context.Context) error {
				preflightStarted.Store(true)
				// Record that the raft-traffic hook had not started yet at the
				// moment the preflight began.
				preflightBeforeRaft.Store(!raftTrafficStarted.Load())

				return inner(ctx)
			}
			lc.Append(hook)
		}),
		fx.Invoke(func(lc fx.Lifecycle) {
			lc.Append(fx.Hook{
				OnStart: func(_ context.Context) error {
					// Stand-in for the Raft gRPC server start / membership.Start()
					// / node.Run() hooks that feed rawNode.Step.
					raftTrafficStarted.Store(true)

					return nil
				},
			})
		}),
	)

	require.NoError(t, app.Err())

	// Cancel the start context once the preflight is under way so the backoff
	// loop unwinds and Start returns the failure. Poll from a plain goroutine
	// (no testing assertions off the test goroutine); a hard deadline cancel
	// guards against the preflight never starting so the test can't hang.
	go func() {
		deadline := time.After(5 * time.Second)
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-deadline:
				cancel()

				return
			case <-tick.C:
				if preflightStarted.Load() {
					cancel()

					return
				}
			}
		}
	}()

	startErr := app.Start(startCtx)
	t.Cleanup(func() { cancel() })

	// The failing preflight must abort Start.
	require.Error(t, startErr, "a failed preflight must abort fx startup")
	assert.True(t, preflightStarted.Load(), "preflight OnStart must have run")
	assert.True(t, preflightBeforeRaft.Load(),
		"preflight must start before the inbound-Raft-traffic hook")
	assert.False(t, raftTrafficStarted.Load(),
		"inbound Raft traffic must NOT start when the preflight fails")

	// App never fully started; Stop should be a no-op error path we can ignore.
	_ = app.Stop(context.Background())
}
