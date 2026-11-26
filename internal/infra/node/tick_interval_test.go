package node

import (
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestNodeConfigTickIntervals(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name             string
		tick, processing time.Duration
		wantErr          string
	}{
		{name: "zero defaults"},
		{name: "one nanosecond", tick: time.Nanosecond, wantErr: "--raft-tick-interval must be at least 10ns when --raft-processing-tick-interval is zero (got 1ns)"},
		{name: "nine nanoseconds", tick: 9 * time.Nanosecond, wantErr: "--raft-tick-interval must be at least 10ns when --raft-processing-tick-interval is zero (got 9ns)"},
		{name: "ten nanoseconds", tick: 10 * time.Nanosecond},
		{name: "one with override", tick: time.Nanosecond, processing: time.Nanosecond},
		{name: "nine with override", tick: 9 * time.Nanosecond, processing: time.Nanosecond},
		{name: "zero with override", processing: time.Nanosecond},
		{name: "large durations", tick: math.MaxInt64, processing: math.MaxInt64},
		{name: "large derived duration", tick: math.MaxInt64},
		{name: "negative tick", tick: -time.Nanosecond, wantErr: "--raft-tick-interval must be ≥ 0 (got -1ns)"},
		{name: "negative processing", processing: -time.Nanosecond, wantErr: "--raft-processing-tick-interval must be ≥ 0 (got -1ns)"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := NodeConfig{NodeID: 1, TickInterval: tt.tick, ProcessingTickInterval: tt.processing}
			err := cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}
			require.EqualError(t, err, tt.wantErr)
			// No WAL or other dependency exists: constructor validation must precede
			// its first storage read, even when the caller bypasses Validate.
			n, err := NewNode(cfg, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			require.Nil(t, n)
			require.EqualError(t, err, tt.wantErr)
		})
	}
}

func TestOrchestrateProcessingIntervalAfterReady(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name                        string
		tick, processing, effective time.Duration
	}{
		{name: "zero defaults", effective: 10 * time.Millisecond},
		{name: "derived default", tick: time.Second, effective: 100 * time.Millisecond},
		{name: "shorter override", tick: time.Second, processing: 20 * time.Millisecond, effective: 20 * time.Millisecond},
		{name: "longer override", tick: time.Second, processing: 200 * time.Millisecond, effective: 200 * time.Millisecond},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				setup := newTestApplierSetup(t)
				raw, err := raft.NewRawNode(&raft.Config{ID: 1, Storage: raft.NewMemoryStorage(), ElectionTick: 10, HeartbeatTick: 1, MaxInflightMsgs: 256, MaxSizePerMsg: 1024, AsyncStorageWrites: true, Logger: NewLoggerAdapter(logging.Testing())})
				require.NoError(t, err)
				heartbeat := &raftpb.Message{Type: new(raftpb.MsgHeartbeat), From: proto.Uint64(2), To: proto.Uint64(1)}
				require.NoError(t, raw.Step(heartbeat))
				require.True(t, raw.HasReady())
				n := &Node{config: NodeConfig{TickInterval: tt.tick, ProcessingTickInterval: tt.processing}, rawNode: raw, transport: newForceRemoveTransport(t), applier: setup.applier, logger: logging.Testing(), readies: make(chan raft.Ready), clusterCommandCh: make(chan *clusterCommand)}
				stop := make(chan struct{})
				result := make(chan error, 1)
				go func() { result <- n.orchestrate(t.Context(), stop) }()
				t.Cleanup(func() {
					close(stop)
					select {
					case err := <-result:
						require.NoError(t, err)
					case <-time.After(time.Second):
						t.Error("orchestrate did not stop")
					}
				})
				started := time.Now()
				var rd raft.Ready
				select {
				case rd = <-n.readies:
				case <-time.After(time.Second):
					t.Fatal("initial Ready was not produced")
				}
				require.Equal(t, tt.effective, time.Since(started), "initial processing cadence")
				require.Empty(t, rd.Entries)
				require.Empty(t, rd.CommittedEntries)
				require.NotEmpty(t, rd.Messages, "real heartbeat response Ready")
				// This message-only Ready has no storage/apply acknowledgements. Finish
				// its real completion path, then wait until the idle timer is rearmed.
				n.readyTerminated <- readyResult{rd: rd}
				synctest.Wait()
				require.Nil(t, n.readyTerminated, "Ready must drain before testing reset")
				// A command runs on the owning goroutine without the transport fast path.
				// The pending response must therefore be detected by the rearmed ticker.
				cmd := &clusterCommand{fn: func() error { return raw.Step(heartbeat) }, errCh: make(chan error, 1)}
				n.clusterCommandCh <- cmd
				require.NoError(t, <-cmd.errCh)
				started = time.Now()
				select {
				case <-n.readies:
				case <-time.After(time.Second):
					t.Fatal("rearmed ticker did not produce Ready")
				}
				require.Equal(t, tt.effective, time.Since(started), "processing cadence after Ready completion")
			})
		})
	}
}
