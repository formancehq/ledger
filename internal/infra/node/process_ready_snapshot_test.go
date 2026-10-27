package node

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

// A follower can receive a snapshot and its next entry before taking Ready.
// Installing that snapshot must preserve the suffix both in raft.Storage and
// after reopening the WAL, before storage acknowledgements release the entry.
func TestProcessReadySnapshotPreservesWALSuffix(t *testing.T) {
	t.Parallel()

	for _, withSuffix := range []bool{false, true} {
		name := "snapshot only"
		if withSuffix {
			name = "snapshot and next entry"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			setup := newTestApplierSetup(t)
			transport := NewMockTransport(gomock.NewController(t))
			transport.EXPECT().Send(gomock.Any())
			n, err := NewNode(NodeConfig{
				NodeID: 1, AdvertiseAddr: "node-1:7000", ServiceAdvertiseAddr: "node-1:8000",
				InstanceID: []byte("0000000000000001"), TickInterval: time.Hour,
				ProcessingTickInterval: time.Hour, MaintenanceInterval: time.Hour,
			}, transport, setup.applier, logging.Testing(), noop.NewMeterProvider().Meter("snapshot-ready"),
				setup.wal, setup.fsm, setup.applier.recovery, setup.applier.synchronizer,
				newTestMembership(t), setup.responseSink)
			require.NoError(t, err)
			rn, err := raft.NewRawNode(&raft.Config{
				ID: 1, ElectionTick: 10, HeartbeatTick: 1, Storage: setup.wal,
				MaxInflightMsgs: 256, MaxSizePerMsg: 1024 * 1024, AsyncStorageWrites: true,
				Logger: NewLoggerAdapter(logging.Testing()),
			})
			require.NoError(t, err)
			conf := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
			snapshot := &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
				Index: proto.Uint64(39), Term: proto.Uint64(2), ConfState: conf,
			}}
			require.NoError(t, rn.Step(&raftpb.Message{
				Type: new(raftpb.MsgSnap), From: proto.Uint64(2), To: proto.Uint64(1), Term: proto.Uint64(2), Snapshot: snapshot,
			}))
			lastIndex := uint64(39)
			entry := &raftpb.Entry{Index: proto.Uint64(40), Term: proto.Uint64(2), Type: new(raftpb.EntryNormal), Data: make([]byte, 4096)}
			if withSuffix {
				lastIndex = 40
				require.NoError(t, rn.Step(&raftpb.Message{
					Type: new(raftpb.MsgApp), From: proto.Uint64(2), To: proto.Uint64(1), Term: proto.Uint64(2),
					Index: proto.Uint64(39), LogTerm: proto.Uint64(2), Commit: proto.Uint64(40), Entries: []*raftpb.Entry{entry},
				}))
			}
			rd := rn.Ready()
			require.True(t, proto.Equal(snapshot, rd.Snapshot))
			require.Empty(t, rd.CommittedEntries, "async storage must release the suffix only after append acknowledgement")
			if withSuffix {
				require.Equal(t, []*raftpb.Entry{entry}, rd.Entries)
			} else {
				require.Empty(t, rd.Entries)
			}
			// Leadership bookkeeping is unrelated to WAL installation. Keep the
			// fixture's local membership and omit SoftState so no remote checkpoint
			// donor is scheduled; the real applier still drains for installation.
			rd.SoftState = nil
			// Exercise the real WAL while enforcing the write budget: ordinary
			// snapshot receipt appends once, and only a nonempty suffix adds
			// one repeated append. No work scales with the retained log size.
			measuredWAL := wal.NewMockWAL(gomock.NewController(t))
			appendCount := 1
			if withSuffix {
				appendCount = 2
			}
			measuredWAL.EXPECT().Append(rd.HardState, rd.Entries).DoAndReturn(setup.wal.Append).Times(appendCount)
			measuredWAL.EXPECT().ApplySnapshot(snapshot).DoAndReturn(setup.wal.ApplySnapshot)
			n.wal = measuredWAL
			runDone := make(chan error, 1)
			go func() { runDone <- setup.applier.Run(context.Background(), setup.stop) }()
			var stopOnce sync.Once
			stopApplier := func() {
				stopOnce.Do(func() {
					close(setup.stop)
					select {
					case runErr := <-runDone:
						require.NoError(t, runErr)
					case <-time.After(5 * time.Second):
						t.Error("applier did not stop")
					}
				})
			}
			t.Cleanup(stopApplier)
			result, err := n.processReady(context.Background(), setup.stop, rd)
			require.NoError(t, err)
			require.True(t, result.snapshotApplied)
			assertStorage := func(w wal.WAL) {
				t.Helper()
				last, storageErr := w.LastIndex()
				require.NoError(t, storageErr)
				require.Equal(t, lastIndex, last)
				term, storageErr := w.Term(lastIndex)
				require.NoError(t, storageErr)
				require.Equal(t, uint64(2), term)
				hs, cs, storageErr := w.InitialState()
				require.NoError(t, storageErr)
				require.True(t, proto.Equal(rd.HardState, hs))
				require.True(t, proto.Equal(conf, cs))
				storedSnapshot, storageErr := w.Snapshot()
				require.NoError(t, storageErr)
				require.True(t, proto.Equal(snapshot, storedSnapshot))
				if withSuffix {
					entries, entriesErr := w.Entries(40, 41, 1024*1024)
					require.NoError(t, entriesErr)
					require.Equal(t, []*raftpb.Entry{entry}, entries)
				}
			}
			assertStorage(setup.wal)
			select {
			case responses := <-n.localResponseCh:
				for _, response := range responses {
					require.NoError(t, rn.Step(response))
				}
			default:
				t.Fatal("snapshot Ready did not acknowledge storage")
			}
			if withSuffix {
				require.True(t, rn.HasReady())
				require.Equal(t, []*raftpb.Entry{entry}, rn.Ready().CommittedEntries,
					"stored suffix must remain available when append acknowledgement releases committed entries")
			}
			stopApplier()
			require.NoError(t, setup.wal.Close())
			reopened, err := wal.New(setup.walDir, logging.Testing(), noop.Meter{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close()) })
			assertStorage(reopened)
		})
	}
}
