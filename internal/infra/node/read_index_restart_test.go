package node

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/pkg/futures"
)

// TestReadIndexRestartRejectsOldCertificate leaves a read outstanding in the
// unchanged leader, reconstructs its requesting follower from the same durable
// Raft storage, and only then commits a distinguishable write on the other two
// voters. The old quorum acknowledgement is delivered, not dropped by the test.
func TestReadIndexRestartRejectsOldCertificate(t *testing.T) {
	t.Parallel()
	h := newReadIndexRestartHarness(t)
	require.NoError(t, h.nodes[1].Campaign())
	h.drain(nil)
	h.nodes[1].Tick()
	h.drain(nil)
	require.Equal(t, raft.StateLeader, h.nodes[1].Status().RaftState)
	term := h.nodes[1].Status().GetTerm()
	oldIndex := h.nodes[1].Status().GetCommit()
	require.Equal(t, oldIndex, h.applied[2])

	// Each process incarnation uses the production request-context generator.
	oldContext := makeReadIndexContext()
	setup := newTestApplierSetup(t)
	for i := uint64(1); i <= oldIndex; i++ {
		_, err := setup.fsm.ApplyEntries(t.Context(), setup.store, &raftpb.Entry{Index: new(i), Term: new(term), Type: new(raftpb.EntryNormal)})
		require.NoError(t, err)
	}
	require.Equal(t, oldIndex, setup.fsm.LastPersistedIndex())
	h.nodes[2].ReadIndex(oldContext)
	var oldAcknowledgement *raftpb.Message
	h.drain(func(m *raftpb.Message) bool {
		if m.GetType() == raftpb.MsgHeartbeatResp && len(m.GetContext()) != 0 {
			if m.GetFrom() == 2 {
				oldAcknowledgement = m
			}

			return false
		}

		return true
	})
	require.NotNil(t, oldAcknowledgement, "old read must await its quorum confirmation")
	require.Empty(t, h.readStates[2])

	// Reuse exactly the same MemoryStorage, including its term and commit index.
	h.restart(2)
	// Re-establish the unchanged leader through an actual heartbeat, without
	// acknowledging the old read. The new caller therefore has a known leader.
	h.nodes[1].Tick()
	h.drain(func(m *raftpb.Message) bool {
		return m.GetType() != raftpb.MsgHeartbeatResp || len(m.GetContext()) == 0
	})
	require.Equal(t, uint64(1), h.nodes[2].Status().Lead)
	require.Equal(t, term, h.nodes[2].Status().GetTerm())
	require.Equal(t, oldIndex, h.applied[2])
	require.NoError(t, h.nodes[1].Propose([]byte("write D completed before new read")))
	deliverWrite := func(m *raftpb.Message) bool {
		return m.GetFrom() != 2 && m.GetTo() != 2 && (m.GetType() != raftpb.MsgHeartbeatResp || len(m.GetContext()) == 0)
	}
	h.drain(deliverWrite)
	h.nodes[1].Tick()
	h.drain(deliverWrite)
	newIndex := h.nodes[1].Status().GetCommit()
	require.Greater(t, newIndex, oldIndex)
	require.Equal(t, newIndex, h.applied[3], "write D completed through voters 1 and 3")
	require.Equal(t, []byte("write D completed before new read"), h.values[3])
	require.Equal(t, oldIndex, h.applied[2], "restarted follower must still lack D")
	require.Equal(t, term, h.nodes[1].Status().GetTerm())
	require.Equal(t, uint64(1), h.nodes[1].Status().Lead)

	// The production resolver owns the new incarnation's waiter. Its orchestrator
	// command can still be queued when the old certificate arrives.
	newContext := makeReadIndexContext()
	waiter := futures.New[uint64]()
	n := &Node{pendingReads: &SyncMap[string, *readIndexRequest]{}}
	n.pendingReads.Store(string(newContext), &readIndexRequest{future: waiter})
	require.NoError(t, h.nodes[1].Step(oldAcknowledgement))
	h.drain(func(m *raftpb.Message) bool { return m.GetType() == raftpb.MsgReadIndexResp })
	require.Len(t, h.readStates[2], 1, "Raft must accept the delayed response after restart")
	oldState := h.readStates[2][0]
	require.Equal(t, oldIndex, oldState.Index)
	require.Equal(t, oldContext, oldState.RequestCtx)
	require.Equal(t, term, h.nodes[2].Status().GetTerm())
	n.resolveReadyReadStates(raft.Ready{ReadStates: []raft.ReadState{oldState}})
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	staleIndex, err := waiter.Wait(cancelled)
	require.ErrorIs(t, err, context.Canceled,
		"old certificate resolved new waiter at %d while follower lacks committed write D at %d", staleIndex, newIndex)

	// The fresh request must acquire a horizon covering the completed write.
	h.readStates[2] = nil
	h.nodes[1].Tick()
	h.drain(nil)
	h.nodes[2].ReadIndex(newContext)
	h.drain(nil)
	require.Len(t, h.readStates[2], 1)
	freshState := h.readStates[2][0]
	require.GreaterOrEqual(t, freshState.Index, newIndex)
	n.resolveReadyReadStates(raft.Ready{ReadStates: []raft.ReadState{freshState}})
	got, err := waiter.Wait(cancelled)
	require.NoError(t, err)
	require.GreaterOrEqual(t, got, newIndex)
	// Raft may deliver a ReadState before the durable application pipeline
	// catches up. Exercise the same real Machine barrier used by
	// ReadIndexAndWait, with the follower's durable FSM still at C.
	require.NoError(t, setup.fsm.WaitForApplied(cancelled, oldIndex))
	require.ErrorIs(t, setup.fsm.WaitForApplied(cancelled, got), context.Canceled,
		"fresh read must not serve while the durable FSM lacks write D")
	for i := oldIndex + 1; i <= got; i++ {
		_, err := setup.fsm.ApplyEntries(t.Context(), setup.store, &raftpb.Entry{Index: new(i), Term: new(term), Type: new(raftpb.EntryNormal)})
		require.NoError(t, err)
	}
	require.NoError(t, setup.fsm.WaitForApplied(t.Context(), got))
	require.Equal(t, got, setup.fsm.LastPersistedIndex())
	require.GreaterOrEqual(t, h.applied[2], got)
	require.Equal(t, []byte("write D completed before new read"), h.values[2])
}

type readIndexRestartHarness struct {
	t          *testing.T
	nodes      map[uint64]*raft.RawNode
	storage    map[uint64]*raft.MemoryStorage
	applied    map[uint64]uint64
	values     map[uint64][]byte
	readStates map[uint64][]raft.ReadState
}

func newReadIndexRestartHarness(t *testing.T) *readIndexRestartHarness {
	t.Helper()
	h := &readIndexRestartHarness{
		t:          t,
		nodes:      map[uint64]*raft.RawNode{},
		storage:    map[uint64]*raft.MemoryStorage{},
		applied:    map[uint64]uint64{},
		values:     map[uint64][]byte{},
		readStates: map[uint64][]raft.ReadState{},
	}
	for id := uint64(1); id <= 3; id++ {
		s := raft.NewMemoryStorage()
		require.NoError(t, s.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: new(uint64(1)), Term: new(uint64(1)), ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}}}}))
		require.NoError(t, s.SetHardState(&raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))}))
		h.storage[id] = s
		h.applied[id] = 1
		h.restart(id)
	}

	return h
}

func (h *readIndexRestartHarness) restart(id uint64) {
	h.t.Helper()
	n, err := raft.NewRawNode(&raft.Config{ID: id, Storage: h.storage[id], Applied: h.applied[id], ElectionTick: 10, HeartbeatTick: 1, MaxInflightMsgs: 256, MaxSizePerMsg: 1024 * 1024, Logger: NewLoggerAdapter(logging.Testing())})
	require.NoError(h.t, err)
	h.nodes[id] = n
}

// drain models synchronous persistence and application before Advance. Its
// explicit delivery predicate controls only the messages named by each phase;
// there are no wall clocks, elections after bootstrap, sleeps, or scheduler races.
func (h *readIndexRestartHarness) drain(deliver func(*raftpb.Message) bool) {
	h.t.Helper()
	for range 100 {
		var messages []*raftpb.Message
		ready := false
		for id := uint64(1); id <= 3; id++ {
			n := h.nodes[id]
			if !n.HasReady() {
				continue
			}
			ready = true
			rd := n.Ready()
			require.NoError(h.t, h.storage[id].Append(rd.Entries))
			if !raft.IsEmptyHardState(rd.HardState) {
				require.NoError(h.t, h.storage[id].SetHardState(rd.HardState))
			}
			for _, e := range rd.CommittedEntries {
				h.applied[id] = e.GetIndex()
				if len(e.GetData()) != 0 {
					h.values[id] = bytes.Clone(e.GetData())
				}
			}
			h.readStates[id] = append(h.readStates[id], rd.ReadStates...)
			messages = append(messages, rd.Messages...)
			n.Advance(rd)
		}
		for _, m := range messages {
			if deliver == nil || deliver(m) {
				require.NoError(h.t, h.nodes[m.GetTo()].Step(m))
			}
		}
		if !ready {
			return
		}
	}
	h.t.Fatal("Raft message drain exceeded bounded deterministic steps")
}

func TestReadIndexRaftEchoesOpaqueContexts(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name           string
		requestContext []byte
	}{
		{name: "legacy counter", requestContext: []byte{0, 0, 0, 0, 0, 0, 0, 1}},
		{name: "UUID", requestContext: []byte{0x34, 0x0e, 0x12, 0x94, 0x8d, 0x96, 0x49, 0x3f, 0x83, 0x21, 0x42, 0xc2, 0x93, 0x1a, 0x45, 0x2b}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newReadIndexRestartHarness(t)
			require.NoError(t, h.nodes[1].Campaign())
			h.drain(nil)
			h.nodes[1].Tick()
			h.drain(nil)
			h.nodes[2].ReadIndex(tt.requestContext)
			h.drain(nil)
			require.Len(t, h.readStates[2], 1)
			require.Equal(t, tt.requestContext, h.readStates[2][0].RequestCtx,
				"pinned Raft must echo the complete context unchanged across peers")
			require.Equal(t, h.nodes[1].Status().GetCommit(), h.readStates[2][0].Index)
		})
	}
}
