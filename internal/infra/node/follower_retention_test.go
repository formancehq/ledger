package node

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestNewFollowerRetentionZeroMaxBytesDisablesTheHold(t *testing.T) {
	t.Parallel()

	require.Nil(t, newFollowerRetention(0, time.Minute))
}

func publishedNeeds(r *followerRetention) []uint64 {
	needs := r.needs.Load()
	if needs == nil {
		return nil
	}

	return *needs
}

func TestFollowerNeed(t *testing.T) {
	t.Parallel()

	require.Equal(t, uint64(40), followerNeed(tracker.Progress{State: tracker.StateReplicate, Match: 40, Next: 60}))
	require.Equal(t, uint64(59), followerNeed(tracker.Progress{State: tracker.StateProbe, Match: 3, Next: 60}),
		"after a snapshot is reported sent, raft probes from the snapshot index while Match is stale")
	require.Equal(t, uint64(3), followerNeed(tracker.Progress{State: tracker.StateProbe, Match: 3, Next: 1}))
	require.Equal(t, uint64(70), followerNeed(tracker.Progress{State: tracker.StateSnapshot, Match: 3, Next: 4, PendingSnapshot: 70}))
}

func TestFollowerRetentionObserve(t *testing.T) {
	t.Parallel()

	t0 := time.Unix(1_000, 0)

	t.Run("publishes every advancing follower's position in ascending order", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		r.observe(t0, true, []followerProgress{{id: 2, need: 50}, {id: 3, need: 30}, {id: 4, need: 0}})
		require.Equal(t, []uint64{0, 30, 50}, publishedNeeds(r))
	})

	t.Run("a follower whose progress stalls past the idle timeout stops holding", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		r.observe(t0, true, []followerProgress{{id: 2, need: 30}})
		r.observe(t0.Add(time.Minute), true, []followerProgress{{id: 2, need: 30}})
		require.Equal(t, []uint64{30}, publishedNeeds(r), "a stall of exactly the idle timeout still holds")

		r.observe(t0.Add(time.Minute+time.Nanosecond), true, []followerProgress{{id: 2, need: 30}})
		require.Nil(t, publishedNeeds(r))

		// Progress resumes: the follower holds again from its new position.
		r.observe(t0.Add(2*time.Minute), true, []followerProgress{{id: 2, need: 31}})
		require.Equal(t, []uint64{31}, publishedNeeds(r))
	})

	t.Run("a state change at the same position is activity", func(t *testing.T) {
		t.Parallel()

		// A new leader probes an offline follower from its log tail 30. The
		// follower stays away past the idle timeout, then comes back and is
		// sent a snapshot at the same index 30.
		r := newFollowerRetention(1<<20, time.Minute)
		r.observe(t0, true, []followerProgress{{id: 2, state: tracker.StateProbe, need: 30}})
		r.observe(t0.Add(2*time.Minute), true, []followerProgress{{id: 2, state: tracker.StateProbe, need: 30}})
		require.Nil(t, publishedNeeds(r))

		r.observe(t0.Add(3*time.Minute), true, []followerProgress{{id: 2, state: tracker.StateSnapshot, need: 30}})
		require.Equal(t, []uint64{30}, publishedNeeds(r))
	})

	t.Run("a follower that keeps advancing keeps holding past the idle timeout", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		for i := range uint64(10) {
			r.observe(t0.Add(time.Duration(i)*50*time.Second), true, []followerProgress{{id: 2, need: 30 + i}})
		}
		require.Equal(t, []uint64{39}, publishedNeeds(r))
	})

	t.Run("losing leadership clears the hold and the history", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		r.observe(t0, true, []followerProgress{{id: 2, need: 30}})
		r.observe(t0, false, nil)
		require.Nil(t, publishedNeeds(r))
		require.Empty(t, r.positions)
	})

	t.Run("a removed follower is forgotten", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		r.observe(t0, true, []followerProgress{{id: 2, need: 30}, {id: 3, need: 40}})
		r.observe(t0, true, []followerProgress{{id: 3, need: 40}})
		require.Equal(t, []uint64{40}, publishedNeeds(r))
		require.NotContains(t, r.positions, uint64(2))
	})
}

// fakeRetentionLog serves entries [first, first+len(entries)).
type fakeRetentionLog struct {
	entries []*raftpb.Entry
	err     error
}

func newFakeRetentionLog(first, last uint64, dataSize int) *fakeRetentionLog {
	l := &fakeRetentionLog{}
	for i := first; i <= last; i++ {
		l.entries = append(l.entries, &raftpb.Entry{
			Index: proto.Uint64(i),
			Term:  proto.Uint64(1),
			Data:  make([]byte, dataSize),
		})
	}

	return l
}

func (l *fakeRetentionLog) FirstIndex() (uint64, error) {
	if l.err != nil {
		return 0, l.err
	}

	return l.entries[0].GetIndex(), nil
}

func (l *fakeRetentionLog) Entries(lo, hi, _ uint64) ([]*raftpb.Entry, error) {
	first := l.entries[0].GetIndex()
	if lo < first {
		return nil, raft.ErrCompacted
	}

	return l.entries[lo-first : hi-first], nil
}

func TestFollowerRetentionCompactionTarget(t *testing.T) {
	t.Parallel()

	const dataSize = 100
	entrySize := uint64(proto.Size(&raftpb.Entry{
		Index: proto.Uint64(10), Term: proto.Uint64(1), Data: make([]byte, dataSize),
	}))

	retention := func(maxBytes uint64, needs ...uint64) *followerRetention {
		r := newFollowerRetention(maxBytes, time.Minute)
		if len(needs) > 0 {
			r.needs.Store(&needs)
		}

		return r
	}
	// The fake WAL retains entries 10..100: the compacted index is 9.
	walLog := func() *fakeRetentionLog { return newFakeRetentionLog(10, 100, dataSize) }

	t.Run("no follower behind the target keeps the target", func(t *testing.T) {
		t.Parallel()

		got, decision, err := retention(math.MaxUint64).compactionTarget(80, walLog())
		require.NoError(t, err)
		require.Equal(t, uint64(80), got)
		require.Equal(t, retentionDecision{}, decision)

		got, decision, err = retention(math.MaxUint64, 80, 90).compactionTarget(80, walLog())
		require.NoError(t, err)
		require.Equal(t, uint64(80), got)
		require.Equal(t, retentionDecision{}, decision)
	})

	t.Run("a follower behind the target holds compaction at its position", func(t *testing.T) {
		t.Parallel()

		// Compacting at 40 keeps entries 40..79 that compacting at 80 releases.
		got, decision, err := retention(math.MaxUint64, 40, 90).compactionTarget(80, walLog())
		require.NoError(t, err)
		require.Equal(t, uint64(40), got)
		require.Equal(t, retentionDecision{held: true, need: 40, retainedBytes: 40 * entrySize}, decision)
	})

	t.Run("a follower already compacted away does not cancel the hold for others", func(t *testing.T) {
		t.Parallel()

		// A new learner at 0 needs a snapshot whatever the leader keeps.
		got, decision, err := retention(math.MaxUint64, 0, 5, 40).compactionTarget(80, walLog())
		require.NoError(t, err)
		require.Equal(t, uint64(40), got)
		require.Equal(t, retentionDecision{held: true, need: 40, retainedBytes: 40 * entrySize}, decision)
	})

	t.Run("only followers already compacted away keep the target", func(t *testing.T) {
		t.Parallel()

		got, decision, err := retention(math.MaxUint64, 0, 5).compactionTarget(80, walLog())
		require.NoError(t, err)
		require.Equal(t, uint64(80), got)
		require.Equal(t, retentionDecision{}, decision)
	})

	t.Run("a follower exactly at the compacted index holds without charging the boundary entry", func(t *testing.T) {
		t.Parallel()

		// Entry 9 is the WAL's boundary: raft.Storage cannot read it, so only
		// entries 10..79 are charged.
		got, decision, err := retention(math.MaxUint64, 9).compactionTarget(80, walLog())
		require.NoError(t, err)
		require.Equal(t, uint64(9), got)
		require.Equal(t, retentionDecision{held: true, need: 9, retainedBytes: 70 * entrySize}, decision)
	})

	t.Run("the byte cap keeps only the newest entries", func(t *testing.T) {
		t.Parallel()

		// Room for 15 entries and a half: entries 65..79 stay, so compaction
		// lands on 65 and keeps it as the matching entry.
		got, decision, err := retention(15*entrySize+entrySize/2, 40).compactionTarget(80, walLog())
		require.NoError(t, err)
		require.Equal(t, uint64(65), got)
		require.Equal(t, retentionDecision{held: true, capped: true, need: 40, retainedBytes: 15 * entrySize}, decision)
	})

	t.Run("a cap smaller than one entry keeps the target", func(t *testing.T) {
		t.Parallel()

		got, decision, err := retention(1, 40).compactionTarget(80, walLog())
		require.NoError(t, err)
		require.Equal(t, uint64(80), got)
		require.Equal(t, retentionDecision{capped: true, need: 40}, decision)
	})

	t.Run("a WAL read failure keeps the target", func(t *testing.T) {
		t.Parallel()

		wantErr := errors.New("boom")
		got, _, err := retention(math.MaxUint64, 40).compactionTarget(80, &fakeRetentionLog{err: wantErr})
		require.ErrorIs(t, err, wantErr)
		require.Equal(t, uint64(80), got)
	})
}

// testRetentionLeader is a single-voter RawNode with one learner peer, on a
// MemoryStorage whose log starts after a snapshot at snapshotIndex.
type testRetentionLeader struct {
	t       *testing.T
	rn      *raft.RawNode
	storage *raft.MemoryStorage
	conf    *raftpb.ConfState
}

const (
	testRetentionLeaderID = uint64(1)
	testRetentionPeerID   = uint64(2)
)

func newTestRetentionLeader(t *testing.T, snapshotIndex uint64) *testRetentionLeader {
	t.Helper()

	conf := &raftpb.ConfState{Voters: []uint64{testRetentionLeaderID}, Learners: []uint64{testRetentionPeerID}}
	storage := raft.NewMemoryStorage()
	require.NoError(t, storage.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
		Index: proto.Uint64(snapshotIndex), Term: proto.Uint64(1), ConfState: conf,
	}}))
	rn, err := raft.NewRawNode(&raft.Config{
		ID: testRetentionLeaderID, ElectionTick: 10, HeartbeatTick: 1, Storage: storage,
		MaxInflightMsgs: 256, MaxSizePerMsg: 1024 * 1024,
		Logger: NewLoggerAdapter(logging.Testing()),
	})
	require.NoError(t, err)

	l := &testRetentionLeader{t: t, rn: rn, storage: storage, conf: conf}
	require.NoError(t, rn.Campaign())
	l.drain()
	require.Equal(t, raft.StateLeader, rn.Status().RaftState)

	return l
}

// drain persists and advances every pending Ready, returning the messages
// addressed to the peer.
func (l *testRetentionLeader) drain() []*raftpb.Message {
	l.t.Helper()
	var toPeer []*raftpb.Message
	for l.rn.HasReady() {
		rd := l.rn.Ready()
		if !raft.IsEmptyHardState(rd.HardState) {
			require.NoError(l.t, l.storage.SetHardState(rd.HardState))
		}
		require.NoError(l.t, l.storage.Append(rd.Entries))
		for _, m := range rd.Messages {
			if m.GetTo() == testRetentionPeerID {
				toPeer = append(toPeer, m)
			}
		}
		l.rn.Advance(rd)
	}

	return toPeer
}

func (l *testRetentionLeader) fromPeer(typ raftpb.MessageType, fields func(*raftpb.Message)) {
	l.t.Helper()
	m := &raftpb.Message{
		Type: typ.Enum(), From: proto.Uint64(testRetentionPeerID), To: proto.Uint64(testRetentionLeaderID),
		Term: new(l.rn.Status().GetTerm()),
	}
	if fields != nil {
		fields(m)
	}
	require.NoError(l.t, l.rn.Step(m))
}

func (l *testRetentionLeader) propose(n int) {
	l.t.Helper()
	for range n {
		require.NoError(l.t, l.rn.Propose([]byte("x")))
	}
	l.drain()
}

// rejectProbe has the peer reject the leader's probe down to hint.
func (l *testRetentionLeader) rejectProbe(hint uint64) []*raftpb.Message {
	l.t.Helper()
	l.fromPeer(raftpb.MsgHeartbeatResp, nil)
	var probe *raftpb.Message
	for _, m := range l.drain() {
		if m.GetType() == raftpb.MsgApp {
			probe = m
		}
	}
	require.NotNil(l.t, probe, "the leader must probe the peer")
	l.fromPeer(raftpb.MsgAppResp, func(m *raftpb.Message) {
		m.Index = new(probe.GetIndex())
		m.Reject = new(true)
		m.RejectHint = proto.Uint64(hint)
		m.LogTerm = proto.Uint64(1)
	})

	return l.drain()
}

// newTestRetentionMaintenanceNode returns a maintenance node whose WAL holds
// six applied entries, with a compaction margin of 1 (normal target 5), the
// hold enabled, and its orchestrate command loop served for the test.
func newTestRetentionMaintenanceNode(t *testing.T) *Node {
	t.Helper()

	node, setup := newTestMaintenanceNode(t)
	setup.applier.compactionMargin = 1
	node.followerRetention = newFollowerRetention(math.MaxUint64, time.Minute)

	entries := make([]*raftpb.Entry, 6)
	for i := range entries {
		entries[i], _ = makeCreateLedgerEntry(t, uint64(i+1), "retention-ledger-"+string(rune('a'+i)))
	}
	require.NoError(t, node.wal.Append(&raftpb.HardState{
		Term:   proto.Uint64(1),
		Vote:   proto.Uint64(1),
		Commit: proto.Uint64(6),
	}, entries))

	runDone := make(chan error, 1)
	go func() { runDone <- setup.applier.Run(context.Background(), setup.stop) }()
	setup.applier.Submit(entries, nil, nil, setup.stop)
	setup.applier.Drain(setup.stop)
	close(setup.stop)
	require.NoError(t, <-runDone)
	node.lastRaftApplied.Store(6)

	// Serve the orchestrate command loop: maintenance refreshes follower
	// positions on it.
	node.clusterCommandCh = make(chan *clusterCommand, 1)
	stop := make(chan struct{})
	done := make(chan struct{})
	t.Cleanup(func() {
		close(stop)
		<-done
	})
	go func() {
		defer close(done)
		for {
			select {
			case cmd := <-node.clusterCommandCh:
				_ = node.executeClusterCommand(cmd) // the closure never fails
			case <-stop:
				return
			}
		}
	}()

	return node
}

// TestDoMaintenanceRetainsLogForCatchingUpFollower reproduces EN-3429 at the
// maintenance boundary: with a follower still replicating behind the
// compaction margin, the leader must keep the entries after the follower's
// position so the follower is not sent a new snapshot. Once the follower
// catches up, the normal margin applies again.
func TestDoMaintenanceRetainsLogForCatchingUpFollower(t *testing.T) {
	t.Parallel()

	node := newTestRetentionMaintenanceNode(t)

	// The leader's log is 2 (its empty entry) .. 6; the follower acknowledged 2.
	leader := newTestRetentionLeader(t, 1)
	leader.propose(4)
	leader.fromPeer(raftpb.MsgAppResp, func(m *raftpb.Message) { m.Index = proto.Uint64(2) })
	leader.drain()
	node.rawNode = leader.rn

	// The margin alone would compact to 5.
	require.NoError(t, node.doMaintenance())
	snapshot, err := node.wal.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(6), snapshot.GetMetadata().GetIndex())
	first, err := node.wal.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(3), first, "entries after the follower's position must stay available")

	// The follower catches up: compaction returns to the margin.
	leader.fromPeer(raftpb.MsgAppResp, func(m *raftpb.Message) { m.Index = proto.Uint64(6) })
	leader.drain()
	require.NoError(t, node.doMaintenance())
	first, err = node.wal.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(6), first)
}

// TestDoMaintenanceSeesSnapshotSentSinceLastTick covers the race between the
// tick-sampled positions and a snapshot RawNode chooses between ticks. The
// last tick saw no follower to hold for; since then the leader sent the
// follower a snapshot at 3. Maintenance must refresh the positions on the
// orchestrate goroutine and keep the entries after 3, not compact to its
// normal target 5 from the stale sample.
func TestDoMaintenanceSeesSnapshotSentSinceLastTick(t *testing.T) {
	t.Parallel()

	node := newTestRetentionMaintenanceNode(t)

	leader := newTestRetentionLeader(t, 3)
	leader.propose(2)
	node.rawNode = leader.rn

	// The follower's log ends at 1, before the leader's: it is sent the
	// snapshot at 3. No tick observes it.
	var sent bool
	for _, m := range leader.rejectProbe(1) {
		sent = sent || m.GetType() == raftpb.MsgSnap
	}
	require.True(t, sent, "the leader must send a snapshot")
	require.Equal(t, tracker.StateSnapshot, leader.rn.Status().Progress[testRetentionPeerID].State)
	require.Nil(t, publishedNeeds(node.followerRetention), "no tick has observed the snapshot yet")

	require.NoError(t, node.doMaintenance())
	first, err := node.wal.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(4), first, "entries after the snapshot index must stay available")
}

// TestObserveFollowerRetentionFollowsSnapshotHandoff drives a real RawNode so
// the hold reads the same Progress fields production reads. It covers the
// EN-3429 handoff: once the leader sends a snapshot, the follower must hold
// at the snapshot index both while the snapshot is in flight and after the
// leader reports it sent, before the follower's acknowledgement advances
// Match. A node that is not the leader holds nothing.
func TestObserveFollowerRetentionFollowsSnapshotHandoff(t *testing.T) {
	t.Parallel()

	follower, err := raft.NewRawNode(&raft.Config{
		ID: testRetentionLeaderID, ElectionTick: 10, HeartbeatTick: 1, Storage: raft.NewMemoryStorage(),
		MaxInflightMsgs: 256, MaxSizePerMsg: 1024 * 1024,
		Logger: NewLoggerAdapter(logging.Testing()),
	})
	require.NoError(t, err)
	node := &Node{
		rawNode:           follower,
		followerRetention: newFollowerRetention(math.MaxUint64, time.Minute),
	}
	now := time.Now()
	node.observeFollowerRetention(now)
	require.Nil(t, publishedNeeds(node.followerRetention), "a node that is not the leader does not hold")

	leader := newTestRetentionLeader(t, 8)
	node.rawNode = leader.rn

	// The single voter commits three more entries on its own (10..12), and
	// the peer acknowledges index 9 (the leader's empty entry).
	leader.propose(3)
	leader.fromPeer(raftpb.MsgAppResp, func(m *raftpb.Message) { m.Index = proto.Uint64(9) })
	leader.drain()
	node.observeFollowerRetention(now)
	require.Equal(t, []uint64{9}, publishedNeeds(node.followerRetention))

	// The leader compacts past the peer's match, then the peer rejects the
	// entries it was sent: the leader can only send it a snapshot at 12.
	_, err = leader.storage.CreateSnapshot(12, leader.conf, nil)
	require.NoError(t, err)
	require.NoError(t, leader.storage.Compact(12))
	leader.fromPeer(raftpb.MsgAppResp, func(m *raftpb.Message) {
		m.Index = proto.Uint64(12)
		m.Reject = new(true)
		m.RejectHint = proto.Uint64(9)
		m.LogTerm = proto.Uint64(1)
	})
	var sent bool
	for _, m := range leader.drain() {
		sent = sent || m.GetType() == raftpb.MsgSnap
	}
	require.True(t, sent, "the leader must send a snapshot")
	pr := leader.rn.Status().Progress[testRetentionPeerID]
	require.Equal(t, tracker.StateSnapshot, pr.State)
	require.Equal(t, uint64(9), pr.Match)
	node.observeFollowerRetention(now)
	require.Equal(t, []uint64{12}, publishedNeeds(node.followerRetention),
		"a follower being sent a snapshot needs the entries after it")

	// The node reports the snapshot sent (finishReady): raft probes from the
	// snapshot index while Match is still the stale 9.
	leader.rn.ReportSnapshot(testRetentionPeerID, raft.SnapshotFinish)
	pr = leader.rn.Status().Progress[testRetentionPeerID]
	require.Equal(t, tracker.StateProbe, pr.State)
	require.Equal(t, uint64(9), pr.Match)
	node.observeFollowerRetention(now)
	require.Equal(t, []uint64{12}, publishedNeeds(node.followerRetention),
		"the hold must cover the snapshot index until the follower acknowledges it")

	// The follower acknowledges the snapshot and then replicates.
	leader.fromPeer(raftpb.MsgAppResp, func(m *raftpb.Message) { m.Index = proto.Uint64(12) })
	leader.drain()
	require.Equal(t, uint64(12), leader.rn.Status().Progress[testRetentionPeerID].Match)
	node.observeFollowerRetention(now)
	require.Equal(t, []uint64{12}, publishedNeeds(node.followerRetention))
}
