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
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestNewFollowerRetentionZeroMaxBytesDisablesTheHold(t *testing.T) {
	t.Parallel()

	require.Nil(t, newFollowerRetention(0, time.Minute))
}

func TestFollowerRetentionObserve(t *testing.T) {
	t.Parallel()

	t0 := time.Unix(1_000, 0)

	t.Run("floor is the lowest match among advancing followers", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		r.observe(t0, true, []followerProgress{{id: 2, match: 50}, {id: 3, match: 30}})
		require.Equal(t, uint64(30), r.floor.Load())
	})

	t.Run("a follower receiving a snapshot does not hold", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		r.observe(t0, true, []followerProgress{{id: 2, match: 50}, {id: 3, match: 30, snapshotting: true}})
		require.Equal(t, uint64(50), r.floor.Load())
		require.NotContains(t, r.matches, uint64(3))
	})

	t.Run("a follower whose match stalls past the idle timeout stops holding", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		r.observe(t0, true, []followerProgress{{id: 2, match: 30}})
		r.observe(t0.Add(time.Minute), true, []followerProgress{{id: 2, match: 30}})
		require.Equal(t, uint64(30), r.floor.Load(), "a stall of exactly the idle timeout still holds")

		r.observe(t0.Add(time.Minute+time.Nanosecond), true, []followerProgress{{id: 2, match: 30}})
		require.Equal(t, uint64(noRetentionFloor), r.floor.Load())

		// Progress resumes: the follower holds again from its new match.
		r.observe(t0.Add(2*time.Minute), true, []followerProgress{{id: 2, match: 31}})
		require.Equal(t, uint64(31), r.floor.Load())
	})

	t.Run("a follower that keeps advancing keeps holding past the idle timeout", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		for i := range uint64(10) {
			r.observe(t0.Add(time.Duration(i)*50*time.Second), true, []followerProgress{{id: 2, match: 30 + i}})
		}
		require.Equal(t, uint64(39), r.floor.Load())
	})

	t.Run("losing leadership clears the hold and the history", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		r.observe(t0, true, []followerProgress{{id: 2, match: 30}})
		r.observe(t0, false, nil)
		require.Equal(t, uint64(noRetentionFloor), r.floor.Load())
		require.Empty(t, r.matches)
	})

	t.Run("a removed follower is forgotten", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		r.observe(t0, true, []followerProgress{{id: 2, match: 30}, {id: 3, match: 40}})
		r.observe(t0, true, []followerProgress{{id: 3, match: 40}})
		require.Equal(t, uint64(40), r.floor.Load())
		require.NotContains(t, r.matches, uint64(2))
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

	retention := func(maxBytes, floor uint64) *followerRetention {
		r := newFollowerRetention(maxBytes, time.Minute)
		r.floor.Store(floor)

		return r
	}

	t.Run("no follower behind the target keeps the target", func(t *testing.T) {
		t.Parallel()

		got, decision, err := retention(math.MaxUint64, noRetentionFloor).compactionTarget(80, newFakeRetentionLog(10, 100, dataSize))
		require.NoError(t, err)
		require.Equal(t, uint64(80), got)
		require.False(t, decision.held)

		got, decision, err = retention(math.MaxUint64, 80).compactionTarget(80, newFakeRetentionLog(10, 100, dataSize))
		require.NoError(t, err)
		require.Equal(t, uint64(80), got)
		require.False(t, decision.held)
	})

	t.Run("a follower behind the target holds compaction at its match", func(t *testing.T) {
		t.Parallel()

		got, decision, err := retention(math.MaxUint64, 40).compactionTarget(80, newFakeRetentionLog(10, 100, dataSize))
		require.NoError(t, err)
		require.Equal(t, uint64(40), got)
		require.Equal(t, retentionDecision{held: true, floor: 40, retainedBytes: 40 * entrySize}, decision)
	})

	t.Run("the byte cap keeps only the newest entries", func(t *testing.T) {
		t.Parallel()

		// Room for 15 entries and a half: entries 66..80 stay, 65 goes.
		got, decision, err := retention(15*entrySize+entrySize/2, 40).compactionTarget(80, newFakeRetentionLog(10, 100, dataSize))
		require.NoError(t, err)
		require.Equal(t, uint64(65), got)
		require.Equal(t, retentionDecision{held: true, capped: true, floor: 40, retainedBytes: 15 * entrySize}, decision)
	})

	t.Run("a cap smaller than one entry keeps the target", func(t *testing.T) {
		t.Parallel()

		got, decision, err := retention(1, 40).compactionTarget(80, newFakeRetentionLog(10, 100, dataSize))
		require.NoError(t, err)
		require.Equal(t, uint64(80), got)
		require.Equal(t, retentionDecision{capped: true, floor: 40}, decision)
	})

	t.Run("a follower already behind the first index does not hold", func(t *testing.T) {
		t.Parallel()

		got, decision, err := retention(math.MaxUint64, 5).compactionTarget(80, newFakeRetentionLog(10, 100, dataSize))
		require.NoError(t, err)
		require.Equal(t, uint64(80), got)
		require.False(t, decision.held)
	})

	t.Run("a follower exactly at the compacted index holds", func(t *testing.T) {
		t.Parallel()

		got, decision, err := retention(math.MaxUint64, 9).compactionTarget(80, newFakeRetentionLog(10, 100, dataSize))
		require.NoError(t, err)
		require.Equal(t, uint64(9), got)
		require.True(t, decision.held)
	})

	t.Run("a WAL read failure keeps the target", func(t *testing.T) {
		t.Parallel()

		wantErr := errors.New("boom")
		got, _, err := retention(math.MaxUint64, 40).compactionTarget(80, &fakeRetentionLog{err: wantErr})
		require.ErrorIs(t, err, wantErr)
		require.Equal(t, uint64(80), got)
	})
}

// TestDoMaintenanceRetainsLogForCatchingUpFollower reproduces EN-3429 at the
// maintenance boundary: with a follower still replicating behind the
// compaction margin, the leader must keep the entries after the follower's
// Match so the follower is not sent a new snapshot. Once the follower catches
// up, the normal margin applies again.
func TestDoMaintenanceRetainsLogForCatchingUpFollower(t *testing.T) {
	t.Parallel()

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

	// The follower has appended up to 2; the margin alone would compact to 5.
	now := time.Now()
	node.followerRetention.observe(now, true, []followerProgress{{id: 2, match: 2}})
	require.NoError(t, node.doMaintenance())

	snapshot, err := node.wal.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(6), snapshot.GetMetadata().GetIndex())
	_, err = node.wal.Entries(3, 7, math.MaxUint64)
	require.NoError(t, err, "entries after the follower's match must stay available")

	// The follower catches up: compaction returns to the margin.
	node.followerRetention.observe(now, true, []followerProgress{{id: 2, match: 6}})
	require.NoError(t, node.doMaintenance())
	_, err = node.wal.Entries(3, 7, math.MaxUint64)
	require.ErrorIs(t, err, raft.ErrCompacted)
	first, err := node.wal.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(6), first)
}

// TestObserveFollowerRetentionReadsLeaderProgress drives a real RawNode so the
// hold reads the same Progress fields production reads: Match while the peer
// replicates, nothing once the leader sends it a snapshot, and nothing on a
// node that is not the leader.
func TestObserveFollowerRetentionReadsLeaderProgress(t *testing.T) {
	t.Parallel()

	const (
		leaderID = uint64(1)
		peerID   = uint64(2)
	)

	conf := &raftpb.ConfState{Voters: []uint64{leaderID}, Learners: []uint64{peerID}}
	storage := raft.NewMemoryStorage()
	require.NoError(t, storage.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
		Index: proto.Uint64(8), Term: proto.Uint64(1), ConfState: conf,
	}}))
	rn, err := raft.NewRawNode(&raft.Config{
		ID: leaderID, ElectionTick: 10, HeartbeatTick: 1, Storage: storage,
		MaxInflightMsgs: 256, MaxSizePerMsg: 1024 * 1024,
		Logger: NewLoggerAdapter(logging.Testing()),
	})
	require.NoError(t, err)

	node := &Node{
		rawNode:           rn,
		followerRetention: newFollowerRetention(math.MaxUint64, time.Minute),
	}

	now := time.Now()
	node.observeFollowerRetention(now)
	require.Equal(t, uint64(noRetentionFloor), node.followerRetention.floor.Load(), "a follower does not hold")

	drain := func() []*raftpb.Message {
		t.Helper()
		var toPeer []*raftpb.Message
		for rn.HasReady() {
			rd := rn.Ready()
			if !raft.IsEmptyHardState(rd.HardState) {
				require.NoError(t, storage.SetHardState(rd.HardState))
			}
			require.NoError(t, storage.Append(rd.Entries))
			for _, m := range rd.Messages {
				if m.GetTo() == peerID {
					toPeer = append(toPeer, m)
				}
			}
			rn.Advance(rd)
		}

		return toPeer
	}
	fromPeer := func(typ raftpb.MessageType, fields func(*raftpb.Message)) {
		t.Helper()
		m := &raftpb.Message{
			Type: typ.Enum(), From: proto.Uint64(peerID), To: proto.Uint64(leaderID),
			Term: new(rn.Status().GetTerm()),
		}
		if fields != nil {
			fields(m)
		}
		require.NoError(t, rn.Step(m))
	}

	require.NoError(t, rn.Campaign())
	drain()
	require.Equal(t, raft.StateLeader, rn.Status().RaftState)

	// The single voter commits three more entries on its own (10..12).
	for range 3 {
		require.NoError(t, rn.Propose([]byte("x")))
	}
	drain()

	// The peer acknowledges index 9 (the leader's empty entry).
	fromPeer(raftpb.MsgAppResp, func(m *raftpb.Message) { m.Index = proto.Uint64(9) })
	drain()
	node.observeFollowerRetention(now)
	require.Equal(t, uint64(9), node.followerRetention.floor.Load())

	// The leader compacts past the peer's match, then the peer rejects the
	// entries it was sent: the leader can only send it a snapshot, and a
	// follower in StateSnapshot stops holding.
	_, err = storage.CreateSnapshot(12, conf, nil)
	require.NoError(t, err)
	require.NoError(t, storage.Compact(12))
	fromPeer(raftpb.MsgAppResp, func(m *raftpb.Message) {
		m.Index = proto.Uint64(12)
		m.Reject = new(true)
		m.RejectHint = proto.Uint64(9)
		m.LogTerm = proto.Uint64(1)
	})
	drain()
	require.Equal(t, "StateSnapshot", rn.Status().Progress[peerID].State.String())
	node.observeFollowerRetention(now)
	require.Equal(t, uint64(noRetentionFloor), node.followerRetention.floor.Load())
}
