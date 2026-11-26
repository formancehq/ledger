package node

import (
	"bytes"
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

	"github.com/formancehq/ledger/v3/internal/infra/membership"
)

func TestNewFollowerRetentionZeroMaxBytesDisablesTheHold(t *testing.T) {
	t.Parallel()

	require.Nil(t, newFollowerRetention(0, time.Minute))
}

func publishedPositions(r *followerRetention) *retentionPositions {
	return r.published.Load()
}

func TestFollowerRetentionObserve(t *testing.T) {
	t.Parallel()

	t0 := time.Unix(1_000, 0)
	const term = 3

	replicating := func(id, match uint64) followerProgress {
		return followerProgress{id: id, state: tracker.StateReplicate, match: match}
	}
	probing := func(id, match uint64) followerProgress {
		return followerProgress{id: id, state: tracker.StateProbe, match: match}
	}

	snapshotting := func(id, match, pending uint64) followerProgress {
		return followerProgress{id: id, state: tracker.StateSnapshot, match: match, pendingSnapshot: pending}
	}

	t.Run("a follower never heard from does not hold", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		r.observe(t0, term, []followerProgress{replicating(2, 50)})
		require.Nil(t, publishedPositions(r))
	})

	t.Run("publishes the heard followers' positions in ascending order", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		r.heard(2, t0)
		r.heard(3, t0)
		r.observe(t0, term, []followerProgress{replicating(2, 50), replicating(3, 30), replicating(4, 40)})
		require.Equal(t, &retentionPositions{positions: []uint64{30, 50}}, publishedPositions(r))
	})

	t.Run("a message heard before the first observation of a term counts", func(t *testing.T) {
		t.Parallel()

		// A new leader can step a follower's response before its first tick.
		r := newFollowerRetention(1<<20, time.Minute)
		r.heard(2, t0)
		r.observe(t0.Add(time.Millisecond), term, []followerProgress{replicating(2, 50)})
		require.Equal(t, &retentionPositions{positions: []uint64{50}}, publishedPositions(r))
	})

	t.Run("a heard follower that acknowledged nothing has an unknown position", func(t *testing.T) {
		t.Parallel()

		// A new leader probes from its log tail; Match stays 0 until the
		// follower acknowledges. The probe cursor is not a position.
		r := newFollowerRetention(1<<20, time.Minute)
		r.heard(2, t0)
		r.heard(3, t0)
		r.observe(t0, term, []followerProgress{probing(2, 0), replicating(3, 40)})
		require.Equal(t, &retentionPositions{positions: []uint64{40}, unknown: true}, publishedPositions(r))
	})

	t.Run("a sent snapshot counts alongside Match until Match passes it", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		r.heard(2, t0)
		r.observe(t0, term, []followerProgress{snapshotting(2, 3, 70)})
		require.Equal(t, &retentionPositions{positions: []uint64{3, 70}}, publishedPositions(r))

		// Reporting the snapshot sent returns the follower to a probe with
		// its pre-snapshot Match: the sent snapshot still counts.
		r.observe(t0, term, []followerProgress{probing(2, 3)})
		require.Equal(t, &retentionPositions{positions: []uint64{3, 70}}, publishedPositions(r))

		// The snapshot may have been lost: appends resume from a Match below
		// it, which stays a candidate.
		r.observe(t0, term, []followerProgress{replicating(2, 50)})
		require.Equal(t, &retentionPositions{positions: []uint64{50, 70}}, publishedPositions(r))

		r.observe(t0, term, []followerProgress{replicating(2, 90)})
		require.Equal(t, &retentionPositions{positions: []uint64{90}}, publishedPositions(r))
		r.observe(t0, term, []followerProgress{probing(2, 90)})
		require.Equal(t, &retentionPositions{positions: []uint64{90}}, publishedPositions(r), "a passed snapshot is forgotten")
	})

	t.Run("a newer snapshot offer keeps the older one", func(t *testing.T) {
		t.Parallel()

		// Snapshot 2000 is sent and reported; a delayed rejection makes the
		// leader offer 4000, which can be lost while 2000 is installed.
		r := newFollowerRetention(1<<20, time.Minute)
		r.heard(2, t0)
		r.observe(t0, term, []followerProgress{snapshotting(2, 3, 2000)})
		r.observe(t0, term, []followerProgress{probing(2, 3)})
		r.observe(t0, term, []followerProgress{snapshotting(2, 3, 4000)})
		require.Equal(t, &retentionPositions{positions: []uint64{3, 2000, 4000}}, publishedPositions(r))

		// The follower acknowledges 2000: only the newer offer remains.
		r.observe(t0, term, []followerProgress{replicating(2, 2000)})
		require.Equal(t, &retentionPositions{positions: []uint64{2000, 4000}}, publishedPositions(r))
	})

	t.Run("sent snapshots are bounded, oldest first", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		r.heard(2, t0)
		for i := range uint64(maxSentSnapshots + 2) {
			r.observe(t0, term, []followerProgress{snapshotting(2, 3, 100*(i+1))})
		}
		require.Equal(t, []uint64{300, 400, 500, 600, 700, 800, 900, 1000}, r.peers[2].sentSnapshots)
	})

	t.Run("a follower silent past the idle timeout stops holding", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		followers := []followerProgress{replicating(2, 30)}
		r.heard(2, t0)
		r.observe(t0.Add(time.Minute), term, followers)
		require.Equal(t, &retentionPositions{positions: []uint64{30}}, publishedPositions(r), "silence of exactly the idle timeout still holds")

		r.observe(t0.Add(time.Minute+time.Nanosecond), term, followers)
		require.Nil(t, publishedPositions(r))

		// Any message makes it hold again, whatever its progress did.
		r.heard(2, t0.Add(2*time.Minute))
		r.observe(t0.Add(2*time.Minute), term, followers)
		require.Equal(t, &retentionPositions{positions: []uint64{30}}, publishedPositions(r))
	})

	t.Run("a message is aged from when it was stepped", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, 10*time.Millisecond)
		followers := []followerProgress{replicating(2, 30)}
		r.observe(t0, term, followers)
		r.heard(2, t0.Add(99*time.Millisecond))
		r.observe(t0.Add(100*time.Millisecond), term, followers)
		require.Equal(t, &retentionPositions{positions: []uint64{30}}, publishedPositions(r))
	})

	t.Run("a new term keeps liveness and drops sent snapshots", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		r.heard(2, t0)
		r.observe(t0, term, []followerProgress{snapshotting(2, 3, 70)})
		require.Equal(t, &retentionPositions{positions: []uint64{3, 70}}, publishedPositions(r))

		r.observe(t0, term+1, []followerProgress{probing(2, 3)})
		require.Equal(t, &retentionPositions{positions: []uint64{3}}, publishedPositions(r))
	})

	t.Run("a node that is not the leader holds nothing", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		r.heard(2, t0)
		r.observe(t0, term, []followerProgress{replicating(2, 30)})
		r.observe(t0, 0, nil)
		require.Nil(t, publishedPositions(r))
		require.Empty(t, r.peers)
	})

	t.Run("a removed follower is forgotten", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		r.heard(2, t0)
		r.heard(3, t0)
		r.observe(t0, term, []followerProgress{replicating(2, 30), replicating(3, 40)})
		r.observe(t0, term, []followerProgress{replicating(3, 40)})
		require.Equal(t, &retentionPositions{positions: []uint64{40}}, publishedPositions(r))
		require.NotContains(t, r.peers, uint64(2))
	})

	t.Run("a member re-added under the same ID starts with no history", func(t *testing.T) {
		t.Parallel()

		// Removal and re-addition both apply between two observations, each
		// forgetting the ID; the replacement has not answered yet.
		r := newFollowerRetention(1<<20, time.Minute)
		r.heard(2, t0)
		r.observe(t0, term, []followerProgress{snapshotting(2, 30, 70)})
		r.forget(2)
		r.forget(2)
		r.observe(t0, term, []followerProgress{probing(2, 0)})
		require.Nil(t, publishedPositions(r), "the replacement has not been heard from")
		require.Equal(t, &followerHistory{}, r.peers[2], "the replacement inherits no sent snapshot")
	})

	t.Run("a replacement's acknowledgement below the old Match holds", func(t *testing.T) {
		t.Parallel()

		r := newFollowerRetention(1<<20, time.Minute)
		r.heard(2, t0)
		r.observe(t0, term, []followerProgress{replicating(2, 30)})
		r.forget(2)
		r.heard(2, t0)
		r.observe(t0, term, []followerProgress{replicating(2, 10)})
		require.Equal(t, &retentionPositions{positions: []uint64{10}}, publishedPositions(r))
	})
}

func TestForgetRetainedFollowersOnConfChange(t *testing.T) {
	t.Parallel()

	t0 := time.Unix(1_000, 0)
	// Voters 1, 2; learners 3, 4, 5. Node 6 is not a member.
	prior := &raftpb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3, 4, 5}}
	node := &Node{followerRetention: newFollowerRetention(1<<20, time.Minute)}
	for _, id := range []uint64{2, 3, 4, 5, 6} {
		node.followerRetention.heard(id, t0)
	}
	node.followerRetention.observe(t0, 3, []followerProgress{
		{id: 2, state: tracker.StateReplicate, match: 20},
		{id: 3, state: tracker.StateReplicate, match: 30},
		{id: 4, state: tracker.StateReplicate, match: 40},
		{id: 5, state: tracker.StateReplicate, match: 50},
		{id: 6, state: tracker.StateReplicate, match: 60},
	})

	node.forgetRetainedFollowers(prior, &raftpb.ConfChangeV2{Changes: []*raftpb.ConfChangeSingle{
		{Type: new(raftpb.ConfChangeRemoveNode), NodeId: proto.Uint64(2)},
		{Type: new(raftpb.ConfChangeAddNode), NodeId: proto.Uint64(3)},
		{Type: new(raftpb.ConfChangeUpdateNode), NodeId: proto.Uint64(4)},
		{Type: new(raftpb.ConfChangeAddLearnerNode), NodeId: proto.Uint64(6)},
	}})
	r := node.followerRetention
	for id, kept := range map[uint64]bool{
		2: false, // removed: the incarnation ends
		3: true,  // promoted: the same replica keeps its hold
		4: false, // given a new identity
		5: true,  // untouched
		6: false, // added: a new member starts afresh
	} {
		require.Equal(t, kept, r.lastHeard[id] == t0, "lastHeard of %d", id)
		_, hasPeer := r.peers[id]
		require.Equal(t, kept, hasPeer, "history of %d", id)
	}

	// Disabled retention ignores configuration changes.
	(&Node{}).forgetRetainedFollowers(prior, &raftpb.ConfChangeV2{Changes: []*raftpb.ConfChangeSingle{
		{Type: new(raftpb.ConfChangeRemoveNode), NodeId: proto.Uint64(2)},
	}})
}

// TestForgetRetainedFollowersOnUpdateNode separates the two uses of
// ConfChangeUpdateNode: refreshing a member's endpoints or capabilities keeps
// the same replica and its hold, while a new identity starts afresh.
func TestForgetRetainedFollowersOnUpdateNode(t *testing.T) {
	t.Parallel()

	t0 := time.Unix(1_000, 0)
	registered := bytes.Repeat([]byte{0xA}, membership.InstanceIDLen)
	replacement := bytes.Repeat([]byte{0xB}, membership.InstanceIDLen)

	update := func(t *testing.T, instanceID []byte) *raftpb.ConfChangeV2 {
		t.Helper()
		ctx, err := membership.MarshalConfChangeContext(membership.ConfChangeContext{
			RaftAddress: "node-2:7000", ServiceAddress: "node-2:8000", InstanceID: instanceID,
		})
		require.NoError(t, err)

		return &raftpb.ConfChangeV2{
			Changes: []*raftpb.ConfChangeSingle{{Type: new(raftpb.ConfChangeUpdateNode), NodeId: proto.Uint64(2)}},
			Context: ctx,
		}
	}

	for name, tc := range map[string]struct {
		instanceID []byte
		kept       bool
	}{
		"same instance refresh keeps the hold": {instanceID: registered, kept: true},
		"new identity forgets the hold":        {instanceID: replacement, kept: false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			node := &Node{
				membership:        newTestMembership(t),
				followerRetention: newFollowerRetention(1<<20, time.Minute),
			}
			require.NoError(t, node.membership.Set(2, "node-2:7000", "node-2:8000", registered, nil))
			node.followerRetention.heard(2, t0)
			node.followerRetention.observe(t0, 3, []followerProgress{
				{id: 2, state: tracker.StateSnapshot, match: 80, pendingSnapshot: 100},
			})

			prior := &raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
			node.forgetRetainedFollowers(prior, update(t, tc.instanceID))
			_, hasPeer := node.followerRetention.peers[2]
			require.Equal(t, tc.kept, hasPeer)
			require.Equal(t, tc.kept, node.followerRetention.lastHeard[2] == t0)
		})
	}
}

func TestConfStateHasMember(t *testing.T) {
	t.Parallel()

	cs := &raftpb.ConfState{
		Voters: []uint64{1}, Learners: []uint64{2}, VotersOutgoing: []uint64{3}, LearnersNext: []uint64{4},
	}
	for _, id := range []uint64{1, 2, 3, 4} {
		require.True(t, confStateHasMember(cs, id), "member %d", id)
	}
	require.False(t, confStateHasMember(cs, 5))
	require.False(t, confStateHasMember(nil, 1))
}

// fakeRetentionLog serves entries [first, first+len(entries)).
type fakeRetentionLog struct {
	entries []*raftpb.Entry
	err     error
	// reads records each Entries range [lo, hi).
	reads [][2]uint64
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
	l.reads = append(l.reads, [2]uint64{lo, hi})
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

	retention := func(maxBytes uint64, published *retentionPositions) *followerRetention {
		r := newFollowerRetention(maxBytes, time.Minute)
		r.published.Store(published)

		return r
	}
	at := func(positions ...uint64) *retentionPositions { return &retentionPositions{positions: positions} }
	// The fake WAL retains entries 10..100: the compacted index is 9.
	walLog := func() *fakeRetentionLog { return newFakeRetentionLog(10, 100, dataSize) }

	t.Run("no follower behind the target keeps the target", func(t *testing.T) {
		t.Parallel()

		got, decision, err := retention(math.MaxUint64, nil).compactionTarget(80, walLog())
		require.NoError(t, err)
		require.Equal(t, uint64(80), got)
		require.Equal(t, retentionDecision{}, decision)

		got, decision, err = retention(math.MaxUint64, at(80, 90)).compactionTarget(80, walLog())
		require.NoError(t, err)
		require.Equal(t, uint64(80), got)
		require.Equal(t, retentionDecision{}, decision)
	})

	t.Run("a follower behind the target holds compaction at its position", func(t *testing.T) {
		t.Parallel()

		// Compacting at 40 keeps entries 40..79 that compacting at 80 releases.
		got, decision, err := retention(math.MaxUint64, at(40, 90)).compactionTarget(80, walLog())
		require.NoError(t, err)
		require.Equal(t, uint64(40), got)
		require.Equal(t, retentionDecision{held: true, need: 40, retainedBytes: 40 * entrySize}, decision)
	})

	t.Run("a follower already compacted away does not cancel the hold for others", func(t *testing.T) {
		t.Parallel()

		got, decision, err := retention(math.MaxUint64, at(5, 40)).compactionTarget(80, walLog())
		require.NoError(t, err)
		require.Equal(t, uint64(40), got)
		require.Equal(t, retentionDecision{held: true, need: 40, retainedBytes: 40 * entrySize}, decision)
	})

	t.Run("a usable Match below a sent snapshot holds", func(t *testing.T) {
		t.Parallel()

		// The snapshot at 150 may have been lost and appends resumed from 110.
		walLog := newFakeRetentionLog(90, 200, dataSize)
		var want uint64
		for _, e := range walLog.entries[110-90 : 140-90] {
			want += uint64(proto.Size(e))
		}
		got, decision, err := retention(math.MaxUint64, at(110, 150)).compactionTarget(140, walLog)
		require.NoError(t, err)
		require.Equal(t, uint64(110), got)
		require.Equal(t, retentionDecision{held: true, need: 110, retainedBytes: want}, decision)
	})

	t.Run("only followers already compacted away keep the target", func(t *testing.T) {
		t.Parallel()

		got, decision, err := retention(math.MaxUint64, at(5)).compactionTarget(80, walLog())
		require.NoError(t, err)
		require.Equal(t, uint64(80), got)
		require.Equal(t, retentionDecision{}, decision)
	})

	t.Run("a follower of unknown position keeps the retained log", func(t *testing.T) {
		t.Parallel()

		got, decision, err := retention(math.MaxUint64, &retentionPositions{positions: []uint64{90}, unknown: true}).compactionTarget(80, walLog())
		require.NoError(t, err)
		require.Equal(t, uint64(9), got)
		require.Equal(t, retentionDecision{held: true, need: 9, retainedBytes: 70 * entrySize}, decision)
	})

	t.Run("a follower exactly at the compacted index holds without charging the boundary entry", func(t *testing.T) {
		t.Parallel()

		// Entry 9 is the WAL's boundary: raft.Storage cannot read it, so only
		// entries 10..79 are charged.
		got, decision, err := retention(math.MaxUint64, at(9)).compactionTarget(80, walLog())
		require.NoError(t, err)
		require.Equal(t, uint64(9), got)
		require.Equal(t, retentionDecision{held: true, need: 9, retainedBytes: 70 * entrySize}, decision)
	})

	t.Run("the byte cap keeps only the newest entries", func(t *testing.T) {
		t.Parallel()

		// Room for 15 entries and a half: entries 65..79 stay, so compaction
		// lands on 65 and keeps it as the matching entry.
		got, decision, err := retention(15*entrySize+entrySize/2, at(40)).compactionTarget(80, walLog())
		require.NoError(t, err)
		require.Equal(t, uint64(65), got)
		require.Equal(t, retentionDecision{held: true, capped: true, need: 40, retainedBytes: 15 * entrySize}, decision)
	})

	t.Run("sizing reads bounded windows from the target down", func(t *testing.T) {
		t.Parallel()

		const target = 3000
		walLog := newFakeRetentionLog(10, 5000, dataSize)
		var want uint64
		for _, e := range walLog.entries[:target-10] {
			want += uint64(proto.Size(e))
		}
		got, decision, err := retention(math.MaxUint64, at(9)).compactionTarget(target, walLog)
		require.NoError(t, err)
		require.Equal(t, uint64(9), got)
		require.Equal(t, retentionDecision{held: true, need: 9, retainedBytes: want}, decision)
		require.Equal(t, [][2]uint64{{1976, 3000}, {952, 1976}, {10, 952}}, walLog.reads)
	})

	t.Run("sizing stops reading at the cap", func(t *testing.T) {
		t.Parallel()

		walLog := newFakeRetentionLog(10, 5000, dataSize)
		_, decision, err := retention(10*entrySize, at(9)).compactionTarget(3000, walLog)
		require.NoError(t, err)
		require.True(t, decision.capped)
		require.Equal(t, [][2]uint64{{1976, 3000}}, walLog.reads)
	})

	t.Run("a cap smaller than one entry keeps the target", func(t *testing.T) {
		t.Parallel()

		got, decision, err := retention(1, at(40)).compactionTarget(80, walLog())
		require.NoError(t, err)
		require.Equal(t, uint64(80), got)
		require.Equal(t, retentionDecision{capped: true, need: 40}, decision)
	})

	t.Run("a WAL read failure keeps the target", func(t *testing.T) {
		t.Parallel()

		wantErr := errors.New("boom")
		got, _, err := retention(math.MaxUint64, at(40)).compactionTarget(80, &fakeRetentionLog{err: wantErr})
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
	// heard, when set, sees every message stepped from the peer, as the
	// orchestrate goroutine's stepMessages does.
	heard func(from uint64)
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
	if l.heard != nil {
		l.heard(m.GetFrom())
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

func (l *testRetentionLeader) ack(index uint64) {
	l.t.Helper()
	l.fromPeer(raftpb.MsgAppResp, func(m *raftpb.Message) { m.Index = proto.Uint64(index) })
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

// attach makes node lead through l and records the peer for the hold, as
// a first Raft tick does; messages stepped from the peer reach the hold.
func (l *testRetentionLeader) attach(node *Node) {
	node.rawNode = l.rn
	node.observeFollowerRetention(time.Now())
	l.heard = func(from uint64) { node.followerRetention.heard(from, time.Now()) }
}

func sentSnapshotRecipients(t *testing.T, msgs []*raftpb.Message, wantIndex uint64) []uint64 {
	t.Helper()
	var recipients []uint64
	for _, m := range msgs {
		if m.GetType() == raftpb.MsgSnap {
			require.Equal(t, wantIndex, m.GetSnapshot().GetMetadata().GetIndex())
			recipients = append(recipients, m.GetTo())
		}
	}

	return recipients
}

// newTestRetentionMaintenanceNode returns a maintenance node whose WAL holds
// six applied entries, with a compaction margin of 1 (normal target 5), the
// hold enabled, and its orchestrate command loop served for the test.
func newTestRetentionMaintenanceNode(t *testing.T) *Node {
	t.Helper()

	node := newTestRetentionMaintenanceNodeWithoutOrchestrator(t)

	// Serve the orchestrate command loop: maintenance refreshes follower
	// positions on it.
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

// newTestRetentionMaintenanceNodeWithoutOrchestrator is
// newTestRetentionMaintenanceNode with nothing serving the command loop, as
// after the orchestrate goroutine has stopped.
func newTestRetentionMaintenanceNodeWithoutOrchestrator(t *testing.T) *Node {
	t.Helper()

	node, setup := newTestMaintenanceNode(t)
	setup.applier.compactionMargin = 1
	node.followerRetention = newFollowerRetention(math.MaxUint64, time.Hour)

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
	node.clusterCommandCh = make(chan *clusterCommand, 1)

	return node
}

func requireFirstIndex(t *testing.T, node *Node, want uint64, msg string) {
	t.Helper()
	first, err := node.wal.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, want, first, msg)
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
	leader.attach(node)
	leader.ack(2)

	// The margin alone would compact to 5.
	require.NoError(t, node.doMaintenance(nil))
	snapshot, err := node.wal.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(6), snapshot.GetMetadata().GetIndex())
	requireFirstIndex(t, node, 3, "entries after the follower's position must stay available")

	// The follower catches up: compaction returns to the margin.
	leader.ack(6)
	require.NoError(t, node.doMaintenance(nil))
	requireFirstIndex(t, node, 6, "a caught-up follower must not hold")
}

// TestDoMaintenanceKeepsLogForFollowerNotYetAcknowledged covers a new leader
// whose follower responds but has acknowledged nothing yet. Raft probes it
// from the leader's log tail as of the election, which says nothing about
// the follower's log: it may hold an entry the leader would otherwise
// compact. The leader keeps its retained log until the follower acknowledges
// a position.
func TestDoMaintenanceKeepsLogForFollowerNotYetAcknowledged(t *testing.T) {
	t.Parallel()

	node := newTestRetentionMaintenanceNode(t)

	leader := newTestRetentionLeader(t, 1)
	leader.propose(4)
	leader.attach(node)
	leader.fromPeer(raftpb.MsgHeartbeatResp, nil)
	leader.drain()
	pr := leader.rn.Status().Progress[testRetentionPeerID]
	require.Equal(t, tracker.StateProbe, pr.State)
	require.Zero(t, pr.Match, "the follower has acknowledged nothing in this term")

	require.NoError(t, node.doMaintenance(nil))
	requireFirstIndex(t, node, 1, "nothing may be compacted before the follower acknowledges")

	leader.ack(3)
	require.NoError(t, node.doMaintenance(nil))
	requireFirstIndex(t, node, 4, "the acknowledged position bounds compaction")
}

// TestDoMaintenanceSeesSnapshotSentSinceLastTick covers a snapshot RawNode
// chooses after the last tick sampled the positions. The tick saw the
// follower before it was heard from, so it held nothing; since then the
// leader sent the follower a snapshot at 3. Maintenance must refresh the
// positions on the orchestrate goroutine and keep the entries after 3, not
// compact to its normal target 5 from the stale sample.
func TestDoMaintenanceSeesSnapshotSentSinceLastTick(t *testing.T) {
	t.Parallel()

	node := newTestRetentionMaintenanceNode(t)

	leader := newTestRetentionLeader(t, 3)
	leader.propose(2)
	leader.attach(node)
	require.Nil(t, publishedPositions(node.followerRetention))

	// The follower's log ends at 1, before the leader's: it is sent the
	// snapshot at 3. No tick observes it.
	require.Equal(t, []uint64{testRetentionPeerID}, sentSnapshotRecipients(t, leader.rejectProbe(1), 3))
	require.Equal(t, tracker.StateSnapshot, leader.rn.Status().Progress[testRetentionPeerID].State)
	require.Nil(t, publishedPositions(node.followerRetention), "no tick has observed the snapshot yet")

	require.NoError(t, node.doMaintenance(nil))
	requireFirstIndex(t, node, 4, "entries after the snapshot index must stay available")
}

// reportSnapshots does what finishReady does with the MsgSnap in msgs.
func (l *testRetentionLeader) reportSnapshots(msgs []*raftpb.Message) {
	for _, to := range snapshotRecipients(msgs) {
		l.rn.ReportSnapshot(to, raft.SnapshotFinish)
	}
}

// TestDoMaintenanceHoldsAtReportedSnapshot covers a snapshot sent and
// reported between two samples. Reporting it returns the follower to a probe
// with its pre-snapshot Match, so only the index recorded from the message
// knows the snapshot. Maintenance must hold at that index.
func TestDoMaintenanceHoldsAtReportedSnapshot(t *testing.T) {
	t.Parallel()

	node := newTestRetentionMaintenanceNode(t)

	leader := newTestRetentionLeader(t, 3)
	leader.propose(2)
	leader.attach(node)

	msgs := leader.rejectProbe(1)
	require.Equal(t, []uint64{testRetentionPeerID}, sentSnapshotRecipients(t, msgs, 3))
	node.recordSentSnapshots(msgs) // as nextReady does
	leader.reportSnapshots(msgs)
	pr := leader.rn.Status().Progress[testRetentionPeerID]
	require.Equal(t, tracker.StateProbe, pr.State)
	require.Zero(t, pr.Match, "the report leaves Match before the snapshot")

	require.NoError(t, node.doMaintenance(nil))
	requireFirstIndex(t, node, 4, "entries after the reported snapshot must stay available")
}

// TestRecordSentSnapshotsKeepsIndexClearedByAck covers an acknowledgement
// stepped after the Ready carrying a snapshot is created and before
// finishReady handles it, with a maintenance refresh in between. The
// acknowledgement is still in the leader's log, so raft leaves StateSnapshot
// and clears the pending snapshot from Progress. The follower may still
// install the snapshot, so its index, recorded from the message when the
// Ready was created, must stay a candidate position.
func TestRecordSentSnapshotsKeepsIndexClearedByAck(t *testing.T) {
	t.Parallel()

	node := &Node{followerRetention: newFollowerRetention(math.MaxUint64, time.Hour)}

	// The leader's log is 2..6; it snapshots at 5 but keeps entries from 4.
	leader := newTestRetentionLeader(t, 1)
	leader.propose(4)
	_, err := leader.storage.CreateSnapshot(5, leader.conf, nil)
	require.NoError(t, err)
	require.NoError(t, leader.storage.Compact(3))
	leader.attach(node)

	// The leader probes the peer from 1, already compacted: it sends the
	// snapshot at 5.
	leader.fromPeer(raftpb.MsgHeartbeatResp, nil)
	msgs := leader.drain()
	require.Equal(t, []uint64{testRetentionPeerID}, sentSnapshotRecipients(t, msgs, 5))
	node.recordSentSnapshots(msgs) // as nextReady does

	// A delayed acknowledgement of 4 lands before finishReady.
	leader.ack(4)
	pr := leader.rn.Status().Progress[testRetentionPeerID]
	require.NotEqual(t, tracker.StateSnapshot, pr.State)
	require.Zero(t, pr.PendingSnapshot, "the acknowledgement cleared the pending snapshot")

	// The maintenance refresh runs before finishReady reports the snapshot.
	node.observeFollowerRetention(time.Now())
	require.Equal(t, &retentionPositions{positions: []uint64{4, 5}}, publishedPositions(node.followerRetention))

	leader.reportSnapshots(msgs)
	node.observeFollowerRetention(time.Now())
	require.Equal(t, &retentionPositions{positions: []uint64{4, 5}}, publishedPositions(node.followerRetention))
}

// TestNextReadyRecordsSentSnapshots checks the production hook: the Ready
// that carries a MsgSnap records its index as it is taken from RawNode.
func TestNextReadyRecordsSentSnapshots(t *testing.T) {
	t.Parallel()

	node := &Node{followerRetention: newFollowerRetention(math.MaxUint64, time.Hour)}
	leader := newTestRetentionLeader(t, 1)
	leader.propose(4)
	_, err := leader.storage.CreateSnapshot(5, leader.conf, nil)
	require.NoError(t, err)
	require.NoError(t, leader.storage.Compact(3))
	leader.attach(node)

	leader.fromPeer(raftpb.MsgHeartbeatResp, nil)
	require.True(t, leader.rn.HasReady())
	rd := node.nextReady()
	require.Equal(t, []uint64{testRetentionPeerID}, sentSnapshotRecipients(t, rd.Messages, 5))
	require.Equal(t, []uint64{5}, node.followerRetention.peers[testRetentionPeerID].sentSnapshots)
}

// TestDoMaintenanceStopAbandonsRetentionRefresh covers shutdown: the refresh
// command can be queued just as the orchestrate goroutine takes its stop
// branch, and nothing consumes it. Closing the maintenance task's stop
// channel must release maintenance long before the refresh timeout, and a
// stopping task compacts no further.
func TestDoMaintenanceStopAbandonsRetentionRefresh(t *testing.T) {
	t.Parallel()

	node := newTestRetentionMaintenanceNodeWithoutOrchestrator(t)
	timeout := node.followerRetention.refreshTimeout

	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- node.doMaintenance(stop) }()
	require.Eventually(t, func() bool { return len(node.clusterCommandCh) == 1 },
		timeout/2, time.Millisecond, "the refresh command must be queued")

	close(stop)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(timeout / 2):
		require.FailNow(t, "closing stop must release maintenance before the refresh timeout")
	}

	snapshot, err := node.wal.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(6), snapshot.GetMetadata().GetIndex(), "the WAL snapshot does not depend on the refresh")
	requireFirstIndex(t, node, 1, "a stopping task must not compact")
}

// TestDoMaintenanceRefreshTimeoutHoldsWithinCap covers an orchestrate
// goroutine too busy to refresh the positions in time. The published
// positions may miss a snapshot sent since, so maintenance treats every
// follower's position as unknown, but it still compacts what lies beyond the
// byte cap. A node not known to lead holds nothing.
func TestDoMaintenanceRefreshTimeoutHoldsWithinCap(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		state     raft.StateType
		maxBytes  uint64
		wantFirst uint64
	}{
		"an unbounded cap holds the whole retained log": {state: raft.StateLeader, maxBytes: math.MaxUint64, wantFirst: 1},
		"a cap below one entry compacts to the target":  {state: raft.StateLeader, maxBytes: 1, wantFirst: 6},
		"a follower compacts to the target":             {state: raft.StateFollower, maxBytes: math.MaxUint64, wantFirst: 6},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			node := newTestRetentionMaintenanceNodeWithoutOrchestrator(t)
			node.followerRetention = newFollowerRetention(tc.maxBytes, time.Hour)
			node.followerRetention.refreshTimeout = time.Millisecond
			node.lastSoftState.Store(&raft.SoftState{Lead: 1, RaftState: tc.state})

			require.NoError(t, node.doMaintenance(nil))
			requireFirstIndex(t, node, tc.wantFirst, "")
		})
	}
}

// TestObserveFollowerRetentionFollowsSnapshotHandoff drives a real RawNode so
// the hold reads the same Progress fields production reads, through the
// snapshot handoff: in flight, reported sent with a stale Match, then
// acknowledged. A node that is not the leader holds nothing.
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
		followerRetention: newFollowerRetention(math.MaxUint64, time.Hour),
	}
	node.observeFollowerRetention(time.Now())
	require.Nil(t, publishedPositions(node.followerRetention), "a node that is not the leader does not hold")

	leader := newTestRetentionLeader(t, 8)
	leader.attach(node)

	// The single voter commits three more entries on its own (10..12), and
	// the peer acknowledges index 9 (the leader's empty entry).
	leader.propose(3)
	leader.ack(9)
	node.observeFollowerRetention(time.Now())
	require.Equal(t, &retentionPositions{positions: []uint64{9}}, publishedPositions(node.followerRetention))

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
	msgs := leader.drain()
	recipients := sentSnapshotRecipients(t, msgs, 12)
	require.Equal(t, []uint64{testRetentionPeerID}, recipients)
	node.observeFollowerRetention(time.Now())
	require.Equal(t, &retentionPositions{positions: []uint64{9, 12}}, publishedPositions(node.followerRetention),
		"a follower being sent a snapshot can resume from its Match or the snapshot")

	leader.reportSnapshots(msgs)
	pr := leader.rn.Status().Progress[testRetentionPeerID]
	require.Equal(t, tracker.StateProbe, pr.State)
	require.Equal(t, uint64(9), pr.Match)
	node.observeFollowerRetention(time.Now())
	require.Equal(t, &retentionPositions{positions: []uint64{9, 12}}, publishedPositions(node.followerRetention),
		"the snapshot index must survive the report until the follower acknowledges it")

	leader.ack(12)
	require.Equal(t, uint64(12), leader.rn.Status().Progress[testRetentionPeerID].Match)
	node.observeFollowerRetention(time.Now())
	require.Equal(t, &retentionPositions{positions: []uint64{12}}, publishedPositions(node.followerRetention))
}
