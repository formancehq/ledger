package node

import (
	"errors"

	"go.etcd.io/raft/v3"
)

// errEmptyLeadershipGainReady reports that the Ready carrying the SoftState
// transition to leader had no entries. etcd/raft is expected to co-emit the
// no-op blank entry of the new term in the same Ready — that entry is the
// leader-completeness anchor the FSM catch-up barrier waits on. Without
// it the new leader cannot establish when it has caught up with everything
// the previous leader had applied (#329).
var errEmptyLeadershipGainReady = errors.New("leadership gain Ready has no entries — etcd/raft contract changed (the no-op blank entry was expected on the same Ready as the SoftState transition)")

// leadershipGainTarget returns the FSM catch-up target index from a Ready
// that carries the leadership transition to StateLeader. Returns
// errEmptyLeadershipGainReady when rd has no entries — see the variable
// comment for the contract rationale.
//
// Kept as a separate function so the empty-entries branch is testable
// without standing up the whole raft node.
func leadershipGainTarget(rd raft.Ready) (uint64, error) {
	if len(rd.Entries) == 0 {
		return 0, errEmptyLeadershipGainReady
	}

	return rd.Entries[len(rd.Entries)-1].GetIndex(), nil
}
