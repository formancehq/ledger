package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// TestSplitReadyMessages_EmptyInput covers the trivial case.
func TestSplitReadyMessages_EmptyInput(t *testing.T) {
	t.Parallel()

	appendR, applyR, out := splitReadyMessages(1, nil)
	require.Empty(t, appendR)
	require.Empty(t, applyR)
	require.Empty(t, out)
}

// TestSplitReadyMessages_OutboundPassthrough — a plain MsgApp / MsgHeartbeat
// with To == peer must flow through as-is to outbound.
func TestSplitReadyMessages_OutboundPassthrough(t *testing.T) {
	t.Parallel()

	const self = uint64(1)

	msgs := []raftpb.Message{
		{Type: raftpb.MsgApp, From: self, To: 2},
		{Type: raftpb.MsgHeartbeat, From: self, To: 3},
	}

	appendR, applyR, out := splitReadyMessages(self, msgs)
	require.Empty(t, appendR)
	require.Empty(t, applyR)
	require.Equal(t, msgs, out)
}

// TestSplitReadyMessages_AppendResponsesSelfPeerSplit — the invariant that
// broke cluster formation on the first attempt. MsgStorageAppend.Responses
// mix self-directed acks (Step locally) and peer-directed acks held back
// until durable (send over the wire). Missing the split routes peer msgs
// through Step() and they never make it to their target.
func TestSplitReadyMessages_AppendResponsesSelfPeerSplit(t *testing.T) {
	t.Parallel()

	const self = uint64(1)

	selfAppendResp := raftpb.Message{Type: raftpb.MsgStorageAppendResp, To: self, From: raft.LocalAppendThread, Index: 42}
	selfAppResp := raftpb.Message{Type: raftpb.MsgAppResp, To: self, From: self, Index: 42} // leader's self-ack
	peerVoteResp := raftpb.Message{Type: raftpb.MsgVoteResp, To: 2, From: self, Term: 3}    // held back until durable
	peerAppResp := raftpb.Message{Type: raftpb.MsgAppResp, To: 3, From: self, Index: 42}    // follower→leader after append
	peerAppRespReject := raftpb.Message{Type: raftpb.MsgAppResp, To: 4, From: self, Reject: true}

	msgs := []raftpb.Message{
		{
			Type: raftpb.MsgStorageAppend,
			To:   raft.LocalAppendThread,
			From: self,
			Responses: []raftpb.Message{
				selfAppendResp,
				selfAppResp,
				peerVoteResp,
				peerAppResp,
				peerAppRespReject,
			},
		},
	}

	appendR, applyR, out := splitReadyMessages(self, msgs)

	require.Equal(t, []raftpb.Message{selfAppendResp, selfAppResp}, appendR,
		"self-directed responses must go to appendResponses")
	require.Empty(t, applyR)
	require.Equal(t, []raftpb.Message{peerVoteResp, peerAppResp, peerAppRespReject}, out,
		"peer-directed responses must go to outbound (transport.Send)")
}

// TestSplitReadyMessages_ApplyResponsesNoSplit — MsgStorageApply.Responses
// are always self-directed per etcd/raft's newStorageApplyRespMsg, so all of
// them belong in applyResponses regardless of the To field on individual
// responses.
func TestSplitReadyMessages_ApplyResponsesNoSplit(t *testing.T) {
	t.Parallel()

	const self = uint64(1)

	applyResp := raftpb.Message{Type: raftpb.MsgStorageApplyResp, To: self, From: raft.LocalApplyThread, Index: 42}

	msgs := []raftpb.Message{
		{
			Type:      raftpb.MsgStorageApply,
			To:        raft.LocalApplyThread,
			From:      self,
			Responses: []raftpb.Message{applyResp},
		},
	}

	appendR, applyR, out := splitReadyMessages(self, msgs)
	require.Empty(t, appendR)
	require.Equal(t, []raftpb.Message{applyResp}, applyR)
	require.Empty(t, out)
}

// TestSplitReadyMessages_MixedRealisticReady — realistic composition: a
// leader emitting a proposal Ready sees an outbound MsgApp to a follower,
// a MsgStorageAppend with self+peer responses, and a MsgStorageApply for
// its own committed entries.
func TestSplitReadyMessages_MixedRealisticReady(t *testing.T) {
	t.Parallel()

	const self = uint64(1)

	outboundApp := raftpb.Message{Type: raftpb.MsgApp, From: self, To: 2, Index: 5}
	selfAppendResp := raftpb.Message{Type: raftpb.MsgStorageAppendResp, To: self, From: raft.LocalAppendThread, Index: 6}
	selfAppResp := raftpb.Message{Type: raftpb.MsgAppResp, To: self, From: self, Index: 6}
	applyResp := raftpb.Message{Type: raftpb.MsgStorageApplyResp, To: self, From: raft.LocalApplyThread, Index: 5}

	msgs := []raftpb.Message{
		outboundApp,
		{
			Type:      raftpb.MsgStorageAppend,
			To:        raft.LocalAppendThread,
			From:      self,
			Responses: []raftpb.Message{selfAppendResp, selfAppResp},
		},
		{
			Type:      raftpb.MsgStorageApply,
			To:        raft.LocalApplyThread,
			From:      self,
			Responses: []raftpb.Message{applyResp},
		},
	}

	appendR, applyR, out := splitReadyMessages(self, msgs)

	require.Equal(t, []raftpb.Message{selfAppendResp, selfAppResp}, appendR)
	require.Equal(t, []raftpb.Message{applyResp}, applyR)
	require.Equal(t, []raftpb.Message{outboundApp}, out)
}

// TestSplitReadyMessages_FollowerAllPeer — a follower after appending has no
// self-directed leader-ack in msgsAfterAppend; only a MsgAppResp targeted at
// the leader and the LocalAppendThread self-ack.
func TestSplitReadyMessages_FollowerAllPeer(t *testing.T) {
	t.Parallel()

	const self = uint64(2) // follower
	const leader = uint64(1)

	selfAppendResp := raftpb.Message{Type: raftpb.MsgStorageAppendResp, To: self, From: raft.LocalAppendThread, Index: 6}
	appRespToLeader := raftpb.Message{Type: raftpb.MsgAppResp, To: leader, From: self, Index: 6}

	msgs := []raftpb.Message{
		{
			Type:      raftpb.MsgStorageAppend,
			To:        raft.LocalAppendThread,
			From:      self,
			Responses: []raftpb.Message{selfAppendResp, appRespToLeader},
		},
	}

	appendR, _, out := splitReadyMessages(self, msgs)
	require.Equal(t, []raftpb.Message{selfAppendResp}, appendR)
	require.Equal(t, []raftpb.Message{appRespToLeader}, out,
		"follower's MsgAppResp to leader must exit via transport")
}
