package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// msg builds a *raftpb.Message from scalar fields for terse test literals.
func msg(mtype raftpb.MessageType, from, to uint64) *raftpb.Message {
	return &raftpb.Message{
		Type: new(mtype),
		From: new(from),
		To:   new(to),
	}
}

func msgWithIndex(mtype raftpb.MessageType, from, to, index uint64) *raftpb.Message {
	m := msg(mtype, from, to)
	m.Index = new(index)

	return m
}

func msgWithTerm(mtype raftpb.MessageType, from, to, term uint64) *raftpb.Message {
	m := msg(mtype, from, to)
	m.Term = new(term)

	return m
}

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

	msgs := []*raftpb.Message{
		msg(raftpb.MsgApp, self, 2),
		msg(raftpb.MsgHeartbeat, self, 3),
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

	selfAppendResp := msgWithIndex(raftpb.MsgStorageAppendResp, raft.LocalAppendThread, self, 42)
	selfAppResp := msgWithIndex(raftpb.MsgAppResp, self, self, 42) // leader's self-ack
	peerVoteResp := msgWithTerm(raftpb.MsgVoteResp, self, 2, 3)    // held back until durable
	peerAppResp := msgWithIndex(raftpb.MsgAppResp, self, 3, 42)    // follower→leader after append
	peerAppRespReject := msg(raftpb.MsgAppResp, self, 4)
	peerAppRespReject.Reject = new(true)

	msgs := []*raftpb.Message{
		{
			Type: new(raftpb.MsgStorageAppend),
			To:   proto.Uint64(raft.LocalAppendThread),
			From: proto.Uint64(self),
			Responses: []*raftpb.Message{
				selfAppendResp,
				selfAppResp,
				peerVoteResp,
				peerAppResp,
				peerAppRespReject,
			},
		},
	}

	appendR, applyR, out := splitReadyMessages(self, msgs)

	require.Equal(t, []*raftpb.Message{selfAppendResp, selfAppResp}, appendR,
		"self-directed responses must go to appendResponses")
	require.Empty(t, applyR)
	require.Equal(t, []*raftpb.Message{peerVoteResp, peerAppResp, peerAppRespReject}, out,
		"peer-directed responses must go to outbound (transport.Send)")
}

// TestSplitReadyMessages_ApplyResponsesNoSplit — MsgStorageApply.Responses
// are always self-directed per etcd/raft's newStorageApplyRespMsg, so all of
// them belong in applyResponses regardless of the To field on individual
// responses.
func TestSplitReadyMessages_ApplyResponsesNoSplit(t *testing.T) {
	t.Parallel()

	const self = uint64(1)

	applyResp := msgWithIndex(raftpb.MsgStorageApplyResp, raft.LocalApplyThread, self, 42)

	msgs := []*raftpb.Message{
		{
			Type:      new(raftpb.MsgStorageApply),
			To:        proto.Uint64(raft.LocalApplyThread),
			From:      proto.Uint64(self),
			Responses: []*raftpb.Message{applyResp},
		},
	}

	appendR, applyR, out := splitReadyMessages(self, msgs)
	require.Empty(t, appendR)
	require.Equal(t, []*raftpb.Message{applyResp}, applyR)
	require.Empty(t, out)
}

// TestSplitReadyMessages_MixedRealisticReady — realistic composition: a
// leader emitting a proposal Ready sees an outbound MsgApp to a follower,
// a MsgStorageAppend with self+peer responses, and a MsgStorageApply for
// its own committed entries.
func TestSplitReadyMessages_MixedRealisticReady(t *testing.T) {
	t.Parallel()

	const self = uint64(1)

	outboundApp := msgWithIndex(raftpb.MsgApp, self, 2, 5)
	selfAppendResp := msgWithIndex(raftpb.MsgStorageAppendResp, raft.LocalAppendThread, self, 6)
	selfAppResp := msgWithIndex(raftpb.MsgAppResp, self, self, 6)
	applyResp := msgWithIndex(raftpb.MsgStorageApplyResp, raft.LocalApplyThread, self, 5)

	msgs := []*raftpb.Message{
		outboundApp,
		{
			Type:      new(raftpb.MsgStorageAppend),
			To:        proto.Uint64(raft.LocalAppendThread),
			From:      proto.Uint64(self),
			Responses: []*raftpb.Message{selfAppendResp, selfAppResp},
		},
		{
			Type:      new(raftpb.MsgStorageApply),
			To:        proto.Uint64(raft.LocalApplyThread),
			From:      proto.Uint64(self),
			Responses: []*raftpb.Message{applyResp},
		},
	}

	appendR, applyR, out := splitReadyMessages(self, msgs)

	require.Equal(t, []*raftpb.Message{selfAppendResp, selfAppResp}, appendR)
	require.Equal(t, []*raftpb.Message{applyResp}, applyR)
	require.Equal(t, []*raftpb.Message{outboundApp}, out)
}

// TestSplitReadyMessages_FollowerAllPeer — a follower after appending has no
// self-directed leader-ack in msgsAfterAppend; only a MsgAppResp targeted at
// the leader and the LocalAppendThread self-ack.
func TestSplitReadyMessages_FollowerAllPeer(t *testing.T) {
	t.Parallel()

	const self = uint64(2) // follower
	const leader = uint64(1)

	selfAppendResp := msgWithIndex(raftpb.MsgStorageAppendResp, raft.LocalAppendThread, self, 6)
	appRespToLeader := msgWithIndex(raftpb.MsgAppResp, self, leader, 6)

	msgs := []*raftpb.Message{
		{
			Type:      new(raftpb.MsgStorageAppend),
			To:        proto.Uint64(raft.LocalAppendThread),
			From:      proto.Uint64(self),
			Responses: []*raftpb.Message{selfAppendResp, appRespToLeader},
		},
	}

	appendR, _, out := splitReadyMessages(self, msgs)
	require.Equal(t, []*raftpb.Message{selfAppendResp}, appendR)
	require.Equal(t, []*raftpb.Message{appRespToLeader}, out,
		"follower's MsgAppResp to leader must exit via transport")
}
