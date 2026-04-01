package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestChannelTransport_NodeID(t *testing.T) {
	t.Parallel()

	transport := NewChannelTransport(42, DefaultChannelTransportConfig())
	defer transport.Close()

	require.Equal(t, uint64(42), transport.NodeID())
}

func TestChannelTransport_GetPeerConnection(t *testing.T) {
	t.Parallel()

	transport := NewChannelTransport(1, DefaultChannelTransportConfig())
	defer transport.Close()

	// ChannelTransport always returns nil for gRPC connections
	conn := transport.GetPeerConnection(1)
	require.Nil(t, conn)

	conn = transport.GetPeerConnection(999)
	require.Nil(t, conn)
}

func TestChannelTransport_IsConnected(t *testing.T) {
	t.Parallel()

	t1 := NewChannelTransport(1, DefaultChannelTransportConfig())
	t2 := NewChannelTransport(2, DefaultChannelTransportConfig())

	defer t1.Close()
	defer t2.Close()

	// Initially not connected
	require.False(t, t1.IsConnected(2))
	require.False(t, t2.IsConnected(1))

	// Connect them
	t1.Connect(t2)

	require.True(t, t1.IsConnected(2))
	require.True(t, t2.IsConnected(1))

	// Unknown peer is not connected
	require.False(t, t1.IsConnected(99))
}

func TestChannelTransport_ConnectedPeers(t *testing.T) {
	t.Parallel()

	t1 := NewChannelTransport(1, DefaultChannelTransportConfig())
	t2 := NewChannelTransport(2, DefaultChannelTransportConfig())
	t3 := NewChannelTransport(3, DefaultChannelTransportConfig())

	defer t1.Close()
	defer t2.Close()
	defer t3.Close()

	// Initially no peers
	require.Empty(t, t1.ConnectedPeers())

	// Connect t1 to t2 and t3
	t1.Connect(t2)
	t1.Connect(t3)

	peers := t1.ConnectedPeers()
	require.Len(t, peers, 2)
	require.Contains(t, peers, uint64(2))
	require.Contains(t, peers, uint64(3))

	// t2 should only see t1
	peers2 := t2.ConnectedPeers()
	require.Len(t, peers2, 1)
	require.Contains(t, peers2, uint64(1))
}

func TestChannelTransport_ConnectedPeers_AfterDisconnect(t *testing.T) {
	t.Parallel()

	t1 := NewChannelTransport(1, DefaultChannelTransportConfig())
	t2 := NewChannelTransport(2, DefaultChannelTransportConfig())
	t3 := NewChannelTransport(3, DefaultChannelTransportConfig())

	defer t1.Close()
	defer t2.Close()
	defer t3.Close()

	t1.Connect(t2)
	t1.Connect(t3)

	t1.Disconnect(2)

	peers := t1.ConnectedPeers()
	require.Len(t, peers, 1)
	require.Contains(t, peers, uint64(3))

	require.False(t, t2.IsConnected(1))
}

func TestChannelTransport_SendToDisconnectedPeer(t *testing.T) {
	t.Parallel()

	t1 := NewChannelTransport(1, DefaultChannelTransportConfig())
	defer t1.Close()

	// Send to a peer that doesn't exist — should report unreachable
	t1.Send([]raftpb.Message{{To: 99, Type: raftpb.MsgApp}})

	// The unreachable channel should have the peer ID
	select {
	case peerID := <-t1.Unreachable():
		require.Equal(t, uint64(99), peerID)
	default:
		t.Fatal("expected unreachable report for disconnected peer")
	}
}

func TestChannelTransport_SendToClosedPeer(t *testing.T) {
	t.Parallel()

	t1 := NewChannelTransport(1, DefaultChannelTransportConfig())
	t2 := NewChannelTransport(2, DefaultChannelTransportConfig())

	t1.Connect(t2)

	// Close t2 first
	t2.Close()

	// Send from t1 to t2 — peer is closed, should report unreachable
	t1.Send([]raftpb.Message{{To: 2, Type: raftpb.MsgApp}})

	select {
	case peerID := <-t1.Unreachable():
		require.Equal(t, uint64(2), peerID)
	default:
		t.Fatal("expected unreachable report for closed peer")
	}

	t1.Close()
}

func TestChannelTransport_MessagePriority(t *testing.T) {
	t.Parallel()

	t1 := NewChannelTransport(1, DefaultChannelTransportConfig())
	t2 := NewChannelTransport(2, DefaultChannelTransportConfig())

	defer t1.Close()
	defer t2.Close()

	t1.Connect(t2)

	// Send a heartbeat (high priority)
	t1.Send([]raftpb.Message{{To: 2, Type: raftpb.MsgHeartbeat}})

	select {
	case msgs := <-t2.RecvHighPriority():
		require.Len(t, msgs, 1)
		require.Equal(t, raftpb.MsgHeartbeat, msgs[0].Type)
	default:
		t.Fatal("expected heartbeat on high priority channel")
	}

	// Send a vote (medium priority)
	t1.Send([]raftpb.Message{{To: 2, Type: raftpb.MsgVote}})

	select {
	case msgs := <-t2.RecvMediumPriority():
		require.Len(t, msgs, 1)
		require.Equal(t, raftpb.MsgVote, msgs[0].Type)
	default:
		t.Fatal("expected vote on medium priority channel")
	}

	// Send a data message (low priority)
	t1.Send([]raftpb.Message{{To: 2, Type: raftpb.MsgApp}})

	select {
	case msgs := <-t2.RecvLowPriority():
		require.Len(t, msgs, 1)
		require.Equal(t, raftpb.MsgApp, msgs[0].Type)
	default:
		t.Fatal("expected app message on low priority channel")
	}
}
