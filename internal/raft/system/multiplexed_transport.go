package system

import (
	"fmt"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type receptionsChannels struct {
	recv        chan raftpb.Message
	unreachable chan uint64
}

type multiplexedTransport struct {
	grpcTransport         *raft.GRPCTransport
	mainReceptionChannels receptionsChannels
	buckets               map[uint64]receptionsChannels
	stopChannel           chan chan struct{}
	mu                    sync.RWMutex
	logger                logging.Logger
}

func (r *multiplexedTransport) Start() {
	for {
		select {
		case ch := <-r.stopChannel:
			close(ch)
		case msg := <-r.grpcTransport.Recv():
			bucketID := bucketIDFromBucketNodeID(msg.To)
			if bucketID == 0 {
				r.logger.Debugf("Received message from main transport: %s", msg.String())
				r.mainReceptionChannels.recv <- msg
				continue
			}

			r.mu.RLock()
			bucket, ok := r.buckets[bucketID]
			r.mu.RUnlock()
			if ok {
				r.logger.Debugf("Received message from bucket transport: %s", msg.String())
				bucket.recv <- msg
			} else {
				r.logger.Infof("Received message from unknown bucket: %d", msg.To)
			}
		case nodeID := <-r.grpcTransport.Unreachable():
			bucketID := bucketIDFromBucketNodeID(nodeID)
			if bucketID == 0 {
				r.mainReceptionChannels.unreachable <- nodeID
				continue
			}

			r.mu.RLock()
			bucket, ok := r.buckets[bucketID]
			r.mu.RUnlock()
			if ok {
				r.logger.Debugf("Received unreachable from bucket transport: %d", nodeID)
				bucket.unreachable <- nodeID
			} else {
				r.logger.Infof("Received unreachable from unknown bucket: %d", nodeID)
			}
		}
	}
}

func (r *multiplexedTransport) Stop() {
	ch := make(chan struct{})
	r.stopChannel <- ch
	<-ch
}

func (r *multiplexedTransport) MainTransport() raft.NodeTransport {
	return &channelsTransport{
		sender:   r.grpcTransport,
		channels: r.mainReceptionChannels,
		logger: r.logger.WithFields(map[string]any{
			"channel": "main",
		}),
		grpcTransport: r.grpcTransport,
	}
}

func (r *multiplexedTransport) NewBucketTransport(bucketID uint64) raft.NodeTransport {
	r.mu.Lock()
	defer r.mu.Unlock()

	channels := receptionsChannels{
		recv:        make(chan raftpb.Message, 100),
		unreachable: make(chan uint64, 100),
	}
	r.buckets[bucketID] = channels

	return &channelsTransport{
		sender:   r.grpcTransport,
		channels: channels,
		logger: r.logger.WithFields(map[string]any{
			"channel": fmt.Sprintf("bucket/%d", bucketID),
		}),
		grpcTransport: r.grpcTransport,
	}
}

func newMultiplexedTransport(logger logging.Logger, grpcTransport *raft.GRPCTransport) *multiplexedTransport {
	return &multiplexedTransport{
		grpcTransport: grpcTransport,
		mainReceptionChannels: receptionsChannels{
			recv:        make(chan raftpb.Message, 100),
			unreachable: make(chan uint64, 100),
		},
		buckets:     make(map[uint64]receptionsChannels),
		stopChannel: make(chan chan struct{}),
		logger:      logger,
	}
}

type sender interface {
	Send(to uint64, msg raftpb.Message)
}

type channelsTransport struct {
	logger        logging.Logger
	sender        sender
	channels      receptionsChannels
	grpcTransport *raft.GRPCTransport
}

func (m *channelsTransport) GetPeerAddress(peerID uint64) string {
	return m.grpcTransport.GetPeerAddress(peerID)
}

func (m *channelsTransport) Unreachable() <-chan uint64 {
	return m.channels.unreachable
}

func (m *channelsTransport) Recv() <-chan raftpb.Message {
	return m.channels.recv
}

func (m *channelsTransport) Send(msg raftpb.Message) {
	target := NodeIDFromBucketNodeID(msg.To)
	m.logger.Debugf("Sending message to node: %d (%d)", msg.To, target)
	m.sender.Send(target, msg)
}

var _ raft.NodeTransport = (*channelsTransport)(nil)
