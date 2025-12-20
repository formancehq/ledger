package system

import (
	"fmt"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
)

type receptionsChannels struct {
	recv        chan raftpb.Message
	unreachable chan uint64
}

type multiplexedTransport struct {
	grpcTransport         *raft.GRPCTransport
	mainReceptionChannels receptionsChannels
	ledgers               map[uint64]receptionsChannels
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
			ledgerID := ledgerIDFromLedgerNodeID(msg.To)
			if ledgerID == 0 {
				r.logger.Debugf("Received message from main transport: %s", msg.String())
				r.mainReceptionChannels.recv <- msg
				continue
			}

			r.mu.RLock()
			ledger, ok := r.ledgers[ledgerID]
			r.mu.RUnlock()
			if ok {
				r.logger.Debugf("Received message from ledger transport: %s", msg.String())
				ledger.recv <- msg
			} else {
				r.logger.Infof("Received message from unknown ledger: %d (%s)", msg.To, msg.Type)
			}
		case nodeID := <-r.grpcTransport.Unreachable():
			ledgerID := ledgerIDFromLedgerNodeID(nodeID)
			if ledgerID == 0 {
				r.mainReceptionChannels.unreachable <- nodeID
				continue
			}

			r.mu.RLock()
			ledger, ok := r.ledgers[ledgerID]
			r.mu.RUnlock()
			if ok {
				r.logger.Debugf("Received unreachable from ledger transport: %d", nodeID)
				ledger.unreachable <- nodeID
			} else {
				r.logger.Infof("Received unreachable from unknown ledger: %d", nodeID)
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

func (r *multiplexedTransport) NewLedgerTransport(ledgerID uint64) raft.NodeTransport {
	r.mu.Lock()
	defer r.mu.Unlock()

	channels := receptionsChannels{
		recv:        make(chan raftpb.Message, 100),
		unreachable: make(chan uint64, 100),
	}
	r.ledgers[ledgerID] = channels

	return &channelsTransport{
		sender:   r.grpcTransport,
		channels: channels,
		logger: r.logger.WithFields(map[string]any{
			"channel": fmt.Sprintf("ledger/%d", ledgerID),
		}),
		grpcTransport: r.grpcTransport,
	}
}

func (r *multiplexedTransport) GetPeerConnection(nodeID uint64) grpc.ClientConnInterface {
	return r.grpcTransport.GetPeerConnection(NodeIDFromLedgerNodeID(nodeID))
}

func newMultiplexedTransport(logger logging.Logger, grpcTransport *raft.GRPCTransport) *multiplexedTransport {
	return &multiplexedTransport{
		grpcTransport: grpcTransport,
		mainReceptionChannels: receptionsChannels{
			recv:        make(chan raftpb.Message, 100),
			unreachable: make(chan uint64, 100),
		},
		ledgers:     make(map[uint64]receptionsChannels),
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
	target := NodeIDFromLedgerNodeID(msg.To)
	m.logger.Debugf("Sending message to node: %d (%d)", msg.To, target)
	m.sender.Send(target, msg)
}

var _ raft.NodeTransport = (*channelsTransport)(nil)

func ledgerIDFromLedgerNodeID(v uint64) uint64 {
	return (v & 0xFFFF0000) >> 16
}

func NodeIDFromLedgerNodeID(ledgerNodeID uint64) uint64 {
	return ledgerNodeID & 0x0000FFFF
}
