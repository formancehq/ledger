package system

import (
	"fmt"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
)

type receptionsChannels struct {
	recv        *raft.Channel[raftpb.Message]
	unreachable *raft.Channel[uint64]
}

type multiplexedTransport struct {
	grpcTransport         *raft.GRPCTransport
	mainReceptionChannels receptionsChannels
	ledgers               map[uint64]receptionsChannels
	stopChannel           chan chan struct{}
	mu                    sync.RWMutex
	logger                logging.Logger
	meterProvider         metric.MeterProvider
}

func (r *multiplexedTransport) Start() {
	for {
		select {
		case ch := <-r.stopChannel:
			close(ch)
			return
		case incoming := <-r.grpcTransport.Recv():
			ledgerID := ledgerIDFromLedgerNodeID(incoming.Msg.To)
			if ledgerID == 0 {
				r.logger.Debugf("Received message from main transport: %s", incoming.Msg.String())
				if !r.mainReceptionChannels.recv.Send(incoming.Msg) {
					incoming.Rsp <- fmt.Errorf("main transport channel full")
				} else {
					incoming.Rsp <- nil
				}
				continue
			}

			r.mu.RLock()
			ledger, ok := r.ledgers[ledgerID]
			r.mu.RUnlock()
			if ok {
				r.logger.Debugf("Received message from ledger transport: %s", incoming.Msg.String())
				if ledger.recv.Send(incoming.Msg) {
					incoming.Rsp <- nil
				} else {
					incoming.Rsp <- fmt.Errorf("ledger transport channel full")
					r.logger.
						WithFields(map[string]any{
							"channel": fmt.Sprintf("ledger/%d", ledgerID),
							"type":    incoming.Msg.Type.String(),
						}).
						Errorf("Ledger transport channel full, dropping message")
				}
			} else {
				incoming.Rsp <- fmt.Errorf("unknown ledger")
				r.logger.Infof("Received message from %x to unknown ledger: %x (%s)", incoming.Msg.From, incoming.Msg.To, incoming.Msg.Type)
			}
		case nodeID := <-r.grpcTransport.Unreachable():
			ledgerID := ledgerIDFromLedgerNodeID(nodeID)
			if ledgerID == 0 {
				if !r.mainReceptionChannels.unreachable.Send(nodeID) {
					r.logger.Errorf("Main transport channel full, dropping unreachable")
				}
				continue
			}

			r.mu.RLock()
			ledger, ok := r.ledgers[ledgerID]
			r.mu.RUnlock()
			if ok {
				r.logger.Debugf("Received unreachable from ledger transport: %d", nodeID)
				if !ledger.unreachable.Send(nodeID) {
					r.logger.Errorf("Ledger transport channel full, dropping unreachable")
				}
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

	meter := r.meterProvider.Meter("raft.multiplexed_transport.ledger", metric.WithInstrumentationAttributes(
		attribute.String("ledger", fmt.Sprintf("%d", ledgerID)),
	))

	channels := receptionsChannels{
		recv: raft.NewChannel[raftpb.Message](
			"raft.multiplexed_transport.ledger.recv",
			raft.WithLogger[raftpb.Message](r.logger),
			raft.WithMeter[raftpb.Message](meter),
			raft.WithAttributesFn(func(msg raftpb.Message) []attribute.KeyValue {
				ret := raft.AddTypeAsAttribute(msg)
				ret = append(ret, attribute.Int("peer", int(NodeIDFromLedgerNodeID(msg.From))))
				return ret
			}),
		),
		unreachable: raft.NewChannel[uint64](
			"raft.multiplexed_transport.ledger.unreachable",
			raft.WithLogger[uint64](r.logger),
			raft.WithMeter[uint64](meter),
			raft.WithAttributesFn(func(peerID uint64) []attribute.KeyValue {
				return []attribute.KeyValue{
					attribute.Int("peer", int(NodeIDFromLedgerNodeID(peerID))),
				}
			}),
		),
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

func newMultiplexedTransport(logger logging.Logger, grpcTransport *raft.GRPCTransport, meterProvider metric.MeterProvider) *multiplexedTransport {

	meter := meterProvider.Meter("raft.multiplexed_transport.system")

	return &multiplexedTransport{
		grpcTransport: grpcTransport,
		mainReceptionChannels: receptionsChannels{
			recv: raft.NewChannel[raftpb.Message](
				"raft.multiplexed_transport.system.recv",
				raft.WithLogger[raftpb.Message](logger),
				raft.WithMeter[raftpb.Message](meter),
				raft.WithAttributesFn(func(msg raftpb.Message) []attribute.KeyValue {
					ret := raft.AddTypeAsAttribute(msg)
					ret = append(ret, attribute.Int("peer", int(msg.From)))
					return ret
				}),
			),
			unreachable: raft.NewChannel[uint64](
				"raft.multiplexed_transport.system.unreachable",
				raft.WithLogger[uint64](logger),
				raft.WithMeter[uint64](meter),
				raft.WithAttributesFn(func(peerID uint64) []attribute.KeyValue {
					return []attribute.KeyValue{
						attribute.Int("peer", int(peerID)),
					}
				}),
			),
		},
		ledgers:       make(map[uint64]receptionsChannels),
		stopChannel:   make(chan chan struct{}),
		logger:        logger,
		meterProvider: meterProvider,
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
	return m.channels.unreachable.Recv()
}

func (m *channelsTransport) Recv() <-chan raftpb.Message {
	return m.channels.recv.Recv()
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
