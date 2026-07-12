package node

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/monitoring/otlplogs"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/proto/rafttransportpb"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source transport.go -destination transport_generated_test.go -typed -package node . Transport
type Transport interface {
	Unreachable() <-chan uint64
	RecvHighPriority() <-chan []raftpb.Message
	RecvMediumPriority() <-chan []raftpb.Message
	RecvLowPriority() <-chan []raftpb.Message
	Send(msg []raftpb.Message)
}

// DefaultTransport handles network communication between Raft nodes using gRPC
// It wraps GRPCClientPool and manages Raft-specific message routing and channels.
type DefaultTransport struct {
	rafttransportpb.UnimplementedRaftTransportServiceServer

	connectionPool *transport.ConnectionPool

	// 3 priority queues for incoming message batches (high to low priority)
	highPriorityRecvCh   chan []raftpb.Message // Heartbeats
	mediumPriorityRecvCh chan []raftpb.Message // Votes, responses
	lowPriorityRecvCh    chan []raftpb.Message // Data messages

	// Channels for outgoing messages per peer
	peersMu sync.RWMutex
	peers   map[uint64]*peerConnection

	// Channel for reporting unreachable peers
	unreachableCh chan uint64

	logger        logging.Logger
	globalMeter   metric.Meter
	meterProvider metric.MeterProvider
	config        TransportConfig
	nodeID        uint64
	clusterID     string
	// fsmDeterminismEnabled is this node's fsm-determinism-enabled flag,
	// advertised on every outgoing Raft stream and validated against the
	// peer's on every incoming one (StreamMessages). See MetadataKeyFSMDeterminism.
	fsmDeterminismEnabled bool

	bufferSize           int
	pendingSendQueue     chan []raftpb.Message
	stopCh               chan chan struct{}
	advertiseAddr        string
	serviceAdvertiseAddr string
	// Metrics for recv queues (indexed by priority: 0=high, 1=medium, 2=low)
	recvQueueLoadHistogram [3]metric.Int64Histogram
	recvQueueFullCounter   [3]metric.Float64Counter
	recvQueueInflight      [3]atomic.Int32

	// Metrics for unreachable queue
	unreachableLoadHistogram metric.Int64Histogram
	unreachableFullCounter   metric.Float64Counter
	unreachableInflight      atomic.Int32

	// Metrics for pending send queue
	pendingSendLoadHistogram metric.Int64Histogram
	pendingSendFullCounter   metric.Float64Counter
	pendingSendInflight      atomic.Int32

	// stopped is set at the beginning of Stop() to guard channel sends
	// from orphaned goroutines that outlive the transport shutdown.
	stopped atomic.Bool
}

func (t *DefaultTransport) RecvHighPriority() <-chan []raftpb.Message {
	return t.highPriorityRecvCh
}

func (t *DefaultTransport) RecvMediumPriority() <-chan []raftpb.Message {
	return t.mediumPriorityRecvCh
}

func (t *DefaultTransport) RecvLowPriority() <-chan []raftpb.Message {
	return t.lowPriorityRecvCh
}

type TransportConfig struct {
	Reception []int
	Send      []int
}

// Validate checks that Reception and Send hold the three priority slots the
// transport reads at startup, that each capacity is non-negative, and that
// no value is large enough to OOM the channel allocator.
func (c TransportConfig) Validate() error {
	for _, side := range []struct {
		name   string
		queues []int
	}{
		{"raft-transport-reception-queues", c.Reception},
		{"raft-transport-send-queues", c.Send},
	} {
		if len(side.queues) != 3 {
			return fmt.Errorf("--%s must have exactly 3 entries (high/medium/low priority); got %d",
				side.name, len(side.queues))
		}

		for i, v := range side.queues {
			if v < 0 || v > maxQueueCapacity {
				return fmt.Errorf("--%s[%d] must be in [0, %d] (got %d)",
					side.name, i, maxQueueCapacity, v)
			}
		}
	}

	return nil
}

// NewTransport creates a new transport with a gRPC connection pool and client pool.
func NewTransport(
	logger logging.Logger,
	connectionPool *transport.ConnectionPool,
	meterProvider metric.MeterProvider,
	nodeID uint64,
	config TransportConfig,
	clusterID string,
	bufferSize int,
	advertiseAddr string,
	serviceAdvertiseAddr string,
	fsmDeterminismEnabled bool,
) *DefaultTransport {
	meter := meterProvider.Meter("raft.transport")

	const (
		unreachableCapacity = 100
		pendingSendCapacity = 100
	)

	t := &DefaultTransport{
		connectionPool:        connectionPool,
		highPriorityRecvCh:    make(chan []raftpb.Message, config.Reception[0]),
		mediumPriorityRecvCh:  make(chan []raftpb.Message, config.Reception[1]),
		lowPriorityRecvCh:     make(chan []raftpb.Message, config.Reception[2]),
		peers:                 make(map[uint64]*peerConnection),
		unreachableCh:         make(chan uint64, unreachableCapacity),
		globalMeter:           meter,
		meterProvider:         meterProvider,
		logger:                logger,
		config:                config,
		nodeID:                nodeID,
		clusterID:             clusterID,
		fsmDeterminismEnabled: fsmDeterminismEnabled,
		bufferSize:            bufferSize,
		stopCh:                make(chan chan struct{}),
		pendingSendQueue:      make(chan []raftpb.Message, pendingSendCapacity),
		advertiseAddr:         advertiseAddr,
		serviceAdvertiseAddr:  serviceAdvertiseAddr,
	}

	// Initialize recv queue metrics for each priority level
	priorityNames := []string{"high", "medium", "low"}
	for priority, name := range priorityNames {
		m := meterProvider.Meter("raft.transport", metric.WithInstrumentationAttributes(
			attribute.Int("priority", priority),
			attribute.String("priority_name", name),
		))

		var err error

		t.recvQueueFullCounter[priority], err = m.Float64Counter("raft.transport.recv.full", metric.WithUnit("1"))
		if err != nil {
			panic(err)
		}

		t.recvQueueLoadHistogram[priority], err = m.Int64Histogram(
			"raft.transport.recv.load",
			metric.WithUnit("1"),
			metric.WithExplicitBucketBoundaries(expBoundaries(12, config.Reception[priority])...),
		)
		if err != nil {
			panic(err)
		}
	}

	// Initialize unreachable queue metrics
	var err error

	t.unreachableFullCounter, err = meter.Float64Counter("raft.transport.unreachable.full", metric.WithUnit("1"))
	if err != nil {
		panic(err)
	}

	t.unreachableLoadHistogram, err = meter.Int64Histogram(
		"raft.transport.unreachable.load",
		metric.WithUnit("1"),
		metric.WithExplicitBucketBoundaries(expBoundaries(12, unreachableCapacity)...),
	)
	if err != nil {
		panic(err)
	}

	// Initialize pending send queue metrics
	t.pendingSendFullCounter, err = meter.Float64Counter("raft.send.pending_messages.full", metric.WithUnit("1"))
	if err != nil {
		panic(err)
	}

	t.pendingSendLoadHistogram, err = meter.Int64Histogram(
		"raft.send.pending_messages.load",
		metric.WithUnit("1"),
		metric.WithExplicitBucketBoundaries(expBoundaries(12, pendingSendCapacity)...),
	)
	if err != nil {
		panic(err)
	}

	return t
}

// pushToRecvQueue pushes a batch of messages to the appropriate priority recv queue.
func (t *DefaultTransport) pushToRecvQueue(priority int, msgs []raftpb.Message) bool {
	if len(msgs) == 0 {
		return true
	}

	if t.stopped.Load() {
		return false
	}

	var queue chan []raftpb.Message

	switch priority {
	case 0: // high
		queue = t.highPriorityRecvCh
	case 1: // medium
		queue = t.mediumPriorityRecvCh
	default: // low
		priority = 2
		queue = t.lowPriorityRecvCh
	}

	select {
	case queue <- msgs:
		t.recvQueueLoadHistogram[priority].Record(context.Background(), int64(t.recvQueueInflight[priority].Add(1)))

		return true
	default:
		t.logger.WithFields(map[string]any{
			"channel":  "raft.transport.recv",
			"priority": priority,
		}).Errorf("Channel full")
		t.recvQueueFullCounter[priority].Add(context.Background(), 1)

		return false
	}
}

// CancelPeerConnections cancels all peer reconnection loops immediately.
// Safe to call before Stop(). This allows early cancellation during service
// shutdown so reconnection loops don't block the shutdown timeout.
func (t *DefaultTransport) CancelPeerConnections() {
	t.peersMu.RLock()
	defer t.peersMu.RUnlock()

	for _, pc := range t.peers {
		pc.stopCancel()
	}
}

// Stop stops the transport.
func (t *DefaultTransport) Stop(ctx context.Context) error {
	t.logger.Infof("Stopping raft transport")

	// Mark as stopped so orphaned goroutines skip channel sends.
	t.stopped.Store(true)

	// Cancel all peer reconnection loops upfront so pc.stop() below
	// returns instantly (loops already exited).
	t.CancelPeerConnections()

	stopCh := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case t.stopCh <- stopCh:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-stopCh:
		}
	}

	t.peersMu.RLock()
	peersSnapshot := make([]*peerConnection, 0, len(t.peers))
	for _, pc := range t.peers {
		peersSnapshot = append(peersSnapshot, pc)
	}
	t.peersMu.RUnlock()

	for _, pc := range peersSnapshot {
		err := pc.stop(ctx)
		if err != nil {
			return err
		}
	}

	// Note: we intentionally do NOT close the recv/unreachable/pendingSend
	// channels here. Orphaned goroutines may still attempt sends after Stop()
	// returns; the stopped flag guards those sends. The channels will be GC'd
	// with the struct. Consumers have their own shutdown path.

	return t.connectionPool.Close()
}

// AddPeer adds a peer to the transport. If the peer already exists, it is a no-op.
func (t *DefaultTransport) AddPeer(id uint64, addr string) {
	t.peersMu.Lock()
	defer t.peersMu.Unlock()

	if _, exists := t.peers[id]; exists {
		return
	}

	if err := t.connectionPool.AddPeer(id, addr); err != nil {
		t.logger.WithFields(map[string]any{"peer": strconv.FormatUint(id, 16), "addr": addr, "error": err}).Errorf("Failed to add peer to client pool")

		return
	}

	meter := t.meterProvider.Meter("raft.transport",
		metric.WithInstrumentationAttributes(
			attribute.Int("peer", int(id)),
		),
	)
	logger := t.logger.WithFields(map[string]any{"peer": strconv.FormatUint(id, 16)})

	pendingResponseCounter, err := meter.Float64UpDownCounter("raft.transport.sending.pending_response")
	if err != nil {
		panic(err)
	}

	pingLatency, err := meter.Int64Histogram("raft.transport.ping.latency", metric.WithUnit("microseconds"))
	if err != nil {
		panic(err)
	}

	stopCtx, stopCancel := context.WithCancel(context.Background())

	conn := &peerConnection{
		highPriorityCh:         make(chan []raftpb.Message, t.config.Send[0]),
		mediumPriorityCh:       make(chan []raftpb.Message, t.config.Send[1]),
		lowPriorityCh:          make(chan []raftpb.Message, t.config.Send[2]),
		closeCh:                make(chan chan struct{}),
		stopCtx:                stopCtx,
		stopCancel:             stopCancel,
		loopDone:               make(chan struct{}),
		pushUnreachable:        t.pushUnreachable,
		connectionPool:         t.connectionPool,
		logger:                 logger,
		peerID:                 id,
		nodeID:                 t.nodeID,
		clusterID:              t.clusterID,
		fsmDeterminismEnabled:  t.fsmDeterminismEnabled,
		bufferSize:             t.bufferSize,
		pendingResponseCounter: pendingResponseCounter,
		pingLatency:            pingLatency,
		reconnected:            make(chan struct{}),
		advertiseAddr:          t.advertiseAddr,
		serviceAdvertiseAddr:   t.serviceAdvertiseAddr,
	}

	// Initialize send queue metrics for each priority level
	priorityNames := []string{"high", "medium", "low"}
	for priority, name := range priorityNames {
		m := t.meterProvider.Meter("raft.transport",
			metric.WithInstrumentationAttributes(
				attribute.Int("peer", int(id)),
				attribute.Int("priority", priority),
				attribute.String("priority_name", name),
			),
		)

		conn.sendQueueFullCounter[priority], err = m.Float64Counter("raft.transport.peer.sending.full", metric.WithUnit("1"))
		if err != nil {
			panic(err)
		}

		conn.sendQueueLoadHistogram[priority], err = m.Int64Histogram(
			"raft.transport.peer.sending.load",
			metric.WithUnit("1"),
			metric.WithExplicitBucketBoundaries(expBoundaries(12, t.config.Send[priority])...),
		)
		if err != nil {
			panic(err)
		}
	}

	t.peers[id] = conn

	go conn.loop()
}

// RemovePeer removes a peer from the transport, stopping its connection and cleaning up resources.
func (t *DefaultTransport) RemovePeer(ctx context.Context, id uint64) {
	t.peersMu.Lock()
	conn, exists := t.peers[id]
	if !exists {
		t.peersMu.Unlock()

		return
	}
	delete(t.peers, id)
	t.peersMu.Unlock()

	err := conn.stop(ctx)
	if err != nil {
		t.logger.WithFields(map[string]any{"peer": strconv.FormatUint(id, 16), "error": err}).
			Errorf("Failed to stop peer connection")
	}

	err = t.connectionPool.RemovePeer(id)
	if err != nil {
		t.logger.WithFields(map[string]any{"peer": strconv.FormatUint(id, 16), "error": err}).
			Errorf("Failed to remove peer from connection pool")
	}
}

// Send sends a message to a peer.
func (t *DefaultTransport) Send(msgs []raftpb.Message) {
	if len(msgs) == 0 {
		return
	}

	if t.stopped.Load() {
		return
	}

	select {
	case t.pendingSendQueue <- msgs:
		t.pendingSendLoadHistogram.Record(context.Background(), int64(t.pendingSendInflight.Add(1)))
	default:
		t.logger.WithFields(map[string]any{
			"channel": "raft.send.pending_messages",
		}).Errorf("Channel full")
		t.pendingSendFullCounter.Add(context.Background(), 1)

		// Signal Unreachable for every peer that had a message in the
		// dropped batch. pendingSendQueue is shared across peers, so we
		// can't blame a single one — but transitioning every affected
		// peer's Progress to StateProbe throttles overall outgoing load,
		// which is the correct systemic response to a full top-level
		// queue.
		reported := make(map[uint64]struct{}, len(msgs))
		for _, m := range msgs {
			if _, ok := reported[m.To]; ok {
				continue
			}
			reported[m.To] = struct{}{}
			t.pushUnreachable(m.To)
		}
	}
}

// messagePriority returns the priority level for a raft message type.
func messagePriority(msgType raftpb.MessageType) int {
	switch msgType {
	case raftpb.MsgHeartbeat, raftpb.MsgHeartbeatResp:
		return 0 // high
	case raftpb.MsgAppResp, raftpb.MsgVote, raftpb.MsgVoteResp, raftpb.MsgPreVote, raftpb.MsgPreVoteResp:
		return 1 // medium
	default:
		return 2 // low
	}
}

func (t *DefaultTransport) Start(_ context.Context) {
	for {
		select {
		case ch := <-t.stopCh:
			close(ch)

			return
		case msgs := <-t.pendingSendQueue:
			t.pendingSendInflight.Add(-1)

			// Group messages by peer and priority
			// Key: peerID, Value: map of priority -> messages
			msgsByPeerAndPriority := make(map[uint64]map[int][]raftpb.Message)

			for _, msg := range msgs {
				if _, exists := msgsByPeerAndPriority[msg.To]; !exists {
					msgsByPeerAndPriority[msg.To] = make(map[int][]raftpb.Message)
				}

				priority := messagePriority(msg.Type)
				msgsByPeerAndPriority[msg.To][priority] = append(msgsByPeerAndPriority[msg.To][priority], msg)
			}

			// Push batches to each peer's priority queues
			for peerID, priorityMsgs := range msgsByPeerAndPriority {
				t.peersMu.RLock()
				peer, exists := t.peers[peerID]
				t.peersMu.RUnlock()

				if !exists {
					t.logger.
						WithFields(map[string]any{
							"peer": strconv.FormatUint(peerID, 16),
						}).
						Errorf("No send channel for peer, dropping messages")

					continue
				}

				for priority, batch := range priorityMsgs {
					if !peer.pushMessages(priority, batch) {
						t.logger.
							WithFields(map[string]any{
								"peer":     strconv.FormatUint(peerID, 16),
								"priority": priority,
								"count":    len(batch),
							}).
							Errorf("Send channel full, dropping messages")
					}
				}
			}
		}
	}
}

// Unreachable returns the channel for reporting unreachable peers.
func (t *DefaultTransport) Unreachable() <-chan uint64 {
	return t.unreachableCh
}

// pushUnreachable pushes a peer ID to the unreachable queue with metrics.
func (t *DefaultTransport) pushUnreachable(peerID uint64) bool {
	if t.stopped.Load() {
		return false
	}

	select {
	case t.unreachableCh <- peerID:
		t.unreachableLoadHistogram.Record(context.Background(), int64(t.unreachableInflight.Add(1)))

		return true
	default:
		t.logger.WithFields(map[string]any{
			"channel": "raft.transport.unreachable",
		}).Errorf("Channel full")
		t.unreachableFullCounter.Add(context.Background(), 1)

		return false
	}
}

// GetPeerConnection returns the gRPC connection for a specific peer, if it exists
// This allows reusing existing connections for service calls instead of creating new ones.
func (t *DefaultTransport) GetPeerConnection(peerID uint64) *grpc.ClientConn {
	return t.connectionPool.GetConnection(peerID)
}

// GetPeerAddress returns the address for a specific peer, if it exists.
func (t *DefaultTransport) GetPeerAddress(peerID uint64) string {
	return t.connectionPool.GetPeerAddress(peerID)
}

// HandleStreamMessages handles client streaming gRPC connection for receiving messages
// This maintains a persistent connection to avoid frequent reconnections
// The server receives all messages and sends a single response at the end.
func (t *DefaultTransport) StreamMessages(stream grpc.BidiStreamingServer[rafttransportpb.SendMessageRequest, rafttransportpb.SendMessageResponse]) error {
	nodeIDStr := metadata.ValueFromIncomingContext(stream.Context(), MetadataKeyNodeID)
	if len(nodeIDStr) == 0 {
		return errors.New("nodeID metadata not found in context")
	}

	peerID, err := strconv.ParseUint(nodeIDStr[0], 16, 64)
	if err != nil {
		return fmt.Errorf("failed to decode nodeID from metadata: %w", err)
	}

	// Validate cluster ID if configured
	if t.clusterID != "" {
		clusterIDStr := metadata.ValueFromIncomingContext(stream.Context(), MetadataKeyClusterID)
		if len(clusterIDStr) == 0 || clusterIDStr[0] != t.clusterID {
			return status.Errorf(codes.PermissionDenied, "invalid cluster ID")
		}
	}

	// Cross-peer fsm-determinism-enabled consistency. Every peer establishes a
	// Raft stream to every other peer, so this gate covers the STATIC BOOTSTRAP
	// path that the JoinAsLearner check cannot (seed nodes never call
	// JoinAsLearner). Rejecting the stream here fails the cluster fast rather
	// than letting a divergent seed encode/hash the FSM differently and surface
	// as perpetual (false) digest divergence at runtime. An absent value means
	// a peer built before this flag existed — treated as false, so a uniformly
	// old (all-OFF) cluster still connects while a mixed one is refused.
	peerFSMDeterminism := false
	if v := metadata.ValueFromIncomingContext(stream.Context(), MetadataKeyFSMDeterminism); len(v) > 0 {
		peerFSMDeterminism = v[0] == "true"
	}

	if peerFSMDeterminism != t.fsmDeterminismEnabled {
		t.logger.WithFields(map[string]any{
			"peerID":    peerID,
			"peerFlag":  peerFSMDeterminism,
			"localFlag": t.fsmDeterminismEnabled,
		}).Errorf("Refusing Raft stream from peer with mismatched fsm-determinism-enabled")

		return status.Errorf(codes.FailedPrecondition,
			"fsm-determinism-enabled mismatch: peer %x has %t but this node runs with %t; "+
				"every peer must set --fsm-determinism-enabled identically",
			peerID, peerFSMDeterminism, t.fsmDeterminismEnabled)
	}

	priorityStr := metadata.ValueFromIncomingContext(stream.Context(), MetadataKeyPriority)
	if len(priorityStr) == 0 {
		return errors.New("priority metadata not found in context")
	}

	priority := priorityStr[0]

	t.logger.Infof("Peer %x connected on %s priority stream!", peerID, priority)

	// Best effort to notify the send loop that the peer is now reachable
	t.peersMu.RLock()
	peer, ok := t.peers[peerID]
	t.peersMu.RUnlock()
	if ok {
		select {
		case peer.reconnected <- struct{}{}:
		default:
		}
	}

	// Receive all messages from the stream
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		switch m := req.GetMessage().(type) {
		case *rafttransportpb.SendMessageRequest_Ping:
			err := stream.Send(&rafttransportpb.SendMessageResponse{
				Message: &rafttransportpb.SendMessageResponse_Pong{
					Pong: &rafttransportpb.PongResponse{
						SeqId: m.Ping.GetSeqId(),
					},
				},
			})
			if err != nil {
				return err
			}
		case *rafttransportpb.SendMessageRequest_Raft:
			// Group messages by priority, tracking request IDs per group.
			type msgGroup struct {
				msgs       []raftpb.Message
				requestIDs []uint64
			}

			groups := [3]msgGroup{} // 0=high, 1=medium, 2=low
			responses := make([]*rafttransportpb.RaftResponseMessage, 0, len(m.Raft.GetMessages()))

			for _, raftMsg := range m.Raft.GetMessages() {
				var msg raftpb.Message

				err := msg.Unmarshal(raftMsg.GetMessage())
				if err != nil {
					responses = append(responses, &rafttransportpb.RaftResponseMessage{
						Error:     fmt.Sprintf("failed to unmarshal message: %v", err),
						RequestId: raftMsg.GetId(),
					})

					continue
				}

				var pri int

				switch msg.Type {
				case raftpb.MsgHeartbeat, raftpb.MsgHeartbeatResp:
					pri = 0
				case raftpb.MsgAppResp, raftpb.MsgVote, raftpb.MsgVoteResp, raftpb.MsgPreVote, raftpb.MsgPreVoteResp:
					pri = 1
				default:
					pri = 2
				}

				groups[pri].msgs = append(groups[pri].msgs, msg)
				groups[pri].requestIDs = append(groups[pri].requestIDs, raftMsg.GetId())
			}

			// Push batches to priority queues and build responses
			// based on whether each group was successfully enqueued.
			for pri := range groups {
				success := t.pushToRecvQueue(pri, groups[pri].msgs)

				for _, reqID := range groups[pri].requestIDs {
					responses = append(responses, &rafttransportpb.RaftResponseMessage{
						Success:   success,
						RequestId: reqID,
					})
				}
			}

			err := stream.Send(&rafttransportpb.SendMessageResponse{
				Message: &rafttransportpb.SendMessageResponse_Raft{
					Raft: &rafttransportpb.RaftResponseBatch{
						Messages: responses,
					},
				},
			})
			if err != nil {
				t.logger.Errorf("Failed to send response to peer: %v", err)
			}
		}
	}
}

// RegisterRaftTransportService registers the RaftTransportService on the given gRPC service registrar.
func RegisterRaftTransportService(registrar grpc.ServiceRegistrar, transport *DefaultTransport) {
	transport.RegisterRaftService(registrar)
}

// RegisterRaftService registers the RaftTransportService on the given gRPC service registrar.
func (t *DefaultTransport) RegisterRaftService(registrar grpc.ServiceRegistrar) {
	rafttransportpb.RegisterRaftTransportServiceServer(registrar, t)
}

type peerConnection struct {
	// 3 priority queues for sending batches of messages (high to low priority)
	highPriorityCh   chan []raftpb.Message // Heartbeats
	mediumPriorityCh chan []raftpb.Message // Votes, responses
	lowPriorityCh    chan []raftpb.Message // Data messages (MsgApp with entries)

	closeCh                chan chan struct{}
	stopCtx                context.Context    // cancelled by stop() to interrupt stream creation and Recv() calls
	stopCancel             context.CancelFunc // called by stop()
	loopDone               chan struct{}      // closed when loop() exits; stop() waits on this
	pushUnreachable        func(peerID uint64) bool
	connectionPool         *transport.ConnectionPool
	logger                 logging.Logger
	peerID                 uint64
	nodeID                 uint64
	clusterID              string
	fsmDeterminismEnabled  bool
	bufferSize             int
	pendingResponseCounter metric.Float64UpDownCounter
	pingLatency            metric.Int64Histogram
	reconnected            chan struct{}
	messageID              uint64
	buf                    []byte
	advertiseAddr          string
	serviceAdvertiseAddr   string

	// Metrics for sending queues (indexed by priority: 0=high, 1=medium, 2=low)
	sendQueueLoadHistogram [3]metric.Int64Histogram
	sendQueueFullCounter   [3]metric.Float64Counter
	sendQueueInflight      [3]atomic.Int32

	// pubMu guards stopped vs. send-into-queue. Publishers (pushMessages)
	// hold pubMu.RLock for the duration of the send so stop() — which takes
	// pubMu.Lock before flipping stopped and closing the queues — observes
	// a quiescent state at the moment of close.
	pubMu   sync.RWMutex
	stopped bool

	// unreachableReported dedups Unreachable notifications during a drop
	// burst so we don't saturate unreachableCh (capacity 100) when a single
	// full send channel fires pushMessages hundreds of times per second.
	// CAS-set on first drop, reset by sendMessages after a successful
	// stream.Send() (peer confirmed responsive → next drop re-signals Raft).
	unreachableReported atomic.Bool
}

// pushMessages pushes a batch of messages to the specified priority queue.
//
// Concurrency note: pushMessages is called by DefaultTransport.Start after it
// snapshots *peerConnection from t.peers under peersMu.RLock and releases
// that lock. RemovePeer can be racing against this send. To make the close
// in stop() safe (the prior never-close policy was forced by the absence of
// publisher coordination, see #315), we acquire pubMu.RLock for the
// duration of the send. stop() acquires pubMu.Lock before flipping stopped
// and closing the queues, which guarantees no in-flight send overlaps the
// close.
func (conn *peerConnection) pushMessages(priority int, msgs []raftpb.Message) bool {
	if len(msgs) == 0 {
		return true
	}

	conn.pubMu.RLock()
	defer conn.pubMu.RUnlock()

	if conn.stopped {
		return false
	}

	var queue chan []raftpb.Message

	switch priority {
	case 0:
		queue = conn.highPriorityCh
	case 1:
		queue = conn.mediumPriorityCh
	default:
		priority = 2
		queue = conn.lowPriorityCh
	}

	select {
	case queue <- msgs:
		conn.sendQueueLoadHistogram[priority].Record(context.Background(), int64(conn.sendQueueInflight[priority].Add(1)))

		return true
	default:
		conn.logger.WithFields(map[string]any{
			"channel":  "raft.transport.peer.sending",
			"priority": priority,
		}).Errorf("Channel full")
		conn.sendQueueFullCounter[priority].Add(context.Background(), 1)

		// Signal Unreachable to Raft so it throttles this peer's Progress
		// to StateProbe instead of continuing optimistic replication.
		// Dedup via CAS: sendMessages resets the flag on the next successful
		// stream.Send(), letting subsequent drops re-signal if the channel
		// keeps refilling. If pushUnreachable itself fails because
		// unreachableCh is full, roll the flag back so a subsequent drop
		// can retry the emit — otherwise a transient unreachableCh overflow
		// permanently silences this peer's signal until sendMessages
		// succeeds, which is the exact stuck-peer case we want to avoid.
		if conn.unreachableReported.CompareAndSwap(false, true) {
			if !conn.pushUnreachable(conn.peerID) {
				conn.unreachableReported.Store(false)
			}
		}

		return false
	}
}

func (conn *peerConnection) loop() {
	defer close(conn.loopDone)
	defer otlplogs.RecoverAndLogPanics(conn.logger)

	conn.buf = make([]byte, 0, conn.bufferSize)

	for {
		select {
		case <-conn.stopCtx.Done():
			return
		case ch := <-conn.closeCh:
			close(ch)

			return
		default:
		}

		conn.logger.Infof("Creating stream to peer %x...", conn.peerID)

		grpcPeerConnection := conn.connectionPool.GetConnection(conn.peerID)
		if grpcPeerConnection == nil {
			conn.logger.Errorf("No gRPC connection for peer %x, peer may have been removed", conn.peerID)

			return
		}

		stopped, err := conn.handleConnection(grpcPeerConnection)
		if stopped {
			return
		}

		if err != nil {
			conn.logger.
				WithFields(map[string]any{
					"error": err,
				}).
				Errorf("Failed to create stream to peer")
			// Report peer as unreachable
			if !conn.pushUnreachable(conn.peerID) {
				conn.logger.Errorf("Unreachable channel full, dropping unreachable")
			}

			// Wait before retrying
			//todo: make configurable
			waitingDelayBeforeReconnect := time.Now().Add(time.Second)

		drainLoop:
			for {
				select {
				case <-conn.stopCtx.Done():
					return
				case ch := <-conn.closeCh:
					close(ch)

					return
				case <-conn.highPriorityCh:
					conn.sendQueueInflight[0].Add(-1)
					conn.pushUnreachable(conn.peerID)
				case <-conn.mediumPriorityCh:
					conn.sendQueueInflight[1].Add(-1)
					conn.pushUnreachable(conn.peerID)
				case <-conn.lowPriorityCh:
					conn.sendQueueInflight[2].Add(-1)
					conn.pushUnreachable(conn.peerID)
				case <-conn.reconnected:
					// restart connection to prevent staled dns cached ip
					conn.logger.Infof("Restarting connection to peer %x...", conn.peerID)

					err := conn.connectionPool.RestartConnection(conn.peerID)
					if err != nil {
						conn.logger.Errorf("Failed to restart connection to peer: %v", err)
					}

					break drainLoop
				case <-time.After(time.Until(waitingDelayBeforeReconnect)):
					conn.logger.Infof("Restarting connection to peer %x...", conn.peerID)

					err := conn.connectionPool.RestartConnection(conn.peerID)
					if err != nil {
						conn.logger.Errorf("Failed to restart connection to peer: %v", err)
					}

					break drainLoop
				}
			}

			continue
		}
	}
}

// priorityStream represents a stream dedicated to a specific priority level.
type priorityStream struct {
	stream       grpc.BidiStreamingClient[rafttransportpb.SendMessageRequest, rafttransportpb.SendMessageResponse]
	priorityName string
}

func (conn *peerConnection) handleConnection(grpcPeerConnection *grpc.ClientConn) (bool, error) {
	client := rafttransportpb.NewRaftTransportServiceClient(grpcPeerConnection)

	// Use a cancelable context derived from conn.stopCtx so that:
	// 1. stop() can interrupt stream creation (via stopCancel)
	// 2. cancelling streamCtx unblocks all stream.Recv() calls in receive goroutines
	streamCtx, streamCancel := context.WithCancel(conn.stopCtx)
	defer streamCancel() // ensure cleanup on all early-return paths (e.g. createStream failure)

	// Create a stream for each priority level
	createStream := func(priorityName string) (grpc.BidiStreamingClient[rafttransportpb.SendMessageRequest, rafttransportpb.SendMessageResponse], error) {
		return client.StreamMessages(
			metadata.NewOutgoingContext(streamCtx, metadata.New(map[string]string{
				MetadataKeyNodeID:         strconv.FormatUint(conn.nodeID, 16),
				MetadataKeyPriority:       priorityName,
				MetadataKeyClusterID:      conn.clusterID,
				MetadataKeyFSMDeterminism: strconv.FormatBool(conn.fsmDeterminismEnabled),
				"advertiseAddr":           conn.advertiseAddr,
				"serviceAddr":             conn.serviceAdvertiseAddr,
			})),
		)
	}

	highStream, err := createStream("high")
	if err != nil {
		return false, fmt.Errorf("failed to create high priority stream: %w", err)
	}

	defer func() { _ = highStream.CloseSend() }()

	mediumStream, err := createStream("medium")
	if err != nil {
		return false, fmt.Errorf("failed to create medium priority stream: %w", err)
	}

	defer func() { _ = mediumStream.CloseSend() }()

	lowStream, err := createStream("low")
	if err != nil {
		return false, fmt.Errorf("failed to create low priority stream: %w", err)
	}

	defer func() { _ = lowStream.CloseSend() }()

	conn.logger.Infof("Created 3 priority streams to peer")

	type ping struct {
		at    time.Time
		seqId uint64
	}

	pending := make(map[uint64]uint64)
	lastPing := atomic.Value{}
	mu := sync.Mutex{}

	// drainPendingUnreachable marks all pending peers as unreachable under the lock.
	// Used on stream/send errors before returning from handleConnection.
	drainPendingUnreachable := func() {
		mu.Lock()
		for _, peerID := range pending {
			conn.pushUnreachable(peerID)
		}
		mu.Unlock()
	}

	defer func() {
		mu.Lock()
		orphaned := len(pending)
		mu.Unlock()

		if orphaned > 0 {
			conn.pendingResponseCounter.Add(context.Background(), -float64(orphaned))
			conn.logger.WithFields(map[string]any{
				"orphanedMessages": orphaned,
			}).Infof("Cleaned up orphaned pending responses on connection close")
		}
	}()

	// Create a channel to signal when all receive goroutines have stopped
	var receiveWg sync.WaitGroup

	// Start a receive goroutine for each stream
	streams := []priorityStream{
		{stream: highStream, priorityName: "high"},
		{stream: mediumStream, priorityName: "medium"},
		{stream: lowStream, priorityName: "low"},
	}

	streamErrors := make(chan error, 3)

	// Ensure all receive goroutines have exited before handleConnection returns.
	// streamCancel() must run BEFORE receiveWg.Wait() to unblock Recv() calls.
	// Combined in a single defer to guarantee correct ordering (LIFO would
	// otherwise execute Wait before Cancel, causing a deadlock).
	defer func() {
		streamCancel()
		receiveWg.Wait()
	}()

	for _, ps := range streams {
		receiveWg.Add(1)

		go func(ps priorityStream) {
			defer receiveWg.Done()

			for {
				res, err := ps.stream.Recv()
				if err != nil {
					streamErrors <- err

					return
				}

				switch msg := res.GetMessage().(type) {
				case *rafttransportpb.SendMessageResponse_Pong:
					// Only high priority stream handles pings
					lastPingVal := lastPing.Load()
					if lastPingVal == nil {
						continue
					}

					lp := lastPingVal.(ping)
					if msg.Pong.GetSeqId() != lp.seqId {
						conn.logger.
							WithFields(map[string]any{
								"expected-seq-id": lp.seqId,
								"received-seq-id": msg.Pong.GetSeqId(),
							}).
							Errorf("Received unexpected ping response from peer")

						continue
					}

					conn.pingLatency.Record(context.Background(), time.Since(lp.at).Microseconds())

				case *rafttransportpb.SendMessageResponse_Raft:
					mu.Lock()
					for _, raftResp := range msg.Raft.GetMessages() {
						nodeID, ok := pending[raftResp.GetRequestId()]
						if ok {
							delete(pending, raftResp.GetRequestId())
							conn.pendingResponseCounter.Add(context.Background(), -1)
						} else {
							conn.logger.
								WithFields(map[string]any{
									"request-id": raftResp.GetRequestId(),
									"priority":   ps.priorityName,
								}).
								Errorf("Received unexpected response from peer")
						}

						if !raftResp.GetSuccess() && ok {
							conn.logger.
								Errorf("Failed to send message on %s stream, peer respond with error: %s", ps.priorityName, raftResp.GetError())
							conn.pushUnreachable(nodeID)
						}
					}
					mu.Unlock()
				default:
					panic(fmt.Sprintf("received unexpected message type: %T", msg))
				}
			}
		}(ps)
	}

	pingInterval := time.NewTicker(time.Second)
	opts := proto.MarshalOptions{}

	// sendMessages handles sending a batch of raft messages on the specified stream
	sendMessages := func(stream grpc.BidiStreamingClient[rafttransportpb.SendMessageRequest, rafttransportpb.SendMessageResponse], msgs []raftpb.Message) error {
		if len(msgs) == 0 {
			return nil
		}

		raftMessages := make([]*rafttransportpb.RaftRequestMessage, 0, len(msgs))
		messageIDs := make([]uint64, 0, len(msgs))

		mu.Lock()
		for _, msg := range msgs {
			data, err := opts.MarshalAppend(conn.buf[:0], protoadapt.MessageV2Of(&msg))
			if err != nil {
				conn.logger.
					WithFields(map[string]any{
						"error": err,
					}).
					Errorf("Failed to marshal message")

				continue
			}

			// Copy data since we reuse the buffer
			dataCopy := make([]byte, len(data))
			copy(dataCopy, data)

			currentMessageID := conn.messageID
			conn.messageID++
			pending[currentMessageID] = msg.To
			messageIDs = append(messageIDs, currentMessageID)

			raftMessages = append(raftMessages, &rafttransportpb.RaftRequestMessage{
				Message: dataCopy,
				Id:      currentMessageID,
			})
		}
		mu.Unlock()

		if len(raftMessages) == 0 {
			return nil
		}

		conn.pendingResponseCounter.Add(context.Background(), float64(len(raftMessages)))

		err := stream.Send(&rafttransportpb.SendMessageRequest{
			Message: &rafttransportpb.SendMessageRequest_Raft{
				Raft: &rafttransportpb.RaftRequestBatch{
					Messages: raftMessages,
				},
			},
		})
		if err != nil {
			conn.logger.
				WithFields(map[string]any{
					"error": err,
					"count": len(raftMessages),
				}).
				Errorf("Failed to send batch via stream")
			mu.Lock()
			for _, msgID := range messageIDs {
				delete(pending, msgID)
			}
			mu.Unlock()
			conn.pendingResponseCounter.Add(context.Background(), -float64(len(raftMessages)))
			// Report peer as unreachable
			if !conn.pushUnreachable(conn.peerID) {
				conn.logger.Errorf("Unreachable channel full, dropping unreachable")
			}

			return err
		}

		// Peer accepted the write — clear the dedup flag so a subsequent
		// pushMessages drop re-signals Unreachable to Raft (Progress
		// currently in Replicate can transition back to Probe if we drop
		// again).
		conn.unreachableReported.Store(false)

		return nil
	}

	for {
		// First, try non-blocking receives in priority order (high -> medium -> low)
		select {
		case msgs := <-conn.highPriorityCh:
			conn.sendQueueInflight[0].Add(-1)

			err := sendMessages(highStream, msgs)
			if err != nil {
				drainPendingUnreachable()

				return false, err
			}

			continue
		default:
		}

		select {
		case msgs := <-conn.mediumPriorityCh:
			conn.sendQueueInflight[1].Add(-1)

			err := sendMessages(mediumStream, msgs)
			if err != nil {
				drainPendingUnreachable()

				return false, err
			}

			continue
		default:
		}

		select {
		case msgs := <-conn.lowPriorityCh:
			conn.sendQueueInflight[2].Add(-1)

			err := sendMessages(lowStream, msgs)
			if err != nil {
				drainPendingUnreachable()

				return false, err
			}

			continue
		default:
		}

		// No messages available, do a blocking select on all channels
		select {
		case ch := <-conn.closeCh:
			// streamCancel() + receiveWg.Wait() are handled by defers.
			close(ch)

			return true, nil
		case err := <-streamErrors:
			drainPendingUnreachable()

			conn.logger.Errorf("Stream error: %v", err)

			return false, err
		case <-pingInterval.C:
			p := ping{
				at:    time.Now(),
				seqId: conn.messageID,
			}
			lastPing.Store(p)
			// Send ping on high priority stream
			err := highStream.Send(&rafttransportpb.SendMessageRequest{
				Message: &rafttransportpb.SendMessageRequest_Ping{
					Ping: &rafttransportpb.PingMessage{
						SeqId: p.seqId,
					},
				},
			})
			if err != nil {
				drainPendingUnreachable()

				conn.logger.Errorf("Failed to send ping to peer: %v", err)

				return false, err
			}
		case msgs := <-conn.highPriorityCh:
			conn.sendQueueInflight[0].Add(-1)

			err := sendMessages(highStream, msgs)
			if err != nil {
				drainPendingUnreachable()

				return false, err
			}
		case msgs := <-conn.mediumPriorityCh:
			conn.sendQueueInflight[1].Add(-1)

			err := sendMessages(mediumStream, msgs)
			if err != nil {
				drainPendingUnreachable()

				return false, err
			}
		case msgs := <-conn.lowPriorityCh:
			conn.sendQueueInflight[2].Add(-1)

			err := sendMessages(lowStream, msgs)
			if err != nil {
				drainPendingUnreachable()

				return false, err
			}
		}
	}
}

func (conn *peerConnection) stop(ctx context.Context) error {
	conn.logger.Infof("Stopping peer connection")

	// Cancel the connection-level context. This unblocks:
	// 1. Any in-progress stream creation (StreamMessages call)
	// 2. All stream.Recv() calls in receive goroutines
	// 3. The drainLoop and top-of-loop checks in loop()
	// Combined, this ensures loop() exits promptly.
	conn.stopCancel()

	// Wait for loop() to fully exit, including all handleConnection defers
	// (streamCancel, receiveWg.Wait, CloseSend). This guarantees no leaked
	// goroutines from this peer connection.
	select {
	case <-conn.loopDone:
	case <-ctx.Done():
		return ctx.Err()
	}

	// loop() is gone, so no consumer drains the queues. Close them so
	// any future range-over-channel exits cleanly and the buffered
	// messages are GC'd promptly.
	//
	// Closing alone is not safe: pushMessages is called by t.Start after a
	// peer snapshot — it can race RemovePeer and would panic with "send on
	// closed channel" (the panic fires even inside a select-with-default).
	// pubMu serialises the close against in-flight sends: pushMessages
	// holds pubMu.RLock during the send; pubMu.Lock here waits for every
	// active send to finish, then we set stopped=true so subsequent calls
	// bail out before touching the channel. See #315.
	conn.pubMu.Lock()
	conn.stopped = true
	close(conn.highPriorityCh)
	close(conn.mediumPriorityCh)
	close(conn.lowPriorityCh)
	conn.pubMu.Unlock()

	return nil
}
