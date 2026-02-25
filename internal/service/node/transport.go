package node

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/monitoring/otlplogs"
	"github.com/formancehq/ledger-v3-poc/internal/proto/rafttransportpb"
	"github.com/formancehq/ledger-v3-poc/internal/infra/transport"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
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
// It wraps GRPCClientPool and manages Raft-specific message routing and channels
type DefaultTransport struct {
	rafttransportpb.UnimplementedRaftTransportServiceServer
	connectionPool *transport.ConnectionPool

	// 3 priority queues for incoming message batches (high to low priority)
	highPriorityRecvCh   chan []raftpb.Message // Heartbeats
	mediumPriorityRecvCh chan []raftpb.Message // Votes, responses
	lowPriorityRecvCh    chan []raftpb.Message // Data messages

	// Channels for outgoing messages per peer
	peers map[uint64]*peerConnection

	// Channel for reporting unreachable peers
	unreachableCh chan uint64

	logger        logging.Logger
	globalMeter   metric.Meter
	meterProvider metric.MeterProvider
	config        TransportConfig
	nodeID        uint64
	clusterID     string

	bufferSize       int
	pendingSendQueue chan []raftpb.Message
	stopCh           chan chan struct{}

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

// NewTransport creates a new transport with a gRPC connection pool and client pool
func NewTransport(
	logger logging.Logger,
	connectionPool *transport.ConnectionPool,
	meterProvider metric.MeterProvider,
	nodeID uint64,
	config TransportConfig,
	clusterID string,
	bufferSize int,
) *DefaultTransport {
	meter := meterProvider.Meter("raft.transport")

	const unreachableCapacity = 100
	const pendingSendCapacity = 100

	t := &DefaultTransport{
		connectionPool:       connectionPool,
		highPriorityRecvCh:   make(chan []raftpb.Message, config.Reception[0]),
		mediumPriorityRecvCh: make(chan []raftpb.Message, config.Reception[1]),
		lowPriorityRecvCh:    make(chan []raftpb.Message, config.Reception[2]),
		peers:                make(map[uint64]*peerConnection),
		unreachableCh:        make(chan uint64, unreachableCapacity),
		globalMeter:          meter,
		meterProvider:        meterProvider,
		logger:               logger,
		config:               config,
		nodeID:               nodeID,
		clusterID:            clusterID,
		bufferSize:           bufferSize,
		stopCh:               make(chan chan struct{}),
		pendingSendQueue:     make(chan []raftpb.Message, pendingSendCapacity),
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

// pushToRecvQueue pushes a batch of messages to the appropriate priority recv queue
func (t *DefaultTransport) pushToRecvQueue(priority int, msgs []raftpb.Message) bool {
	if len(msgs) == 0 {
		return true
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

// Stop stops the transport
func (t *DefaultTransport) Stop(ctx context.Context) error {

	t.logger.Infof("Stopping raft transport")

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
	for _, peerConnection := range t.peers {
		if err := peerConnection.stop(ctx); err != nil {
			return err
		}
	}

	close(t.highPriorityRecvCh)
	close(t.mediumPriorityRecvCh)
	close(t.lowPriorityRecvCh)
	close(t.unreachableCh)
	close(t.pendingSendQueue)

	return t.connectionPool.Close()
}

// AddPeer adds a peer to the transport. If the peer already exists, it is a no-op.
func (t *DefaultTransport) AddPeer(id uint64, addr string) {
	if _, exists := t.peers[id]; exists {
		return
	}

	if err := t.connectionPool.AddPeer(id, addr); err != nil {
		t.logger.WithFields(map[string]any{"peer": fmt.Sprintf("%x", id), "addr": addr, "error": err}).Errorf("Failed to add peer to client pool")
		return
	}

	meter := t.meterProvider.Meter("raft.transport",
		metric.WithInstrumentationAttributes(
			attribute.Int("peer", int(id)),
		),
	)
	logger := t.logger.WithFields(map[string]any{"peer": fmt.Sprintf("%x", id)})

	pendingResponseCounter, err := meter.Float64UpDownCounter("raft.transport.sending.pending_response")
	if err != nil {
		panic(err)
	}

	pingLatency, err := meter.Int64Histogram("raft.transport.ping.latency", metric.WithUnit("microseconds"))
	if err != nil {
		panic(err)
	}

	conn := &peerConnection{
		highPriorityCh:         make(chan []raftpb.Message, t.config.Send[0]),
		mediumPriorityCh:       make(chan []raftpb.Message, t.config.Send[1]),
		lowPriorityCh:          make(chan []raftpb.Message, t.config.Send[2]),
		closeCh:                make(chan chan struct{}),
		pushUnreachable:        t.pushUnreachable,
		connectionPool:         t.connectionPool,
		logger:                 logger,
		peerID:                 id,
		nodeID:                 t.nodeID,
		clusterID:              t.clusterID,
		bufferSize:             t.bufferSize,
		pendingResponseCounter: pendingResponseCounter,
		pingLatency:            pingLatency,
		reconnected:            make(chan struct{}),
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
	conn, exists := t.peers[id]
	if !exists {
		return
	}

	if err := conn.stop(ctx); err != nil {
		t.logger.WithFields(map[string]any{"peer": fmt.Sprintf("%x", id), "error": err}).
			Errorf("Failed to stop peer connection")
	}
	delete(t.peers, id)

	if err := t.connectionPool.RemovePeer(id); err != nil {
		t.logger.WithFields(map[string]any{"peer": fmt.Sprintf("%x", id), "error": err}).
			Errorf("Failed to remove peer from connection pool")
	}
}

// Send sends a message to a peer
func (t *DefaultTransport) Send(msgs []raftpb.Message) {
	if len(msgs) == 0 {
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
	}
}

// messagePriority returns the priority level for a raft message type
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
				peer, exists := t.peers[peerID]
				if !exists {
					t.logger.
						WithFields(map[string]any{
							"peer": fmt.Sprintf("%x", peerID),
						}).
						Errorf("No send channel for peer, dropping messages")
					continue
				}

				for priority, batch := range priorityMsgs {
					if !peer.pushMessages(priority, batch) {
						t.logger.
							WithFields(map[string]any{
								"peer":     fmt.Sprintf("%x", peerID),
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

// Unreachable returns the channel for reporting unreachable peers
func (t *DefaultTransport) Unreachable() <-chan uint64 {
	return t.unreachableCh
}

// pushUnreachable pushes a peer ID to the unreachable queue with metrics
func (t *DefaultTransport) pushUnreachable(peerID uint64) bool {
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
// This allows reusing existing connections for service calls instead of creating new ones
func (t *DefaultTransport) GetPeerConnection(peerID uint64) *grpc.ClientConn {
	return t.connectionPool.GetConnection(peerID)
}

// GetPeerAddress returns the address for a specific peer, if it exists
func (t *DefaultTransport) GetPeerAddress(peerID uint64) string {
	return t.connectionPool.GetPeerAddress(peerID)
}

// HandleStreamMessages handles client streaming gRPC connection for receiving messages
// This maintains a persistent connection to avoid frequent reconnections
// The server receives all messages and sends a single response at the end
func (t *DefaultTransport) StreamMessages(stream grpc.BidiStreamingServer[rafttransportpb.SendMessageRequest, rafttransportpb.SendMessageResponse]) error {

	nodeIDStr := metadata.ValueFromIncomingContext(stream.Context(), "nodeID")
	if len(nodeIDStr) == 0 {
		return fmt.Errorf("nodeID metadata not found in context")
	}

	peerID, err := strconv.ParseUint(nodeIDStr[0], 16, 64)
	if err != nil {
		return fmt.Errorf("failed to decode nodeID from metadata: %w", err)
	}

	// Validate cluster ID if configured
	if t.clusterID != "" {
		clusterIDStr := metadata.ValueFromIncomingContext(stream.Context(), "clusterID")
		if len(clusterIDStr) == 0 || clusterIDStr[0] != t.clusterID {
			return status.Errorf(codes.PermissionDenied, "invalid cluster ID")
		}
	}

	priorityStr := metadata.ValueFromIncomingContext(stream.Context(), "priority")
	if len(priorityStr) == 0 {
		return fmt.Errorf("priority metadata not found in context")
	}
	priority := priorityStr[0]

	t.logger.Infof("Peer %x connected on %s priority stream!", peerID, priority)
	// This is a best effort to notify the send loop than the peer is now reachable
	if peer, ok := t.peers[peerID]; ok {
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

		switch m := req.Message.(type) {
		case *rafttransportpb.SendMessageRequest_Ping:
			if err := stream.Send(&rafttransportpb.SendMessageResponse{
				Message: &rafttransportpb.SendMessageResponse_Pong{
					Pong: &rafttransportpb.PongResponse{
						SeqId: m.Ping.SeqId,
					},
				},
			}); err != nil {
				return err
			}
		case *rafttransportpb.SendMessageRequest_Raft:
			responses := make([]*rafttransportpb.RaftResponseMessage, 0, len(m.Raft.Messages))

			// Group messages by priority
			highPriorityMsgs := make([]raftpb.Message, 0)
			mediumPriorityMsgs := make([]raftpb.Message, 0)
			lowPriorityMsgs := make([]raftpb.Message, 0)

			for _, raftMsg := range m.Raft.Messages {
				var msg raftpb.Message
				if err := msg.Unmarshal(raftMsg.Message); err != nil {
					responses = append(responses, &rafttransportpb.RaftResponseMessage{
						Error:     fmt.Sprintf("failed to unmarshal message: %v", err),
						RequestId: raftMsg.Id,
					})
					continue
				}

				// Group by priority
				switch msg.Type {
				case raftpb.MsgHeartbeat, raftpb.MsgHeartbeatResp:
					highPriorityMsgs = append(highPriorityMsgs, msg)
				case raftpb.MsgAppResp, raftpb.MsgVote, raftpb.MsgVoteResp, raftpb.MsgPreVote, raftpb.MsgPreVoteResp:
					mediumPriorityMsgs = append(mediumPriorityMsgs, msg)
				default:
					lowPriorityMsgs = append(lowPriorityMsgs, msg)
				}

				responses = append(responses, &rafttransportpb.RaftResponseMessage{
					Success:   true,
					RequestId: raftMsg.Id,
				})
			}

			// Push batches to priority queues
			if !t.pushToRecvQueue(0, highPriorityMsgs) {
				t.logger.Errorf("High priority recv queue full, some messages may be dropped")
			}
			if !t.pushToRecvQueue(1, mediumPriorityMsgs) {
				t.logger.Errorf("Medium priority recv queue full, some messages may be dropped")
			}
			if !t.pushToRecvQueue(2, lowPriorityMsgs) {
				t.logger.Errorf("Low priority recv queue full, some messages may be dropped")
			}

			if err := stream.Send(&rafttransportpb.SendMessageResponse{
				Message: &rafttransportpb.SendMessageResponse_Raft{
					Raft: &rafttransportpb.RaftResponseBatch{
						Messages: responses,
					},
				},
			}); err != nil {
				t.logger.Errorf("Failed to send response to peer: %v", err)
			}
		}
	}
}

// RegisterRaftTransportService registers the RaftTransportService on the given gRPC server
func RegisterRaftTransportService(server *grpc.Server, transport *DefaultTransport) {
	transport.RegisterRaftService(server)
}

// RegisterRaftService registers the RaftTransportService on the given gRPC server
func (t *DefaultTransport) RegisterRaftService(server *grpc.Server) {
	rafttransportpb.RegisterRaftTransportServiceServer(server, t)
}

type peerConnection struct {
	// 3 priority queues for sending batches of messages (high to low priority)
	highPriorityCh   chan []raftpb.Message // Heartbeats
	mediumPriorityCh chan []raftpb.Message // Votes, responses
	lowPriorityCh    chan []raftpb.Message // Data messages (MsgApp with entries)

	closeCh                chan chan struct{}
	pushUnreachable        func(peerID uint64) bool
	connectionPool         *transport.ConnectionPool
	logger                 logging.Logger
	peerID                 uint64
	nodeID                 uint64
	clusterID              string
	bufferSize             int
	pendingResponseCounter metric.Float64UpDownCounter
	pingLatency            metric.Int64Histogram
	reconnected            chan struct{}
	messageID              uint64
	buf                    []byte

	// Metrics for sending queues (indexed by priority: 0=high, 1=medium, 2=low)
	sendQueueLoadHistogram [3]metric.Int64Histogram
	sendQueueFullCounter   [3]metric.Float64Counter
	sendQueueInflight      [3]atomic.Int32
}

// pushMessages pushes a batch of messages to the specified priority queue
func (conn *peerConnection) pushMessages(priority int, msgs []raftpb.Message) bool {
	if len(msgs) == 0 {
		return true
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
		return false
	}
}

// closeQueues closes all priority queues
func (conn *peerConnection) closeQueues() {
	close(conn.highPriorityCh)
	close(conn.mediumPriorityCh)
	close(conn.lowPriorityCh)
}

func (conn *peerConnection) loop() {
	defer otlplogs.RecoverAndLogPanics(conn.logger)

	conn.buf = make([]byte, 0, conn.bufferSize)

	for {
		select {
		case ch := <-conn.closeCh:
			close(ch)
			return
		default:
		}

		conn.logger.Infof("Creating stream to peer %x...", conn.peerID)
		grpcPeerConnection := conn.connectionPool.GetConnection(conn.peerID)
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
					if err := conn.connectionPool.RestartConnection(conn.peerID); err != nil {
						conn.logger.Errorf("Failed to restart connection to peer: %v", err)
					}
					break drainLoop
				case <-time.After(time.Until(waitingDelayBeforeReconnect)):
					conn.logger.Infof("Restarting connection to peer %x...", conn.peerID)
					if err := conn.connectionPool.RestartConnection(conn.peerID); err != nil {
						conn.logger.Errorf("Failed to restart connection to peer: %v", err)
					}
					break drainLoop
				}
			}
			continue
		}
	}
}

// priorityStream represents a stream dedicated to a specific priority level
type priorityStream struct {
	stream       grpc.BidiStreamingClient[rafttransportpb.SendMessageRequest, rafttransportpb.SendMessageResponse]
	priorityName string
}

func (conn *peerConnection) handleConnection(grpcPeerConnection *grpc.ClientConn) (bool, error) {
	client := rafttransportpb.NewRaftTransportServiceClient(grpcPeerConnection)

	// Create a stream for each priority level
	createStream := func(priorityName string) (grpc.BidiStreamingClient[rafttransportpb.SendMessageRequest, rafttransportpb.SendMessageResponse], error) {
		return client.StreamMessages(
			metadata.NewOutgoingContext(context.Background(), metadata.New(map[string]string{
				"nodeID":    fmt.Sprintf("%x", conn.nodeID),
				"priority":  priorityName,
				"clusterID": conn.clusterID,
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

	// Create a channel to signal when all receive goroutines have stopped
	var receiveWg sync.WaitGroup

	// Start a receive goroutine for each stream
	streams := []priorityStream{
		{stream: highStream, priorityName: "high"},
		{stream: mediumStream, priorityName: "medium"},
		{stream: lowStream, priorityName: "low"},
	}

	streamErrors := make(chan error, 3)

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

				switch msg := res.Message.(type) {
				case *rafttransportpb.SendMessageResponse_Pong:
					// Only high priority stream handles pings
					lastPingVal := lastPing.Load()
					if lastPingVal == nil {
						continue
					}
					lp := lastPingVal.(ping)
					if msg.Pong.SeqId != lp.seqId {
						conn.logger.
							WithFields(map[string]any{
								"expected-seq-id": lp.seqId,
								"received-seq-id": msg.Pong.SeqId,
							}).
							Errorf("Received unexpected ping response from peer")
						continue
					}
					conn.pingLatency.Record(context.Background(), time.Since(lp.at).Microseconds())

				case *rafttransportpb.SendMessageResponse_Raft:
					mu.Lock()
					for _, raftResp := range msg.Raft.Messages {
						nodeID, ok := pending[raftResp.RequestId]
						if ok {
							delete(pending, raftResp.RequestId)
							conn.pendingResponseCounter.Add(context.Background(), -1)
						} else {
							conn.logger.
								WithFields(map[string]any{
									"request-id": raftResp.RequestId,
									"priority":   ps.priorityName,
								}).
								Errorf("Received unexpected response from peer")
						}
						if !raftResp.Success && ok {
							conn.logger.
								Errorf("Failed to send message on %s stream, peer respond with error: %s", ps.priorityName, raftResp.Error)
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

		conn.logger.
			WithFields(map[string]any{
				"count": len(raftMessages),
			}).
			Debugf("Sending batch of messages to peer via stream")

		conn.pendingResponseCounter.Add(context.Background(), float64(len(raftMessages)))

		if err := stream.Send(&rafttransportpb.SendMessageRequest{
			Message: &rafttransportpb.SendMessageRequest_Raft{
				Raft: &rafttransportpb.RaftRequestBatch{
					Messages: raftMessages,
				},
			},
		}); err != nil {
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
		return nil
	}

	for {
		// First, try non-blocking receives in priority order (high -> medium -> low)
		select {
		case msgs := <-conn.highPriorityCh:
			conn.sendQueueInflight[0].Add(-1)
			if err := sendMessages(highStream, msgs); err != nil {
				for _, peerID := range pending {
					conn.pushUnreachable(peerID)
				}
				return false, err
			}
			continue
		default:
		}
		select {
		case msgs := <-conn.mediumPriorityCh:
			conn.sendQueueInflight[1].Add(-1)
			if err := sendMessages(mediumStream, msgs); err != nil {
				for _, peerID := range pending {
					conn.pushUnreachable(peerID)
				}
				return false, err
			}
			continue
		default:
		}
		select {
		case msgs := <-conn.lowPriorityCh:
			conn.sendQueueInflight[2].Add(-1)
			if err := sendMessages(lowStream, msgs); err != nil {
				for _, peerID := range pending {
					conn.pushUnreachable(peerID)
				}
				return false, err
			}
			continue
		default:
		}

		// No messages available, do a blocking select on all channels
		select {
		case ch := <-conn.closeCh:
			_ = highStream.CloseSend()
			_ = mediumStream.CloseSend()
			_ = lowStream.CloseSend()
			receiveWg.Wait()
			close(ch)
			return true, nil
		case err := <-streamErrors:
			for _, peerID := range pending {
				conn.pushUnreachable(peerID)
			}
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
				for _, peerID := range pending {
					conn.pushUnreachable(peerID)
				}
				conn.logger.Errorf("Failed to send ping to peer: %v", err)
				return false, err
			}
		case msgs := <-conn.highPriorityCh:
			conn.sendQueueInflight[0].Add(-1)
			if err := sendMessages(highStream, msgs); err != nil {
				for _, peerID := range pending {
					conn.pushUnreachable(peerID)
				}
				return false, err
			}
		case msgs := <-conn.mediumPriorityCh:
			conn.sendQueueInflight[1].Add(-1)
			if err := sendMessages(mediumStream, msgs); err != nil {
				for _, peerID := range pending {
					conn.pushUnreachable(peerID)
				}
				return false, err
			}
		case msgs := <-conn.lowPriorityCh:
			conn.sendQueueInflight[2].Add(-1)
			if err := sendMessages(lowStream, msgs); err != nil {
				for _, peerID := range pending {
					conn.pushUnreachable(peerID)
				}
				return false, err
			}
		}
	}
}

func (conn *peerConnection) stop(ctx context.Context) error {
	conn.logger.Infof("Stopping peer connection")
	ch := make(chan struct{})
	select {
	case conn.closeCh <- ch:
		select {
		case <-ch:
			conn.closeQueues()
			close(conn.closeCh)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}
