package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/otlplogs"
	"github.com/formancehq/ledger-v3-poc/internal/transport"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

// DefaultTransport handles network communication between Raft nodes using gRPC
// It wraps GRPCClientPool and manages Raft-specific message routing and channels
type DefaultTransport struct {
	UnimplementedRaftTransportServiceServer
	connectionPool *transport.ConnectionPool

	// Channel for Incoming messages
	recvCh Queue[raftpb.Message]

	// Channels for outgoing messages per peer
	peers map[uint64]peerConnection

	// Channel for reporting unreachable peers
	unreachableCh Queue[uint64]

	logger        logging.Logger
	globalMeter   metric.Meter
	meterProvider metric.MeterProvider
	config        TransportConfig
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
	config TransportConfig,
) *DefaultTransport {
	meter := meterProvider.Meter("raft.transport")

	createQueue := func(capacity, priority int) Queue[raftpb.Message] {

		meter := meterProvider.Meter("raft.transport", metric.WithInstrumentationAttributes(
			attribute.Int("priority", priority),
		))

		return NewQueueObserver[raftpb.Message](
			"raft.transport.recv",
			NewSimpleQueue[raftpb.Message](capacity),
			WithLogger[raftpb.Message](logger),
			WithMeter[raftpb.Message](meter),
			WithAttributesFn(AddTypeAsAttribute),
		)
	}

	return &DefaultTransport{
		connectionPool: connectionPool,
		recvCh: NewPriorityQueue[raftpb.Message](
			RaftMessagePriority,
			logger,
			CreateQueues[raftpb.Message](config.Reception, createQueue)...,
		),
		peers: make(map[uint64]peerConnection),
		unreachableCh: NewQueueObserver[uint64](
			"raft.transport.unreachable",
			NewSimpleQueue[uint64](100),
			WithMeter[uint64](meter),
			WithLogger[uint64](logger),
		),
		globalMeter:   meter,
		meterProvider: meterProvider,
		logger:        logger,
		config:        config,
	}
}

// Stop stops the transport
func (t *DefaultTransport) Stop(ctx context.Context) error {
	t.logger.Infof("Stopping raft transport")
	for _, peerConnection := range t.peers {
		if err := peerConnection.stop(ctx); err != nil {
			return err
		}
	}

	if err := t.connectionPool.Close(); err != nil {
		return err
	}

	t.unreachableCh.Close()

	return nil
}

// AddPeer adds a peer to the transport
func (t *DefaultTransport) AddPeer(id uint64, addr string) {
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

	createQueue := func(capacity, priority int) Queue[raftpb.Message] {
		meter := t.meterProvider.Meter("raft.transport",
			metric.WithInstrumentationAttributes(
				attribute.Int("peer", int(id)),
				attribute.Int("priority", priority),
			),
		)

		return NewQueueObserver[raftpb.Message](
			"raft.transport.peer.sending",
			NewSimpleQueue[raftpb.Message](capacity),
			WithLogger[raftpb.Message](logger),
			WithMeter[raftpb.Message](meter),
			WithAttributesFn(func(msg raftpb.Message) []attribute.KeyValue {
				ret := AddTypeAsAttribute(msg)
				ret = append(ret, attribute.Int("peer", int(id)))
				return ret
			}),
		)
	}

	conn := peerConnection{
		sendCh: NewPriorityQueue[raftpb.Message](
			RaftMessagePriority,
			logger,
			CreateQueues[raftpb.Message](t.config.Send, createQueue)...,
		),
		closeCh:                make(chan chan struct{}),
		unreachableCh:          t.unreachableCh,
		connection:             t.connectionPool.GetConnection(id),
		logger:                 logger,
		peerID:                 id,
		pendingResponseCounter: pendingResponseCounter,
		pingLatency:            pingLatency,
	}
	t.peers[id] = conn

	go conn.loop()
}

// Send sends a message to a peer
func (t *DefaultTransport) Send(msg raftpb.Message) {
	peer, exists := t.peers[msg.To]

	if exists {
		if !peer.sendCh.Push(msg) {
			t.logger.
				WithFields(map[string]any{
					"peer": fmt.Sprintf("%x", msg.To),
					"type": msg.Type.String(),
				}).
				Errorf("Send channel full, dropping message")
		}
	} else {
		t.logger.
			WithFields(map[string]any{
				"peer": fmt.Sprintf("%x", msg.To),
				"type": msg.Type.String(),
			}).
			Errorf("No send channel for peer, dropping message")
	}
}

// Recv returns the channel for receiving messages
func (t *DefaultTransport) Recv() <-chan raftpb.Message {
	return t.recvCh.Recv()
}

// Unreachable returns the channel for reporting unreachable peers
func (t *DefaultTransport) Unreachable() <-chan uint64 {
	return t.unreachableCh.Recv()
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
func (t *DefaultTransport) StreamMessages(stream grpc.BidiStreamingServer[SendMessageRequest, SendMessageResponse]) error {

	// Receive all messages from the stream
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		switch m := req.Message.(type) {
		case *SendMessageRequest_Ping:
			if err := stream.Send(&SendMessageResponse{
				Message: &SendMessageResponse_Pong{
					Pong: &PongResponse{
						SeqId: m.Ping.SeqId,
					},
				},
			}); err != nil {
				return err
			}
		case *SendMessageRequest_Raft:
			var msg raftpb.Message
			if err := msg.Unmarshal(m.Raft.Message); err != nil {
				if err := stream.Send(&SendMessageResponse{
					Message: &SendMessageResponse_Raft{
						Raft: &RaftResponseMessage{
							Error:     fmt.Sprintf("failed to unmarshal message: %v", err),
							RequestId: m.Raft.Id,
						},
					},
				}); err != nil {
					return err
				}
				continue
			}

			// Send message to recvCh for processing
			if !t.recvCh.Push(msg) {
				if err := stream.Send(&SendMessageResponse{
					Message: &SendMessageResponse_Raft{
						Raft: &RaftResponseMessage{
							Error:     "recv channel full, dropping message",
							RequestId: m.Raft.Id,
						},
					},
				}); err != nil {
					return err
				}
				continue
			}

			if err := stream.Send(&SendMessageResponse{
				Message: &SendMessageResponse_Raft{
					Raft: &RaftResponseMessage{
						Success:   true,
						RequestId: m.Raft.Id,
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
	RegisterRaftTransportServiceServer(server, t)
}

type peerConnection struct {
	sendCh                 Queue[raftpb.Message]
	closeCh                chan chan struct{}
	unreachableCh          Queue[uint64]
	connection             *grpc.ClientConn
	logger                 logging.Logger
	peerID                 uint64
	pendingResponseCounter metric.Float64UpDownCounter
	pingLatency            metric.Int64Histogram
}

func (conn *peerConnection) loop() {
	defer otlplogs.RecoverAndLogPanics(conn.logger)

	messageID := uint64(0)
	pingInterval := time.NewTicker(time.Second)
	opts := proto.MarshalOptions{}
	buf := make([]byte, 0, 1024*1024*10) // todo: make configurable

	for {
		select {
		case ch := <-conn.closeCh:
			close(ch)
			return
		default:
		}

		// Create client streaming connection
		client := NewRaftTransportServiceClient(conn.connection)
		stream, err := client.StreamMessages(context.Background())
		if err != nil {
			conn.logger.
				WithFields(map[string]any{
					"error": err,
				}).
				Errorf("Failed to create stream to peer")
			// Report peer as unreachable
			if !conn.unreachableCh.Push(conn.peerID) {
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
				case <-conn.sendCh.Recv():
					conn.unreachableCh.Push(conn.peerID)
				case <-time.After(time.Until(waitingDelayBeforeReconnect)):
					break drainLoop
				}
			}
			continue
		}
		conn.logger.Infof("Created stream to peer")

		type ping struct {
			at    time.Time
			seqId uint64
		}

		pending := make(map[uint64]uint64)
		lastPing := atomic.Value{}
		mu := sync.Mutex{}
		otlplogs.Go(func() {
			for {
				res, err := stream.Recv()
				if err != nil {
					return
				}

				switch msg := res.Message.(type) {
				case *SendMessageResponse_Pong:
					lastPing := lastPing.Load().(ping)
					if msg.Pong.SeqId != lastPing.seqId {
						conn.logger.
							WithFields(map[string]any{
								"expected-seq-id": lastPing.seqId,
								"received-seq-id": msg.Pong.SeqId,
							}).
							Errorf("Received unexpected ping response from peer")
						continue
					}
					conn.pingLatency.Record(context.Background(), time.Since(lastPing.at).Microseconds())

				case *SendMessageResponse_Raft:
					mu.Lock()
					nodeID, ok := pending[msg.Raft.RequestId]
					if ok {
						delete(pending, msg.Raft.RequestId)
						conn.pendingResponseCounter.Add(context.Background(), -1)
					} else {
						conn.logger.
							WithFields(map[string]any{
								"request-id": msg.Raft.RequestId,
							}).
							Errorf("Received unexpected response from peer")
					}
					mu.Unlock()
					if !msg.Raft.Success && ok {
						conn.logger.
							Errorf("Failed to send message, peer respond with error: %s", msg.Raft.Error)
						conn.unreachableCh.Push(nodeID)
					}
				default:
					panic(fmt.Sprintf("received unexpected message type: %T", msg))
				}
			}
		}, conn.logger)

	l:
		for {
			select {
			case ch := <-conn.closeCh:
				close(ch)
				return
			case <-pingInterval.C:
				p := ping{
					at:    time.Now(),
					seqId: messageID,
				}
				lastPing.Store(p)
				err := stream.Send(&SendMessageRequest{
					Message: &SendMessageRequest_Ping{
						Ping: &PingMessage{
							SeqId: p.seqId,
						},
					},
				})
				if err != nil {
					if errors.Is(err, io.EOF) {
						conn.logger.Errorf("Peer connection broken, reconnect")
						break l
					}
					conn.logger.Errorf("Failed to send ping to peer: %v", err)
				}
			case msg := <-conn.sendCh.Recv():
				data, err := opts.MarshalAppend(buf, protoadapt.MessageV2Of(&msg))
				if err != nil {
					conn.logger.
						WithFields(map[string]any{
							"error": err,
						}).
						Errorf("Failed to marshal message")
					continue
				}

				conn.logger.
					WithFields(map[string]any{
						"type": msg.Type.String(),
					}).
					Debugf("Sending message to peer via stream")

				mu.Lock()
				currentMessageID := messageID
				messageID++
				pending[currentMessageID] = msg.To
				mu.Unlock()

				conn.pendingResponseCounter.Add(context.Background(), 1)

				if err := stream.Send(&SendMessageRequest{
					Message: &SendMessageRequest_Raft{
						Raft: &RaftRequestMessage{
							Message: data,
							Id:      currentMessageID,
						},
					},
				}); err != nil {
					conn.logger.
						WithFields(map[string]any{
							"error": err,
						}).
						Errorf("Failed to send message via stream")
					mu.Lock()
					delete(pending, currentMessageID)
					mu.Unlock()
					// Report peer as unreachable
					if !conn.unreachableCh.Push(msg.To) {
						conn.logger.Errorf("Unreachable channel full, dropping unreachable")
					}
					break l
				}
			}
		}

		_ = stream.CloseSend()
	}
}

func (conn *peerConnection) stop(ctx context.Context) error {
	conn.logger.Infof("Stopping peer connection")
	ch := make(chan struct{})
	select {
	case conn.closeCh <- ch:
		select {
		case <-ch:
			conn.sendCh.Close()
			close(conn.closeCh)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}
