package grpc

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/aws/smithy-go"
	"github.com/soheilhy/cmux"
	"go.etcd.io/raft/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/infra/health"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// vtFallbackCodec is a gRPC codec that uses vtprotobuf when available
// and falls back to standard proto.Marshal/Unmarshal otherwise.
// This is necessary because the process registers a single global codec,
// and non-VT messages (e.g. OpenTelemetry OTLP) must still work.
type vtFallbackCodec struct{}

func (vtFallbackCodec) Name() string { return "proto" }

func (vtFallbackCodec) Marshal(v any) ([]byte, error) {
	if m, ok := v.(interface{ MarshalVT() ([]byte, error) }); ok {
		return m.MarshalVT()
	}

	if m, ok := v.(proto.Message); ok {
		return proto.Marshal(m)
	}

	return nil, fmt.Errorf("failed to marshal: %T is not a proto.Message", v)
}

func (vtFallbackCodec) Unmarshal(data []byte, v any) error {
	if m, ok := v.(interface{ UnmarshalVT([]byte) error }); ok {
		return m.UnmarshalVT(data)
	}

	if m, ok := v.(proto.Message); ok {
		return proto.Unmarshal(data, m)
	}

	return fmt.Errorf("failed to unmarshal: %T is not a proto.Message", v)
}

func init() {
	encoding.RegisterCodec(vtFallbackCodec{})
}

// baseServer holds the listener and one or both gRPC servers.
//
// Modes:
//   - tlsCfg == nil, acceptPlaintext == true  → plaintext only (TLS disabled).
//   - tlsCfg != nil, acceptPlaintext == false → TLS only (TLS required).
//   - tlsCfg != nil, acceptPlaintext == true  → dual listener via cmux
//     (TLS optional, used as a transitional state during a TLS toggle).
//
// In dual mode, both servers share the same registered services via the
// multiRegistrar returned by GetServer.
type baseServer struct {
	tlsServer       *ggrpc.Server
	plaintextServer *ggrpc.Server
	tlsConfig       *tls.Config

	listener net.Listener
	mu       sync.Mutex

	logger logging.Logger
	host   string // bind host; empty means "0.0.0.0"
	port   int
	name   string
}

// multiRegistrar fans out RegisterService calls to every underlying gRPC
// server. It implements grpc.ServiceRegistrar.
type multiRegistrar struct {
	servers []*ggrpc.Server
}

func (m *multiRegistrar) RegisterService(desc *ggrpc.ServiceDesc, impl any) {
	for _, s := range m.servers {
		s.RegisterService(desc, impl)
	}
}

// underlying returns the non-nil grpc.Server instances backing this server.
func (s *baseServer) underlying() []*ggrpc.Server {
	servers := make([]*ggrpc.Server, 0, 2)
	if s.tlsServer != nil {
		servers = append(servers, s.tlsServer)
	}

	if s.plaintextServer != nil {
		servers = append(servers, s.plaintextServer)
	}

	return servers
}

// GetServer returns a grpc.ServiceRegistrar that registers handlers on every
// underlying gRPC server (one or two, depending on the TLS mode).
func (s *baseServer) GetServer() ggrpc.ServiceRegistrar {
	return &multiRegistrar{servers: s.underlying()}
}

// registerReflection enables gRPC reflection on every underlying server.
func (s *baseServer) registerReflection() {
	for _, srv := range s.underlying() {
		reflection.Register(srv)
	}
}

func (s *baseServer) Start(listening chan struct{}) error {
	host := s.host
	if host == "" {
		host = "0.0.0.0"
	}

	lis, err := net.Listen("tcp4", fmt.Sprintf("%s:%d", host, s.port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.mu.Lock()
	s.listener = lis
	s.mu.Unlock()

	s.logger.
		WithFields(map[string]any{
			"addr":      lis.Addr().String(),
			"tls":       s.tlsServer != nil,
			"plaintext": s.plaintextServer != nil,
		}).
		Infof("Starting %s server", s.name)

	close(listening)

	switch {
	case s.tlsServer != nil && s.plaintextServer != nil:
		return s.serveDual(lis)
	case s.tlsServer != nil:
		return s.serveSingle(s.tlsServer, tls.NewListener(lis, s.tlsConfig))
	default:
		return s.serveSingle(s.plaintextServer, lis)
	}
}

func (s *baseServer) serveSingle(server *ggrpc.Server, lis net.Listener) error {
	if err := server.Serve(lis); err != nil && !errors.Is(err, ggrpc.ErrServerStopped) && !errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("%s server failed: %w", s.name, err)
	}

	return nil
}

// serveDual multiplexes TLS and plaintext connections on the same listener
// via cmux. TLS connections are routed to s.tlsServer (wrapped via
// tls.NewListener), plaintext HTTP/2 connections (gRPC's plaintext mode is
// HTTP/2 with the cleartext preface) are routed to s.plaintextServer.
func (s *baseServer) serveDual(lis net.Listener) error {
	m := cmux.New(lis)
	tlsListener := m.Match(cmux.TLS())
	plainListener := m.Match(cmux.HTTP2())

	errCh := make(chan error, 3)

	go func() {
		errCh <- s.tlsServer.Serve(tls.NewListener(tlsListener, s.tlsConfig))
	}()

	go func() {
		errCh <- s.plaintextServer.Serve(plainListener)
	}()

	go func() {
		errCh <- m.Serve()
	}()

	// Wait for the first goroutine to exit. Any error other than the
	// expected shutdown errors is surfaced.
	for range 3 {
		err := <-errCh
		if err != nil && !errors.Is(err, ggrpc.ErrServerStopped) && !errors.Is(err, net.ErrClosed) {
			// cmux returns ErrServerClosed-like errors when the listener
			// is closed; tolerate them.
			if err.Error() == "mux: listener closed" {
				continue
			}

			return fmt.Errorf("%s server failed: %w", s.name, err)
		}
	}

	return nil
}

func (s *baseServer) stopImmediate() {
	for _, srv := range s.underlying() {
		srv.Stop()
	}
}

func (s *baseServer) stopGraceful(timeout time.Duration) {
	done := make(chan struct{})

	go func() {
		var wg sync.WaitGroup

		for _, srv := range s.underlying() {
			wg.Add(1)

			go func(srv *ggrpc.Server) {
				defer wg.Done()
				srv.GracefulStop()
			}(srv)
		}

		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		s.logger.Infof("Graceful stop timed out, forcing stop")
		s.stopImmediate()
		<-done
	}
}

func (s *baseServer) closeListener() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
}

// RaftServer is the gRPC server for Raft transport (internal inter-node communication).
type RaftServer struct {
	*baseServer
}

func (s *RaftServer) Stop() error {
	s.logger.Infof("Stopping %s server", s.name)
	s.stopImmediate()
	s.closeListener()

	return nil
}

// ServiceServer is the gRPC server for service API (external client-facing).
type ServiceServer struct {
	*baseServer
}

// Stop gracefully shuts down the service server, waiting up to 2 seconds for
// in-flight handlers to complete before forcing a stop. This prevents panics
// from handlers accessing resources (e.g. pebble) that are closed after the
// gRPC server stops.
func (s *ServiceServer) Stop() error {
	s.logger.Infof("Stopping %s server", s.name)
	s.stopGraceful(2 * time.Second)
	s.closeListener()

	return nil
}

// newCorrelationID returns a short hex token. It is mentioned in the
// generic error message returned to clients and logged alongside the
// raw cause server-side, so ops can grep the logs for the same string
// the user reports.
func newCorrelationID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// rand.Read returning an error on a server is exceptional. Fall
		// back to a non-secret deterministic token so the caller still
		// has SOMETHING to quote.
		return fmt.Sprintf("ts-%d", time.Now().UnixNano())
	}

	return hex.EncodeToString(b[:])
}

// handlePanic records a panic with its stack trace on the current OTel
// span, logs it server-side, and returns a SANITIZED error to the
// caller. The raw panic value and stack used to be embedded in the
// gRPC error message — internal file paths, goroutine state, and
// invariant strings disclosed to any client (#326). We now only return
// a generic codes.Internal carrying a correlation ID; ops resolves
// `correlation_id=<id>` against the logs.
func handlePanic(ctx context.Context, logger logging.Logger, r any, stack []byte) error {
	correlationID := newCorrelationID()
	logger.WithFields(map[string]any{
		"correlation_id": correlationID,
	}).Errorf("gRPC handler panicked: %v\n%s", r, stack)

	span := trace.SpanFromContext(ctx)
	span.SetAttributes(
		attribute.String("panic.value", fmt.Sprintf("%v", r)),
		attribute.String("panic.stack", string(stack)),
		attribute.String("correlation_id", correlationID),
	)

	grpcErr := status.Errorf(codes.Internal, "internal server error (correlation ID: %s)", correlationID)
	span.RecordError(grpcErr)

	return grpcErr
}

// recoveryInterceptor catches panics and records stack traces on the OTel span.
func recoveryInterceptor(logger logging.Logger) ggrpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = handlePanic(ctx, logger, r, debug.Stack())
			}
		}()

		return handler(ctx, req)
	}
}

// recoveryStreamInterceptor catches panics in streaming RPCs and records stack traces on the OTel span.
func recoveryStreamInterceptor(logger logging.Logger) ggrpc.StreamServerInterceptor {
	return func(srv any, ss ggrpc.ServerStream, info *ggrpc.StreamServerInfo, handler ggrpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = handlePanic(ss.Context(), logger, r, debug.Stack())
			}
		}()

		return handler(srv, ss)
	}
}

// loggingInterceptor logs every unary RPC with method, duration, and status code.
func loggingInterceptor(logger logging.Logger, slowThreshold time.Duration) ggrpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start)

		fields := map[string]any{
			"method":   info.FullMethod,
			"duration": duration.String(),
			"code":     status.Code(err).String(),
		}
		switch {
		case err != nil:
			fields["error"] = err.Error()
			logger.WithFields(fields).Errorf("gRPC call failed")
		case duration > slowThreshold:
			fields["slow"] = true
			logger.WithFields(fields).Infof("gRPC call slow")
		default:
			if logger.Enabled(logging.TraceLevel) {
				logger.WithFields(fields).Tracef("gRPC call")
			}
		}

		return resp, err
	}
}

// loggingStreamInterceptor logs every streaming RPC with method, duration, and status code.
func loggingStreamInterceptor(logger logging.Logger, slowThreshold time.Duration) ggrpc.StreamServerInterceptor {
	return func(srv any, ss ggrpc.ServerStream, info *ggrpc.StreamServerInfo, handler ggrpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		duration := time.Since(start)

		code := status.Code(err)
		fields := map[string]any{
			"method":   info.FullMethod,
			"duration": duration.String(),
			"code":     code.String(),
		}
		switch {
		case err != nil && code != codes.Canceled && code != codes.DeadlineExceeded:
			fields["error"] = err.Error()
			logger.WithFields(fields).Errorf("gRPC stream failed")
		case err != nil:
			if logger.Enabled(logging.TraceLevel) {
				fields["error"] = err.Error()
				logger.WithFields(fields).Tracef("gRPC stream canceled")
			}
		case duration > slowThreshold:
			fields["slow"] = true
			logger.WithFields(fields).Infof("gRPC stream slow")
		default:
			if logger.Enabled(logging.TraceLevel) {
				logger.WithFields(fields).Tracef("gRPC stream")
			}
		}

		return err
	}
}

// errorConversionInterceptor converts known errors to proper gRPC status codes.
func errorConversionInterceptor(logger logging.Logger) ggrpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			err = convertToGRPCError(err, logger)
		}

		return resp, err
	}
}

// errorConversionStreamInterceptor converts known errors to proper gRPC status codes for streaming RPCs.
func errorConversionStreamInterceptor(logger logging.Logger) ggrpc.StreamServerInterceptor {
	return func(srv any, ss ggrpc.ServerStream, info *ggrpc.StreamServerInfo, handler ggrpc.StreamHandler) error {
		err := handler(srv, ss)
		if err != nil {
			err = convertToGRPCError(err, logger)
		}

		return err
	}
}

// convertToGRPCError converts known errors to proper gRPC status errors.
// The bulk of the work is delegated to describableToGRPCStatus: any domain
// error that implements Describable (every typed *Err* and BusinessError
// itself) flows through a single exhaustive Kind switch. The remaining
// branches below cover non-domain errors (signing, raft, AWS smithy, etc.)
// that don't fit the domain Describable model.
// Unmapped errors are logged with a correlation ID and replaced with a
// generic codes.Unknown so internal implementation details (Pebble
// strings, file paths, invariant messages) are not disclosed to API
// clients (#326).
func convertToGRPCError(err error, logger logging.Logger) error {
	// Already a gRPC status error, return as-is
	if _, ok := status.FromError(err); ok {
		return err
	}

	// Convert signature errors to proper gRPC codes
	if errors.Is(err, signing.ErrMissingSignature) {
		return status.Error(codes.Unauthenticated, err.Error())
	}

	if errors.Is(err, signing.ErrInvalidSignature) {
		return status.Error(codes.PermissionDenied, err.Error())
	}

	if errors.Is(err, signing.ErrUnknownKeyID) {
		return status.Error(codes.PermissionDenied, err.Error())
	}

	// Convert context deadline/cancellation from internal timeouts (e.g. proposeTimeout)
	// to Unavailable so the client retry policy handles them.
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return status.Error(codes.Unavailable, err.Error())
	}

	// Convert Raft transient errors to Unavailable (client should retry).
	// ErrProposalDropped: the leader lost leadership while processing the proposal.
	// ErrLeadershipLost: the proposal was truncated by a later term, so it did
	// not commit — retrying re-applies it exactly once.
	// ErrNotLeader/ErrNodeSyncing: node cannot serve the request right now.
	// ErrTransferLeaderTimeout: leadership transfer did not complete in time.
	// ErrNoLeader: target peer doesn't know who the leader is yet (election
	// still in progress, or it just joined and hasn't received the heartbeat).
	if errors.Is(err, raft.ErrProposalDropped) ||
		errors.Is(err, node.ErrLeadershipLost) ||
		errors.Is(err, node.ErrNotLeader) ||
		errors.Is(err, node.ErrNodeSyncing) ||
		errors.Is(err, node.ErrTransferLeaderTimeout) ||
		errors.Is(err, commonpb.ErrNoLeader) {
		return status.Error(codes.Unavailable, err.Error())
	}

	// Convert leadership transfer client errors to FailedPrecondition.
	if errors.Is(err, node.ErrUnknownTransferee) ||
		errors.Is(err, node.ErrLearnerNotEligible) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}

	// Backup-destination busy: stable retry signal so clients can distinguish
	// a normal concurrent-backup rejection from an opaque server error. The
	// FSM Start path rejects duplicate destinations and duplicate job_ids;
	// both surface here through state's typed sentinels.
	if errors.Is(err, state.ErrBackupInProgress) ||
		errors.Is(err, state.ErrBackupJobIDCollision) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}

	// Convert ErrNodeAlreadyInCluster to AlreadyExists (idempotent AddLearner).
	if errors.Is(err, node.ErrNodeAlreadyInCluster) {
		return status.Error(codes.AlreadyExists, err.Error())
	}

	// Convert ErrUnhealthy to Unavailable with ErrorInfo (client should retry later)
	if errors.Is(err, health.ErrUnhealthy) {
		st := status.New(codes.Unavailable, err.Error())

		detailed, detailErr := st.WithDetails(&errdetails.ErrorInfo{
			Reason: domain.ErrReasonClusterUnhealthy,
			Domain: errorDomain,
		})
		if detailErr == nil {
			return detailed.Err()
		}

		return st.Err()
	}

	// Convert NotFoundError to NotFound
	var notFoundErr *commonpb.NotFoundError
	if errors.As(err, &notFoundErr) {
		return status.Error(codes.NotFound, notFoundErr.Error())
	}

	// Convert ErrReadIndexNotCaughtUp to FailedPrecondition with details
	var notCaughtUp *query.ErrReadIndexNotCaughtUp
	if errors.As(err, &notCaughtUp) {
		st := status.New(codes.FailedPrecondition, notCaughtUp.Error())

		detailed, detailErr := st.WithDetails(&errdetails.ErrorInfo{
			Reason: "READ_INDEX_NOT_CAUGHT_UP",
			Domain: "ledger",
			Metadata: map[string]string{
				"requested": strconv.FormatUint(notCaughtUp.Requested, 10),
				"current":   strconv.FormatUint(notCaughtUp.Current, 10),
			},
		})
		if detailErr == nil {
			return detailed.Err()
		}

		return st.Err()
	}

	// Domain errors: any *Err* type or sentinel that implements Describable,
	// whether wrapped in BusinessError or returned raw, flows through one
	// exhaustive Kind switch in describableToGRPCStatus.
	var d domain.Describable
	if errors.As(err, &d) {
		return describableToGRPCStatus(d).Err()
	}

	// Convert AWS S3/infrastructure errors to FailedPrecondition so clients
	// can distinguish infrastructure misconfiguration from application bugs.
	// APIError covers S3 API errors (NoSuchBucket, AccessDenied, etc.).
	// OperationError covers transport-level failures (DNS, connection refused).
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		st := status.New(codes.FailedPrecondition, err.Error())

		detailed, detailErr := st.WithDetails(&errdetails.ErrorInfo{
			Reason: "EXTERNAL_SERVICE_ERROR",
			Domain: errorDomain,
			Metadata: map[string]string{
				"code":    apiErr.ErrorCode(),
				"service": "s3",
			},
		})
		if detailErr == nil {
			return detailed.Err()
		}

		return st.Err()
	}

	var opErr *smithy.OperationError
	if errors.As(err, &opErr) {
		st := status.New(codes.FailedPrecondition, err.Error())

		detailed, detailErr := st.WithDetails(&errdetails.ErrorInfo{
			Reason: "EXTERNAL_SERVICE_ERROR",
			Domain: errorDomain,
			Metadata: map[string]string{
				"service":   opErr.Service(),
				"operation": opErr.Operation(),
			},
		})
		if detailErr == nil {
			return detailed.Err()
		}

		return st.Err()
	}

	// Default: sanitize. The raw error may contain Pebble error strings,
	// file system paths, or internal invariant messages; do not leak any
	// of that to the client. Log server-side with a correlation ID and
	// return a generic Unknown.
	correlationID := newCorrelationID()
	logger.WithFields(map[string]any{
		"correlation_id": correlationID,
	}).Errorf("Unmapped gRPC handler error: %v", err)

	return status.Errorf(codes.Unknown, "unknown server error (correlation ID: %s)", correlationID)
}

// buildBaseServer constructs the baseServer fields shared by RaftServer and
// ServiceServer. It instantiates one or two underlying gRPC servers based on
// the (tlsCfg, acceptPlaintext) combination.
//
// host controls the bind address; empty defaults to "0.0.0.0" (every
// interface). Restore mode passes "127.0.0.1" to keep the destructive RPCs
// off the public network unless an operator explicitly opts in.
//
// Invariant: at least one of (tlsCfg != nil, acceptPlaintext) must hold.
func buildBaseServer(name, host string, port int, logger logging.Logger, tlsCfg *tls.Config, acceptPlaintext bool, opts []ggrpc.ServerOption) (*baseServer, error) {
	if tlsCfg == nil && !acceptPlaintext {
		return nil, fmt.Errorf("%s: cannot construct server with neither TLS nor plaintext enabled", name)
	}

	bs := &baseServer{
		host:      host,
		port:      port,
		logger:    logger,
		name:      name,
		tlsConfig: tlsCfg,
	}

	if tlsCfg != nil {
		bs.tlsServer = ggrpc.NewServer(opts...)
	}

	if acceptPlaintext {
		bs.plaintextServer = ggrpc.NewServer(opts...)
	}

	return bs, nil
}

// NewRaftServer creates a new gRPC server for Raft transport (internal).
// This server is optimized for high-throughput inter-node communication
// and does not include OpenTelemetry instrumentation to minimize overhead.
//
// (tlsCfg, acceptPlaintext) maps to the TLS mode:
//   - (nil, true):  plaintext only
//   - (cfg, false): TLS only
//   - (cfg, true):  dual listener (cmux), used as a transitional state
//
// clusterSecret guards every RPC on this server (transport stream + snapshot
// service). When empty, the server runs unauthenticated — historical default,
// matches single-node setups that never set --cluster-secret. When set, the
// caller MUST present `authorization: Bearer <clusterSecret>` on every call
// or the RPC is rejected with codes.Unauthenticated (#310).
func NewRaftServer(port int, logger logging.Logger, tlsCfg *tls.Config, acceptPlaintext bool, clusterSecret string) (*RaftServer, error) {
	opts := []ggrpc.ServerOption{
		ggrpc.InitialWindowSize(transport.GRPCInitialWindowSize),
		ggrpc.InitialConnWindowSize(transport.GRPCInitialConnWindowSize),
		ggrpc.ReadBufferSize(transport.GRPCReadBufferSize),
		ggrpc.WriteBufferSize(transport.GRPCWriteBufferSize),
		ggrpc.MaxRecvMsgSize(transport.GRPCMaxMsgSize),
		ggrpc.MaxSendMsgSize(transport.GRPCMaxMsgSize),
	}

	if unary, stream := raftAuthInterceptors(clusterSecret); unary != nil {
		opts = append(opts,
			ggrpc.ChainUnaryInterceptor(unary),
			ggrpc.ChainStreamInterceptor(stream),
		)
	}

	bs, err := buildBaseServer("Raft gRPC", "", port, logger, tlsCfg, acceptPlaintext, opts)
	if err != nil {
		return nil, err
	}

	srv := &RaftServer{baseServer: bs}
	srv.registerReflection()

	return srv, nil
}

// NewServiceServer creates a new gRPC server for service API (external).
// This server includes OpenTelemetry instrumentation and error conversion.
// Authentication is handled explicitly in each service method via auth.Authenticate.
//
// host controls the bind address. Empty means "0.0.0.0" (every interface) —
// the normal mode. Restore mode passes "127.0.0.1" by default so the
// destructive restore RPCs are not exposed on the public network.
//
// See NewRaftServer for the (tlsCfg, acceptPlaintext) semantics.
func NewServiceServer(host string, port int, logger logging.Logger, debug bool, slowThreshold time.Duration, tlsCfg *tls.Config, acceptPlaintext bool) (*ServiceServer, error) {
	// Recovery interceptor must be first (outermost) to catch panics from all handlers.
	// Logging is placed before error conversion so that on the response path
	// (innermost-first), error conversion runs first and logging sees the
	// proper gRPC status code instead of "Unknown" for domain errors.
	unaryInterceptors := []ggrpc.UnaryServerInterceptor{
		recoveryInterceptor(logger),
		consistencyInterceptor(),
		loggingInterceptor(logger, slowThreshold),
		errorConversionInterceptor(logger),
	}
	streamInterceptors := []ggrpc.StreamServerInterceptor{
		recoveryStreamInterceptor(logger),
		consistencyStreamInterceptor(),
		loggingStreamInterceptor(logger, slowThreshold),
		errorConversionStreamInterceptor(logger),
	}

	opts := []ggrpc.ServerOption{
		ggrpc.StatsHandler(otelgrpc.NewServerHandler()),
		ggrpc.InitialWindowSize(transport.GRPCInitialWindowSize),
		ggrpc.InitialConnWindowSize(transport.GRPCInitialConnWindowSize),
		ggrpc.ReadBufferSize(transport.GRPCReadBufferSize),
		ggrpc.WriteBufferSize(transport.GRPCWriteBufferSize),
		ggrpc.MaxRecvMsgSize(transport.GRPCMaxMsgSize),
		ggrpc.MaxSendMsgSize(transport.GRPCMaxMsgSize),
		ggrpc.ChainUnaryInterceptor(unaryInterceptors...),
		ggrpc.ChainStreamInterceptor(streamInterceptors...),
	}

	bs, err := buildBaseServer("Service gRPC", host, port, logger, tlsCfg, acceptPlaintext, opts)
	if err != nil {
		return nil, err
	}

	srv := &ServiceServer{baseServer: bs}
	srv.registerReflection()

	return srv, nil
}
