package application

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"github.com/formancehq/ledger-v3-poc/internal/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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

// baseServer contains common server functionality
type baseServer struct {
	server   *grpc.Server
	listener net.Listener
	logger   logging.Logger
	port     int
	name     string
}

func (s *baseServer) GetServer() *grpc.Server {
	return s.server
}

func (s *baseServer) Start(listening chan struct{}) error {
	lis, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = lis

	s.logger.
		WithFields(map[string]any{"addr": lis.Addr().String()}).
		Infof("Starting %s server", s.name)

	close(listening)

	if err := s.server.Serve(lis); err != nil {
		return fmt.Errorf("%s server failed: %w", s.name, err)
	}
	return nil
}

func (s *baseServer) Stop() error {
	s.logger.Infof("Stopping %s server", s.name)
	if s.server != nil {
		s.server.Stop()
		s.server = nil
	}
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
	return nil
}

// RaftServer is the gRPC server for Raft transport (internal inter-node communication)
type RaftServer struct {
	baseServer
}

// ServiceServer is the gRPC server for service API (external client-facing)
type ServiceServer struct {
	baseServer
}

// Stop gracefully shuts down the service server, waiting up to 2 seconds for
// in-flight handlers to complete before forcing a stop. This prevents panics
// from handlers accessing resources (e.g. pebble) that are closed after the
// gRPC server stops.
func (s *ServiceServer) Stop() error {
	s.logger.Infof("Stopping %s server", s.name)
	if s.server != nil {
		done := make(chan struct{})
		go func() {
			s.server.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			s.server.Stop()
		}
		s.server = nil
	}
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
	return nil
}

// handlePanic records a panic with its stack trace on the current OTel span
// and logs it server-side.
func handlePanic(ctx context.Context, logger logging.Logger, r any, stack []byte) error {
	logger.Errorf("gRPC handler panicked: %v\n%s", r, stack)

	span := trace.SpanFromContext(ctx)
	span.SetAttributes(
		attribute.String("panic.value", fmt.Sprintf("%v", r)),
		attribute.String("panic.stack", string(stack)),
	)
	grpcErr := status.Errorf(codes.Internal, "panic: %v\n%s", r, stack)
	span.RecordError(grpcErr)
	return grpcErr
}

// recoveryInterceptor catches panics and records stack traces on the OTel span.
func recoveryInterceptor(logger logging.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = handlePanic(ctx, logger, r, debug.Stack())
			}
		}()
		return handler(ctx, req)
	}
}

// recoveryStreamInterceptor catches panics in streaming RPCs and records stack traces on the OTel span.
func recoveryStreamInterceptor(logger logging.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = handlePanic(ss.Context(), logger, r, debug.Stack())
			}
		}()
		return handler(srv, ss)
	}
}

// errorConversionInterceptor converts known errors to proper gRPC status codes
func errorConversionInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			err = convertToGRPCError(err)
		}
		return resp, err
	}
}

// errorConversionStreamInterceptor converts known errors to proper gRPC status codes for streaming RPCs
func errorConversionStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, ss)
		if err != nil {
			err = convertToGRPCError(err)
		}
		return err
	}
}

// convertToGRPCError converts known errors to proper gRPC status errors
func convertToGRPCError(err error) error {
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

	// Convert ErrNoLeader to Unavailable (client should retry)
	if errors.Is(err, commonpb.ErrNoLeader) {
		return status.Error(codes.Unavailable, "no leader available, please retry")
	}

	// Convert NotFoundError to NotFound
	var notFoundErr *commonpb.NotFoundError
	if errors.As(err, &notFoundErr) {
		return status.Error(codes.NotFound, notFoundErr.Error())
	}

	// Convert ErrAuditDisabled to FailedPrecondition with ErrorInfo
	if errors.Is(err, processing.ErrAuditDisabled) {
		st := status.New(codes.FailedPrecondition, err.Error())
		detailed, detailErr := st.WithDetails(&errdetails.ErrorInfo{
			Reason: processing.ErrReasonAuditDisabled,
			Domain: "ledger",
		})
		if detailErr == nil {
			return detailed.Err()
		}
		return st.Err()
	}

	// Convert ErrPeriodNotClosed to FailedPrecondition with ErrorInfo
	var periodNotClosedErr *processing.ErrPeriodNotClosed
	if errors.As(err, &periodNotClosedErr) {
		st := status.New(codes.FailedPrecondition, err.Error())
		detailed, detailErr := st.WithDetails(&errdetails.ErrorInfo{
			Reason: processing.ErrReasonPeriodNotClosed,
			Domain: "ledger",
		})
		if detailErr == nil {
			return detailed.Err()
		}
		return st.Err()
	}

	// Convert ErrPeriodNotArchiving to FailedPrecondition with ErrorInfo
	var periodNotArchivingErr *processing.ErrPeriodNotArchiving
	if errors.As(err, &periodNotArchivingErr) {
		st := status.New(codes.FailedPrecondition, err.Error())
		detailed, detailErr := st.WithDetails(&errdetails.ErrorInfo{
			Reason: processing.ErrReasonPeriodNotArchiving,
			Domain: "ledger",
		})
		if detailErr == nil {
			return detailed.Err()
		}
		return st.Err()
	}

	// Convert BusinessError to proper gRPC status with ErrorInfo details
	var bizErr *processing.BusinessError
	if errors.As(err, &bizErr) {
		return businessErrorToGRPCStatus(bizErr).Err()
	}

	// Default: return as Unknown (preserves the original error message)
	return err
}

// NewRaftServer creates a new gRPC server for Raft transport (internal)
// This server is optimized for high-throughput inter-node communication
// and does not include OpenTelemetry instrumentation to minimize overhead
func NewRaftServer(port int, logger logging.Logger) *RaftServer {
	opts := []grpc.ServerOption{
		grpc.InitialWindowSize(16 * 1024 * 1024),
		grpc.InitialConnWindowSize(64 * 1024 * 1024),
		grpc.ReadBufferSize(1 * 1024 * 1024),
		grpc.WriteBufferSize(1 * 1024 * 1024),
		grpc.MaxRecvMsgSize(64 * 1024 * 1024),
		grpc.MaxSendMsgSize(64 * 1024 * 1024),
	}

	server := grpc.NewServer(opts...)

	return &RaftServer{
		baseServer: baseServer{
			port:   port,
			logger: logger,
			server: server,
			name:   "Raft gRPC",
		},
	}
}

// NewServiceServer creates a new gRPC server for service API (external)
// This server includes OpenTelemetry instrumentation and error conversion
func NewServiceServer(port int, logger logging.Logger, debug bool) *ServiceServer {
	// Recovery interceptor must be first (outermost) to catch panics from all handlers
	unaryInterceptors := []grpc.UnaryServerInterceptor{
		recoveryInterceptor(logger),
		errorConversionInterceptor(),
	}
	streamInterceptors := []grpc.StreamServerInterceptor{
		recoveryStreamInterceptor(logger),
		errorConversionStreamInterceptor(),
	}

	// Add logging interceptor in debug mode
	if debug {
		unaryInterceptors = append(unaryInterceptors,
			func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				logger.WithFields(map[string]any{
					"method": info.FullMethod,
				}).Debug("gRPC request received")
				resp, err := handler(ctx, req)
				if err != nil {
					logger.WithFields(map[string]any{
						"method": info.FullMethod,
						"error":  err.Error(),
					}).Debug("gRPC request failed")
				}
				return resp, err
			},
		)
	}

	opts := []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.InitialWindowSize(16 * 1024 * 1024),
		grpc.InitialConnWindowSize(64 * 1024 * 1024),
		grpc.ReadBufferSize(1 * 1024 * 1024),
		grpc.WriteBufferSize(1 * 1024 * 1024),
		grpc.MaxRecvMsgSize(64 * 1024 * 1024),
		grpc.MaxSendMsgSize(64 * 1024 * 1024),
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		grpc.ChainStreamInterceptor(streamInterceptors...),
	}

	server := grpc.NewServer(opts...)

	// Enable gRPC reflection for debugging tools like grpcurl
	reflection.Register(server)

	return &ServiceServer{
		baseServer: baseServer{
			port:   port,
			logger: logger,
			server: server,
			name:   "Service gRPC",
		},
	}
}
