package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"strconv"
	"time"

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

	"github.com/formancehq/ledger-v3-poc/internal/application/admission"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/health"
	"github.com/formancehq/ledger-v3-poc/internal/infra/transport"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
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

// baseServer contains common server functionality.
type baseServer struct {
	server   *ggrpc.Server
	listener net.Listener
	logger   logging.Logger
	port     int
	name     string
}

func (s *baseServer) GetServer() *ggrpc.Server {
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

// RaftServer is the gRPC server for Raft transport (internal inter-node communication).
type RaftServer struct {
	baseServer
}

// ServiceServer is the gRPC server for service API (external client-facing).
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
			s.logger.Infof("Graceful stop timed out, forcing stop")
			s.server.Stop()
			<-done // Wait for the GracefulStop goroutine to exit
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
			if logger.Enabled(logging.DebugLevel) {
				logger.WithFields(fields).Debugf("gRPC call")
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
			if logger.Enabled(logging.DebugLevel) {
				fields["error"] = err.Error()
				logger.WithFields(fields).Debugf("gRPC stream canceled")
			}
		case duration > slowThreshold:
			fields["slow"] = true
			logger.WithFields(fields).Infof("gRPC stream slow")
		default:
			if logger.Enabled(logging.DebugLevel) {
				logger.WithFields(fields).Debugf("gRPC stream")
			}
		}

		return err
	}
}

// errorConversionInterceptor converts known errors to proper gRPC status codes.
func errorConversionInterceptor() ggrpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			err = convertToGRPCError(err)
		}

		return resp, err
	}
}

// errorConversionStreamInterceptor converts known errors to proper gRPC status codes for streaming RPCs.
func errorConversionStreamInterceptor() ggrpc.StreamServerInterceptor {
	return func(srv any, ss ggrpc.ServerStream, info *ggrpc.StreamServerInfo, handler ggrpc.StreamHandler) error {
		err := handler(srv, ss)
		if err != nil {
			err = convertToGRPCError(err)
		}

		return err
	}
}

// convertToGRPCError converts known errors to proper gRPC status errors.
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

	// Convert maintenance mode error to Unavailable (client should retry later)
	if errors.Is(err, admission.ErrMaintenanceMode) {
		return status.Error(codes.Unavailable, err.Error())
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
	if errors.Is(err, domain.ErrAuditDisabled) {
		st := status.New(codes.FailedPrecondition, err.Error())

		detailed, detailErr := st.WithDetails(&errdetails.ErrorInfo{
			Reason: domain.ErrReasonAuditDisabled,
			Domain: "ledger",
		})
		if detailErr == nil {
			return detailed.Err()
		}

		return st.Err()
	}

	// Convert ErrPeriodNotClosed to FailedPrecondition with ErrorInfo
	var periodNotClosedErr *domain.ErrPeriodNotClosed
	if errors.As(err, &periodNotClosedErr) {
		st := status.New(codes.FailedPrecondition, err.Error())

		detailed, detailErr := st.WithDetails(&errdetails.ErrorInfo{
			Reason: domain.ErrReasonPeriodNotClosed,
			Domain: "ledger",
		})
		if detailErr == nil {
			return detailed.Err()
		}

		return st.Err()
	}

	// Convert ErrPeriodNotArchiving to FailedPrecondition with ErrorInfo
	var periodNotArchivingErr *domain.ErrPeriodNotArchiving
	if errors.As(err, &periodNotArchivingErr) {
		st := status.New(codes.FailedPrecondition, err.Error())

		detailed, detailErr := st.WithDetails(&errdetails.ErrorInfo{
			Reason: domain.ErrReasonPeriodNotArchiving,
			Domain: "ledger",
		})
		if detailErr == nil {
			return detailed.Err()
		}

		return st.Err()
	}

	// Convert ErrColdStorageDisabled to FailedPrecondition with ErrorInfo
	if errors.Is(err, domain.ErrColdStorageDisabled) {
		st := status.New(codes.FailedPrecondition, err.Error())

		detailed, detailErr := st.WithDetails(&errdetails.ErrorInfo{
			Reason: domain.ErrReasonColdStorageDisabled,
			Domain: "ledger",
		})
		if detailErr == nil {
			return detailed.Err()
		}

		return st.Err()
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

	// Convert BusinessError to proper gRPC status with ErrorInfo details
	var bizErr *domain.BusinessError
	if errors.As(err, &bizErr) {
		return businessErrorToGRPCStatus(bizErr).Err()
	}

	// Default: return as Unknown (preserves the original error message)
	return err
}

// NewRaftServer creates a new gRPC server for Raft transport (internal)
// This server is optimized for high-throughput inter-node communication
// and does not include OpenTelemetry instrumentation to minimize overhead.
// If tlsOpt is non-nil, it is appended to the server options to enable TLS.
func NewRaftServer(port int, logger logging.Logger, tlsOpt ggrpc.ServerOption) *RaftServer {
	opts := []ggrpc.ServerOption{
		ggrpc.InitialWindowSize(transport.GRPCInitialWindowSize),
		ggrpc.InitialConnWindowSize(transport.GRPCInitialConnWindowSize),
		ggrpc.ReadBufferSize(transport.GRPCReadBufferSize),
		ggrpc.WriteBufferSize(transport.GRPCWriteBufferSize),
		ggrpc.MaxRecvMsgSize(transport.GRPCMaxMsgSize),
		ggrpc.MaxSendMsgSize(transport.GRPCMaxMsgSize),
	}

	if tlsOpt != nil {
		opts = append(opts, tlsOpt)
	}

	server := ggrpc.NewServer(opts...)

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
// This server includes OpenTelemetry instrumentation and error conversion.
// Authentication is handled explicitly in each service method via auth.Authenticate.
// If tlsOpt is non-nil, it is appended to the server options to enable TLS.
func NewServiceServer(port int, logger logging.Logger, debug bool, slowThreshold time.Duration, tlsOpt ggrpc.ServerOption) *ServiceServer {
	// Recovery interceptor must be first (outermost) to catch panics from all handlers.
	// Logging is placed before error conversion so that on the response path
	// (innermost-first), error conversion runs first and logging sees the
	// proper gRPC status code instead of "Unknown" for domain errors.
	unaryInterceptors := []ggrpc.UnaryServerInterceptor{
		recoveryInterceptor(logger),
		consistencyInterceptor(),
		loggingInterceptor(logger, slowThreshold),
		errorConversionInterceptor(),
	}
	streamInterceptors := []ggrpc.StreamServerInterceptor{
		recoveryStreamInterceptor(logger),
		consistencyStreamInterceptor(),
		loggingStreamInterceptor(logger, slowThreshold),
		errorConversionStreamInterceptor(),
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

	if tlsOpt != nil {
		opts = append(opts, tlsOpt)
	}

	server := ggrpc.NewServer(opts...)

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
