package application

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

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

	// Convert ErrNoLeader to Unavailable (client should retry)
	if errors.Is(err, commonpb.ErrNoLeader) {
		return status.Error(codes.Unavailable, "no leader available, please retry")
	}

	// Convert NotFoundError to NotFound
	var notFoundErr *commonpb.NotFoundError
	if errors.As(err, &notFoundErr) {
		return status.Error(codes.NotFound, notFoundErr.Error())
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
	// Always add error conversion interceptor
	unaryInterceptors := []grpc.UnaryServerInterceptor{
		errorConversionInterceptor(),
	}
	streamInterceptors := []grpc.StreamServerInterceptor{
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
