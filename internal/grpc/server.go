package grpc

import (
	"context"
	"fmt"
	"net"

	"github.com/formancehq/ledger-v3-poc/api"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Server struct {
	server   *grpc.Server
	listener net.Listener
	logger   *zap.Logger
	port     int
}

func NewServer(port int, logger *zap.Logger) *Server {
	return &Server{
		port:   port,
		logger: logger,
	}
}

func (s *Server) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = lis

	s.server = grpc.NewServer()
	api.RegisterEchoServiceServer(s.server, &echoService{logger: s.logger})

	s.logger.Info("Starting gRPC server", zap.Int("port", s.port))

	go func() {
		if err := s.server.Serve(lis); err != nil {
			s.logger.Error("gRPC server failed", zap.Error(err))
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	return s.Stop()
}

func (s *Server) Stop() error {
	if s.server != nil {
		s.logger.Info("Stopping gRPC server")
		s.server.GracefulStop()
	}
	return nil
}

// echoService implements the EchoService
type echoService struct {
	api.UnimplementedEchoServiceServer
	logger *zap.Logger
}

func (e *echoService) Echo(ctx context.Context, req *api.EchoRequest) (*api.EchoResponse, error) {
	e.logger.Debug("Echo request received", zap.String("message", req.Message))
	return &api.EchoResponse{
		Message: req.Message,
	}, nil
}
