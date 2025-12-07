package grpc

import (
	"context"
	"fmt"
	"sync"

	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn   *grpc.ClientConn
	client service.LedgerServiceClient
	logger *zap.Logger
	mu     sync.RWMutex
}

func NewClient(logger *zap.Logger) *Client {
	return &Client{
		logger: logger,
	}
}

func (c *Client) Connect(ctx context.Context, address string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Close existing connection if any
	if c.conn != nil {
		c.conn.Close()
	}

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	c.conn = conn
	c.client = service.NewLedgerServiceClient(conn)

	c.logger.Info("Connected to leader gRPC server", zap.String("address", address))
	return nil
}

// ConnectWithConnection connects using an existing gRPC connection
// This allows reusing connections from the transport layer
func (c *Client) ConnectWithConnection(conn *grpc.ClientConn) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Close existing connection if any (but don't close the one we're reusing)
	if c.conn != nil && c.conn != conn {
		c.conn.Close()
	}

	c.conn = conn
	c.client = service.NewLedgerServiceClient(conn)

	c.logger.Info("Reusing existing gRPC connection for leader")
	return nil
}

func (c *Client) GetClient() service.LedgerServiceClient {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.client
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

