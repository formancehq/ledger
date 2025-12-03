package grpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/formancehq/ledger-v3-poc/api"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn   *grpc.ClientConn
	client api.EchoServiceClient
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
	c.client = api.NewEchoServiceClient(conn)

	c.logger.Info("Connected to leader gRPC server", zap.String("address", address))
	return nil
}

func (c *Client) Echo(ctx context.Context, message string) (string, error) {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()

	if client == nil {
		return "", fmt.Errorf("not connected to leader")
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := client.Echo(ctx, &api.EchoRequest{Message: message})
	if err != nil {
		return "", fmt.Errorf("echo failed: %w", err)
	}

	return resp.Message, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

