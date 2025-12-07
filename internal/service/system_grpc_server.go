package service

import (
	"context"
	"fmt"

	"github.com/formancehq/go-libs/v3/logging"
	"google.golang.org/grpc"
)

// SnapshotClient is an interface for snapshot operations
type SnapshotClient interface {
	Snapshot() error
	CreateBucketSnapshot(bucketName string) error
}

type SystemServiceServerImpl struct {
	UnimplementedSystemServiceServer
	logger         logging.Logger
	snapshotClient SnapshotClient
}

func NewSystemServiceServer(logger logging.Logger, snapshotClient SnapshotClient) SystemServiceServer {
	return &SystemServiceServerImpl{
		logger:         logger,
		snapshotClient: snapshotClient,
	}
}

func (l *SystemServiceServerImpl) CreateClusterSnapshot(ctx context.Context, req *CreateClusterSnapshotRequest) (*CreateClusterSnapshotResponse, error) {
	l.logger.Debug("CreateClusterSnapshot request received")

	if l.snapshotClient == nil {
		return nil, fmt.Errorf("snapshot client not available")
	}

	if err := l.snapshotClient.Snapshot(); err != nil {
		return nil, fmt.Errorf("creating cluster snapshot: %w", err)
	}

	return &CreateClusterSnapshotResponse{
		Message: "Snapshot created successfully",
	}, nil
}

func (l *SystemServiceServerImpl) CreateBucketSnapshot(ctx context.Context, req *CreateBucketSnapshotRequest) (*CreateBucketSnapshotResponse, error) {
	l.logger.WithFields(map[string]any{"bucket": req.BucketName}).Debugf("CreateBucketSnapshot request received")

	if l.snapshotClient == nil {
		return nil, fmt.Errorf("snapshot client not available")
	}

	if req.BucketName == "" {
		return nil, fmt.Errorf("bucket name is required")
	}

	if err := l.snapshotClient.CreateBucketSnapshot(req.BucketName); err != nil {
		return nil, fmt.Errorf("creating bucket snapshot: %w", err)
	}

	return &CreateBucketSnapshotResponse{
		Message: "Snapshot created successfully",
	}, nil
}

func RegisterSystemService(server *grpc.Server, systemServiceServer SystemServiceServer) {
	RegisterSystemServiceServer(server, systemServiceServer)
}
