package service

import (
	"context"
	"fmt"

	"github.com/formancehq/go-libs/v3/logging"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SystemServiceServerImpl struct {
	UnimplementedSystemServiceServer
	logger  logging.Logger
	cluster MasterCluster
}

func NewSystemServiceServer(logger logging.Logger, cluster MasterCluster) SystemServiceServer {
	return &SystemServiceServerImpl{
		logger:  logger,
		cluster: cluster,
	}
}

func (impl *SystemServiceServerImpl) CreateBucket(ctx context.Context, req *CreateBucketRequest) (*CreateBucketResponse, error) {
	impl.logger.WithFields(map[string]any{"name": req.Name, "driver": req.Driver}).Debugf("CreateBucket request received")

	if req.Name == "" {
		return nil, fmt.Errorf("bucket name is required")
	}

	if req.Driver == "" {
		return nil, fmt.Errorf("bucket driver is required")
	}

	// Convert protobuf Struct to map[string]interface{}
	config := make(map[string]interface{})
	if req.Config != nil {
		config = req.Config.AsMap()
	}

	bucket, err := impl.cluster.CreateBucket(ctx, req.Name, req.Driver, config)
	if err != nil {
		return nil, fmt.Errorf("creating bucket: %w", err)
	}

	cfg, err := structpb.NewStruct(bucket.Config)
	if err != nil {
		return nil, fmt.Errorf("converting bucket config to protobuf Struct: %w", err)
	}

	return &CreateBucketResponse{
		Id:            bucket.ID,
		Name:          bucket.Name,
		Config:        cfg,
		Driver:        bucket.Driver,
		CreatedAt:     timestamppb.New(bucket.CreatedAt.Time),
	}, nil
}

func (impl *SystemServiceServerImpl) DeleteBucket(ctx context.Context, req *DeleteBucketRequest) (*DeleteBucketResponse, error) {
	impl.logger.WithFields(map[string]any{"name": req.Name}).Debugf("DeleteBucket request received")

	if req.Name == "" {
		return nil, fmt.Errorf("bucket name is required")
	}

	if err := impl.cluster.DeleteBucket(ctx, req.Name); err != nil {
		return nil, fmt.Errorf("deleting bucket: %w", err)
	}

	return &DeleteBucketResponse{
		Message: "BucketCluster deleted successfully",
	}, nil
}

func (impl *SystemServiceServerImpl) Snapshot(ctx context.Context, req *SnapshotRequest) (*SnapshotResponse, error) {
	impl.logger.Debugf("Snapshot request received")
	if err := impl.cluster.Snapshot(ctx); err != nil {
		return nil, fmt.Errorf("snapshotting cluster: %w", err)
	}
	return &SnapshotResponse{Message: "Snapshotting completed successfully"}, nil
}

func RegisterSystemService(server *grpc.Server, systemServiceServer SystemServiceServer) {
	RegisterSystemServiceServer(server, systemServiceServer)
}
