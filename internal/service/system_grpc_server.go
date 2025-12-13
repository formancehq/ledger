package service

import (
	"context"
	"encoding/json"
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

	var snapshotThreshold *uint64
	if req.SnapshotThreshold > 0 {
		snapshotThreshold = &req.SnapshotThreshold
	}

	bucket, err := impl.cluster.CreateBucket(ctx, req.Name, req.Driver, config, snapshotThreshold)
	if err != nil {
		return nil, fmt.Errorf("creating bucket: %w", err)
	}

	// Convert json.RawMessage to map[string]interface{} for protobuf conversion
	var configMap map[string]interface{}
	if err := json.Unmarshal(bucket.Config, &configMap); err != nil {
		return nil, fmt.Errorf("unmarshaling bucket config: %w", err)
	}

	cfg, err := structpb.NewStruct(configMap)
	if err != nil {
		return nil, fmt.Errorf("converting bucket config to protobuf Struct: %w", err)
	}

	resp := &CreateBucketResponse{
		Id:        bucket.ID,
		Name:      bucket.Name,
		Config:    cfg,
		Driver:    bucket.Driver,
		CreatedAt: timestamppb.New(bucket.CreatedAt.Time),
	}
	if bucket.SnapshotThreshold > 0 {
		resp.SnapshotThreshold = bucket.SnapshotThreshold
	}
	return resp, nil
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

func (impl *SystemServiceServerImpl) ResolveLedger(ctx context.Context, req *ResolveLedgerRequest) (*ResolveLedgerResponse, error) {
	impl.logger.WithFields(map[string]any{"ledger_name": req.LedgerName}).Debugf("ResolveLedger request received")

	if req.LedgerName == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	bucketName, _, err := impl.cluster.ResolveLedger(ctx, req.LedgerName)
	if err != nil {
		return nil, fmt.Errorf("resolving ledger '%s': %w", req.LedgerName, err)
	}

	bucketInfo, err := impl.cluster.GetBucketInfo(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("resolving ledger '%s': %w", req.LedgerName, err)
	}
	return &ResolveLedgerResponse{
		BucketName: bucketInfo.Name,
		BucketId:   bucketInfo.ID,
	}, nil
}

func (impl *SystemServiceServerImpl) GetAllBucketsInfo(ctx context.Context, req *GetAllBucketsRequest) (*GetAllBucketsResponse, error) {
	impl.logger.Debugf("GetAllBucketsInfo request received")

	buckets := impl.cluster.GetAllBucketsInfo(ctx)

	// Convert map[string]ledger.BucketInfo to []CreateBucketResponse
	bucketsList := make([]*CreateBucketResponse, 0, len(buckets))
	for _, bucketInfo := range buckets {
		// Convert json.RawMessage to map[string]interface{} for protobuf conversion
		var configMap map[string]interface{}
		if len(bucketInfo.Config) > 0 {
			if err := json.Unmarshal(bucketInfo.Config, &configMap); err != nil {
				return nil, fmt.Errorf("unmarshaling bucket config for '%s': %w", bucketInfo.Name, err)
			}
		}

		cfg, err := structpb.NewStruct(configMap)
		if err != nil {
			return nil, fmt.Errorf("converting bucket config to protobuf Struct for '%s': %w", bucketInfo.Name, err)
		}

		bucketResp := &CreateBucketResponse{
			Id:        bucketInfo.ID,
			Name:      bucketInfo.Name,
			Config:    cfg,
			Driver:    bucketInfo.Driver,
			CreatedAt: timestamppb.New(bucketInfo.CreatedAt.Time),
		}
		if bucketInfo.SnapshotThreshold > 0 {
			bucketResp.SnapshotThreshold = bucketInfo.SnapshotThreshold
		}

		bucketsList = append(bucketsList, bucketResp)
	}

	return &GetAllBucketsResponse{
		Buckets: bucketsList,
	}, nil
}

func (impl *SystemServiceServerImpl) GetBucketInfo(ctx context.Context, req *GetBucketByNameRequest) (*GetBucketByNameResponse, error) {
	impl.logger.WithFields(map[string]any{"name": req.Name}).Debugf("GetBucketInfo request received")

	if req.Name == "" {
		return nil, fmt.Errorf("bucket name is required")
	}

	bucketInfo, err := impl.cluster.GetBucketInfo(ctx, req.Name)
	if err != nil {
		return nil, fmt.Errorf("getting bucket '%s': %w", req.Name, err)
	}

	// Convert json.RawMessage to map[string]interface{} for protobuf conversion
	var configMap map[string]interface{}
	if len(bucketInfo.Config) > 0 {
		if err := json.Unmarshal(bucketInfo.Config, &configMap); err != nil {
			return nil, fmt.Errorf("unmarshaling bucket config: %w", err)
		}
	}

	cfg, err := structpb.NewStruct(configMap)
	if err != nil {
		return nil, fmt.Errorf("converting bucket config to protobuf Struct: %w", err)
	}

	resp := &GetBucketByNameResponse{
		Id:        bucketInfo.ID,
		Name:      bucketInfo.Name,
		Config:    cfg,
		Driver:    bucketInfo.Driver,
		CreatedAt: timestamppb.New(bucketInfo.CreatedAt.Time),
	}
	if bucketInfo.SnapshotThreshold > 0 {
		resp.SnapshotThreshold = bucketInfo.SnapshotThreshold
	}

	return resp, nil
}

func RegisterSystemService(server *grpc.Server, systemServiceServer SystemServiceServer) {
	RegisterSystemServiceServer(server, systemServiceServer)
}
