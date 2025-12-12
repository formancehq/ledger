package service

import (
	"context"
	"encoding/json"

	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"google.golang.org/protobuf/types/known/structpb"
)

type grpcSystemClient struct {
	client SystemServiceClient
}

func (g *grpcSystemClient) Snapshot(ctx context.Context) error {
	_, err := g.client.Snapshot(ctx, &SnapshotRequest{})
	return err
}

func (g *grpcSystemClient) CreateBucket(ctx context.Context, name, driver string, config map[string]interface{}, snapshotThreshold *uint64) (*ledger.BucketInfo, error) {
	cfg, err := structpb.NewStruct(config)
	if err != nil {
		return nil, err
	}
	req := &CreateBucketRequest{
		Name:   name,
		Driver: driver,
		Config: cfg,
	}
	if snapshotThreshold != nil && *snapshotThreshold > 0 {
		req.SnapshotThreshold = *snapshotThreshold
	}
	bucket, err := g.client.CreateBucket(ctx, req)
	if err != nil {
		return nil, err
	}

	// Convert protobuf Struct to json.RawMessage
	configJSON, err := json.Marshal(bucket.Config.AsMap())
	if err != nil {
		return nil, err
	}

	result := &ledger.BucketInfo{
		ID:        bucket.Id,
		Name:      bucket.Name,
		Driver:    bucket.Driver,
		Config:    configJSON,
		CreatedAt: time.New(bucket.CreatedAt.AsTime()),
	}
	if bucket.SnapshotThreshold > 0 {
		result.SnapshotThreshold = bucket.SnapshotThreshold
	}
	return result, nil
}

func (g *grpcSystemClient) DeleteBucket(ctx context.Context, name string) error {
	_, err := g.client.DeleteBucket(ctx, &DeleteBucketRequest{Name: name})
	return err
}

var _ SystemWriter = (*grpcSystemClient)(nil)

func NewGrpcSystemClient(client SystemServiceClient) *grpcSystemClient {
	return &grpcSystemClient{client}
}
