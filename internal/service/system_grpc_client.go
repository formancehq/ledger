package service

import (
	"context"

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

func (g *grpcSystemClient) CreateBucket(ctx context.Context, name, driver string, config map[string]interface{}) (*ledger.BucketInfo, error) {
	cfg, err := structpb.NewStruct(config)
	if err != nil {
		return nil, err
	}
	bucket, err := g.client.CreateBucket(ctx, &CreateBucketRequest{
		Name:   name,
		Driver: driver,
		Config: cfg,
	})
	if err != nil {
		return nil, err
	}

	return &ledger.BucketInfo{
		ID:        bucket.Id,
		Name:      bucket.Name,
		Driver:    bucket.Driver,
		Config:    bucket.Config.AsMap(),
		CreatedAt: time.New(bucket.CreatedAt.AsTime()),
	}, nil
}

func (g *grpcSystemClient) DeleteBucket(ctx context.Context, name string) error {
	_, err := g.client.DeleteBucket(ctx, &DeleteBucketRequest{Name: name})
	return err
}

var _ SystemWriter = (*grpcSystemClient)(nil)

func NewGrpcSystemClient(client SystemServiceClient) *grpcSystemClient {
	return &grpcSystemClient{client}
}
