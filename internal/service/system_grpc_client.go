package service

import (
	"context"
	"encoding/json"
	"fmt"

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

func (g *grpcSystemClient) ResolveLedger(ctx context.Context, ledgerName string) (string, uint64, error) {
	resp, err := g.client.ResolveLedger(ctx, &ResolveLedgerRequest{LedgerName: ledgerName})
	if err != nil {
		return "", 0, err
	}
	return resp.BucketName, resp.BucketId, nil
}

func (g *grpcSystemClient) GetAllBucketsInfo(ctx context.Context) map[string]ledger.BucketInfo {
	resp, err := g.client.GetAllBucketsInfo(ctx, &GetAllBucketsRequest{})
	if err != nil {
		// Return empty map on error - this is a limitation of the interface
		// In practice, this should not happen as GetAllBucketsInfo is typically called locally
		return make(map[string]ledger.BucketInfo)
	}

	// Convert []*CreateBucketResponse to map[string]ledger.BucketInfo
	result := make(map[string]ledger.BucketInfo, len(resp.Buckets))
	for _, bucketResp := range resp.Buckets {
		// Convert protobuf Struct to json.RawMessage
		configJSON, err := json.Marshal(bucketResp.Config.AsMap())
		if err != nil {
			// Skip this bucket if config conversion fails
			continue
		}

		bucketInfo := ledger.BucketInfo{
			ID:        bucketResp.Id,
			Name:      bucketResp.Name,
			Driver:    bucketResp.Driver,
			Config:    configJSON,
			CreatedAt: time.New(bucketResp.CreatedAt.AsTime()),
		}
		if bucketResp.SnapshotThreshold > 0 {
			bucketInfo.SnapshotThreshold = bucketResp.SnapshotThreshold
		}

		result[bucketInfo.Name] = bucketInfo
	}

	return result
}

func (g *grpcSystemClient) GetBucketInfo(ctx context.Context, name string) (*ledger.BucketInfo, error) {
	resp, err := g.client.GetBucketInfo(ctx, &GetBucketByNameRequest{Name: name})
	if err != nil {
		return nil, err
	}

	// Convert protobuf Struct to json.RawMessage
	configJSON, err := json.Marshal(resp.Config.AsMap())
	if err != nil {
		return nil, fmt.Errorf("marshaling bucket config: %w", err)
	}

	bucketInfo := ledger.BucketInfo{
		ID:        resp.Id,
		Name:      resp.Name,
		Driver:    resp.Driver,
		Config:    configJSON,
		CreatedAt: time.New(resp.CreatedAt.AsTime()),
	}
	if resp.SnapshotThreshold > 0 {
		bucketInfo.SnapshotThreshold = resp.SnapshotThreshold
	}

	return &bucketInfo, nil
}

var _ System = (*grpcSystemClient)(nil)

func NewGrpcSystemClient(client SystemServiceClient) *grpcSystemClient {
	return &grpcSystemClient{client}
}
