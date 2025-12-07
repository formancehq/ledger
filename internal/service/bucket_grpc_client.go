package service

import (
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// BucketGrpcClient implements Ledger by forwarding requests via gRPC to the leader
type BucketGrpcClient struct {
	client BucketServiceClient
	name   string
}

// newGRPCLedger creates a new gRPC-based ledger implementation
func NewBucketGrpcClient(name string, client BucketServiceClient) *BucketGrpcClient {
	return &BucketGrpcClient{
		client: client,
		name:   name,
	}
}

func (g *BucketGrpcClient) Snapshot(ctx context.Context) error {
	_, err := g.client.Snapshot(ctx, &BucketSnapshotRequest{
		Bucket: g.name,
	})
	return err
}

// CreateTransaction forwards the request via gRPC to the leader
func (g *BucketGrpcClient) CreateTransaction(ctx context.Context, ledgerName string, parameters Parameters[CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, error) {

	// Convert service parameters to protobuf request
	req, err := g.createTransactionRequestToProto(ledgerName, parameters)
	if err != nil {
		return nil, nil, fmt.Errorf("converting request to protobuf: %w", err)
	}

	// Call leader via gRPC
	resp, err := g.client.CreateTransaction(ctx, req)
	if err != nil {
		return nil, nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	// Convert protobuf response to service types
	log, createdTx, err := g.createTransactionResponseFromProto(resp)
	if err != nil {
		return nil, nil, fmt.Errorf("converting response from protobuf: %w", err)
	}

	return log, createdTx, nil
}

func (g *BucketGrpcClient) CreateLedger(ctx context.Context, name string, metadata metadata.Metadata) (*ledger.LedgerInfo, error) {
	md, err := metadataToStruct(metadata)
	if err != nil {
		return nil, fmt.Errorf("converting metadata to protobuf: %w", err)
	}
	ret, err := g.client.CreateLedger(ctx, &CreateLedgerRequest{
		Name:     name,
		Metadata: md,
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}
	return &ledger.LedgerInfo{
		ID:   ret.Id,
		Name: ret.Name,
	}, nil
}

func (g *BucketGrpcClient) RevertTransaction(ctx context.Context, ledgerName string, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	return nil, nil, ErrNotFound
}

func (g *BucketGrpcClient) SaveTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

func (g *BucketGrpcClient) SaveAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

func (g *BucketGrpcClient) DeleteTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

func (g *BucketGrpcClient) DeleteAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

func (g *BucketGrpcClient) Import(ctx context.Context, ledgerName string, stream chan ledger.Log) error {
	return ErrNotFound
}

func (g *BucketGrpcClient) Export(ctx context.Context, ledgerName string, w ExportWriter) error {
	return ErrNotFound
}

func (g *BucketGrpcClient) createTransactionRequestToProto(ledgerName string, params Parameters[CreateTransaction]) (*CreateTransactionRequest, error) {
	input := params.Input

	// Convert postings
	postings := make([]*Posting, 0, len(input.Postings))
	for _, p := range input.Postings {
		postings = append(postings, &Posting{
			Source:      p.Source,
			Destination: p.Destination,
			Amount:      p.Amount.String(),
			Asset:       p.Asset,
		})
	}

	// Convert account metadata
	accountMetadata := make(map[string]*structpb.Struct)
	for addr, md := range input.AccountMetadata {
		if s, err := metadataToStruct(md); err == nil {
			accountMetadata[addr] = s
		}
	}

	// Convert metadata
	var metadata *structpb.Struct
	if len(input.Metadata) > 0 {
		if md, err := metadataToStruct(input.Metadata); err == nil {
			metadata = md
		}
	}

	// Convert timestamp
	var timestamp *timestamppb.Timestamp
	if input.Timestamp != nil && !input.Timestamp.IsZero() {
		timestamp = timestamppb.New(input.Timestamp.Time)
	}

	return &CreateTransactionRequest{
		Bucket:          g.name,
		AccountMetadata: accountMetadata,
		Timestamp:       timestamp,
		Metadata:        metadata,
		Reference:       input.Reference,
		Postings:        postings,
		DryRun:          params.DryRun,
		IdempotencyKey:  params.IdempotencyKey,
		Ledger:          ledgerName,
	}, nil
}

func (g *BucketGrpcClient) createTransactionResponseFromProto(resp *CreateTransactionResponse) (*ledger.Log, *ledger.CreatedTransaction, error) {
	if resp.Transaction == nil {
		return nil, nil, fmt.Errorf("empty transaction in response")
	}

	// Convert transaction
	tx := ledger.NewTransaction()

	// Convert postings
	postings := make(ledger.Postings, 0, len(resp.Transaction.Postings))
	for _, p := range resp.Transaction.Postings {
		amount, ok := new(big.Int).SetString(p.Amount, 10)
		if !ok {
			return nil, nil, fmt.Errorf("invalid amount: %s", p.Amount)
		}
		postings = append(postings, ledger.NewPosting(p.Source, p.Destination, p.Asset, amount))
	}
	tx = tx.WithPostings(postings...)

	// Convert metadata
	if resp.Transaction.Metadata != nil {
		tx = tx.WithMetadata(structToMetadata(resp.Transaction.Metadata))
	}

	// Convert timestamp
	if resp.Transaction.Timestamp != nil {
		tx = tx.WithTimestamp(time.New(resp.Transaction.Timestamp.AsTime()))
	}

	// Convert reference
	if resp.Transaction.Reference != "" {
		tx = tx.WithReference(resp.Transaction.Reference)
	}

	// Convert account metadata
	accountMetadata := make(ledger.AccountMetadata)
	for addr, md := range resp.AccountMetadata {
		if md != nil {
			accountMetadata[addr] = structToMetadata(md)
		}
	}

	createdTx := &ledger.CreatedTransaction{
		Transaction:     tx,
		AccountMetadata: accountMetadata,
	}

	// Create a log (we don't have the full log from the response, so we create a minimal one)
	log := ledger.NewLog(createdTx)
	if resp.Transaction.Id != 0 {
		log = log.WithID(resp.Transaction.Id)
		// Assign log ID to transaction
		createdTx.Transaction = createdTx.Transaction.WithID(resp.Transaction.Id)
	}

	return &log, createdTx, nil
}
