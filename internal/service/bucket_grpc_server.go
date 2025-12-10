package service

import (
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type LedgerServiceServerImpl struct {
	UnimplementedBucketServiceServer
	logger  logging.Logger
	cluster MasterCluster
}

func NewLedgerServiceServer(logger logging.Logger, cluster MasterCluster) BucketServiceServer {
	return &LedgerServiceServerImpl{
		logger:  logger,
		cluster: cluster,
	}
}

func (impl *LedgerServiceServerImpl) Snapshot(ctx context.Context, req *BucketSnapshotRequest) (*BucketSnapshotResponse, error) {
	bucket, err := impl.cluster.GetBucket(ctx, req.Bucket)
	if err != nil {
		return nil, err
	}

	if err := bucket.Snapshot(ctx); err != nil {
		return nil, err
	}

	return &BucketSnapshotResponse{
		Message: "Snapshotting completed successfully",
	}, nil
}

// todo: use bucket name from request
func (impl *LedgerServiceServerImpl) CreateTransaction(ctx context.Context, req *CreateTransactionRequest) (*CreateTransactionResponse, error) {
	impl.logger.WithFields(map[string]any{"reference": req.Reference}).Debugf("CreateTransaction request received")

	// Convert protobuf request to service types
	postings := make(ledger.Postings, 0, len(req.Postings))
	for _, p := range req.Postings {
		amount, ok := new(big.Int).SetString(p.Amount, 10)
		if !ok {
			return nil, fmt.Errorf("invalid amount: %s", p.Amount)
		}
		postings = append(postings, ledger.NewPosting(p.Source, p.Destination, p.Asset, amount))
	}

	// Convert account metadata
	accountMetadata := make(map[string]metadata.Metadata)
	for addr, md := range req.AccountMetadata {
		if md != nil {
			accountMetadata[addr] = structToMetadata(md)
		}
	}

	// Convert metadata
	var txMetadata metadata.Metadata
	if req.Metadata != nil {
		txMetadata = structToMetadata(req.Metadata)
	}

	// Convert timestamp
	var timestamp *time.Time
	if req.Timestamp != nil {
		t := time.New(req.Timestamp.AsTime())
		timestamp = &t
	}

	// Convert script if provided
	var script *TransactionScript
	if req.Script != nil {
		script = &TransactionScript{
			Plain: req.Script.Plain,
			Vars:  req.Script.Vars,
		}
	}

	// Create transaction parameters
	params := Parameters[CreateTransaction]{
		DryRun:         req.DryRun,
		IdempotencyKey: req.IdempotencyKey,
		Input: CreateTransaction{
			AccountMetadata: accountMetadata,
			Timestamp:       timestamp,
			Metadata:        txMetadata,
			Reference:       req.Reference,
			Postings:        postings,
			Script:          script,
			Runtime:         req.Runtime,
		},
	}

	// Extract ledger name from request
	ledgerName := req.Ledger
	if ledgerName == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	bucket, err := impl.cluster.GetBucketOfLedger(ctx, ledgerName)
	if err != nil {
		return nil, err
	}

	// Call ledger service
	_, createdTx, err := bucket.CreateTransaction(ctx, ledgerName, params)
	if err != nil {
		return nil, err
	}

	// Convert response to protobuf
	response := &CreateTransactionResponse{
		Transaction:     transactionToProto(createdTx.Transaction),
		AccountMetadata: metadataMapToProto(createdTx.AccountMetadata),
	}

	return response, nil
}

func transactionToProto(tx ledger.Transaction) *Transaction {
	postings := make([]*Posting, 0, len(tx.Postings))
	for _, p := range tx.Postings {
		postings = append(postings, &Posting{
			Source:      p.Source,
			Destination: p.Destination,
			Amount:      p.Amount.String(),
			Asset:       p.Asset,
		})
	}

	var metadata *structpb.Struct
	if len(tx.Metadata) > 0 {
		if md, err := metadataToStruct(tx.Metadata); err == nil {
			metadata = md
		}
	}

	var timestamp *timestamppb.Timestamp
	if !tx.Timestamp.IsZero() {
		timestamp = timestamppb.New(tx.Timestamp.Time)
	}

	var id uint64
	if tx.ID != nil {
		id = *tx.ID
	}

	return &Transaction{
		Postings:  postings,
		Metadata:  metadata,
		Timestamp: timestamp,
		Reference: tx.Reference,
		Id:        id,
	}
}

func metadataMapToProto(md map[string]metadata.Metadata) map[string]*structpb.Struct {
	result := make(map[string]*structpb.Struct)
	for k, v := range md {
		if s, err := metadataToStruct(v); err == nil {
			result[k] = s
		}
	}
	return result
}

func RegisterBucketService(server *grpc.Server, ledgerServiceServer BucketServiceServer) {
	RegisterBucketServiceServer(server, ledgerServiceServer)
}
