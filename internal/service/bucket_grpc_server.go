package service

import (
	"context"
	"fmt"
	"io"
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

func (impl *LedgerServiceServerImpl) StreamLogs(req *StreamLogsRequest, stream BucketService_StreamLogsServer) error {
	ctx := stream.Context()

	// Get bucket
	bucket, err := impl.cluster.GetBucket(ctx, req.Bucket)
	if err != nil {
		return fmt.Errorf("getting bucket: %w", err)
	}

	// Get LogReader from bucket
	// Try to get it from the bucket node (if it's a local node)
	var logReader LogReader
	if bucketNode, ok := bucket.(interface{ GetLogReader() LogReader }); ok {
		logReader = bucketNode.GetLogReader()
	} else {
		// If bucket is a gRPC client, we need to use the client's StreamLogs method
		// For now, return error as we need direct access to LogReader
		return fmt.Errorf("bucket does not support direct log reading, use gRPC client")
	}

	// Get all logs from the bucket (all ledgers), starting from fromSequence if specified
	cursorPtr, err := logReader.GetAllLogs(ctx, req.FromSequence)
	if err != nil {
		return fmt.Errorf("getting logs: %w", err)
	}
	if cursorPtr == nil {
		return nil
	}
	cursor := *cursorPtr
	defer func() {
		_ = cursor.Close()
	}()

	// Stream logs (already filtered by sequence in GetAllLogs)
	for {
		log, err := cursor.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("reading log: %w", err)
		}

		logProto, err := logToBucketProto(log)
		if err != nil {
			return fmt.Errorf("converting log to proto: %w", err)
		}

		if err := stream.Send(&StreamLogsResponse{
			Log: logProto,
		}); err != nil {
			return fmt.Errorf("sending log: %w", err)
		}
	}

	return nil
}

// logToBucketProto converts a ledger.Log to bucket.proto Log
func logToBucketProto(l ledger.Log) (*Log, error) {
	logProto := &Log{
		Type:            int32(l.Type),
		Ledger:          l.Ledger,
		IdempotencyKey:  l.IdempotencyKey,
		IdempotencyHash: l.IdempotencyHash,
		Sequence:        l.Sequence,
	}

	if l.ID != nil {
		logProto.Id = *l.ID
	}

	if !l.Date.IsZero() {
		logProto.Date = timestamppb.New(l.Date.Time)
	}

	// Convert LogPayload to protobuf
	logPayloadProto, err := logPayloadToBucketProto(l.Data)
	if err != nil {
		return nil, fmt.Errorf("converting log payload to proto: %w", err)
	}
	logProto.Data = logPayloadProto

	return logProto, nil
}

// logPayloadToBucketProto converts a ledger.LogPayload to bucket.proto LogPayload
func logPayloadToBucketProto(payload ledger.LogPayload) (*LogPayload, error) {
	switch p := payload.(type) {
	case *ledger.CreatedTransaction:
		return &LogPayload{
			Payload: &LogPayload_CreatedTransaction{
				CreatedTransaction: transactionToProto(p.Transaction),
			},
		}, nil
	case *ledger.RevertedTransaction:
		return &LogPayload{
			Payload: &LogPayload_RevertedTransaction{
				RevertedTransaction: &RevertedTransaction{
					RevertedTransaction: transactionToProto(p.RevertedTransaction),
					RevertTransaction:   transactionToProto(p.RevertTransaction),
				},
			},
		}, nil
	case *ledger.SavedMetadata:
		mdStruct, _ := metadataToStruct(p.Metadata)
		proto := &SavedMetadata{
			TargetType: p.TargetType,
			Metadata:   mdStruct,
		}
		switch id := p.TargetID.(type) {
		case string:
			proto.TargetId = &SavedMetadata_AccountId{AccountId: id}
		case uint64:
			proto.TargetId = &SavedMetadata_TransactionId{TransactionId: id}
		}
		return &LogPayload{
			Payload: &LogPayload_SavedMetadata{
				SavedMetadata: proto,
			},
		}, nil
	case *ledger.DeletedMetadata:
		proto := &DeletedMetadata{
			TargetType: p.TargetType,
			Key:        p.Key,
		}
		switch id := p.TargetID.(type) {
		case string:
			proto.TargetId = &DeletedMetadata_AccountId{AccountId: id}
		case uint64:
			proto.TargetId = &DeletedMetadata_TransactionId{TransactionId: id}
		}
		return &LogPayload{
			Payload: &LogPayload_DeletedMetadata{
				DeletedMetadata: proto,
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown log payload type: %T", payload)
	}
}

func RegisterBucketService(server *grpc.Server, ledgerServiceServer BucketServiceServer) {
	RegisterBucketServiceServer(server, ledgerServiceServer)
}
