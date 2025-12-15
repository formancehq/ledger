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

type BucketServiceServerImpl struct {
	UnimplementedBucketServiceServer
	logger  logging.Logger
	cluster MasterCluster
}

func NewBucketServiceServer(logger logging.Logger, cluster MasterCluster) BucketServiceServer {
	return &BucketServiceServerImpl{
		logger:  logger,
		cluster: cluster,
	}
}

func (impl *BucketServiceServerImpl) Snapshot(ctx context.Context, req *BucketSnapshotRequest) (*BucketSnapshotResponse, error) {
	bucket, err := impl.cluster.GetBucketCluster(ctx, req.Bucket)
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

func (impl *BucketServiceServerImpl) CreateLedger(ctx context.Context, req *CreateLedgerRequest) (*CreateLedgerResponse, error) {
	bucket, err := impl.cluster.GetBucketCluster(ctx, req.Bucket)
	if err != nil {
		return nil, fmt.Errorf("getting bucket '%s': %w", req.Bucket, err)
	}

	ledgerInfo, err := bucket.CreateLedger(ctx, req.Name, structToMetadata(req.Metadata))
	if err != nil {
		return nil, fmt.Errorf("creating ledger: %w", err)
	}

	return &CreateLedgerResponse{
		Id:     ledgerInfo.ID,
		Name:   ledgerInfo.Name,
		Bucket: req.Bucket,
	}, nil
}

// todo: use bucket name from request
func (impl *BucketServiceServerImpl) CreateTransaction(ctx context.Context, req *CreateTransactionRequest) (*CreateTransactionResponse, error) {
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

	bucketName, _, err := impl.cluster.ResolveLedger(ctx, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("resolving ledger '%s': %w", ledgerName, err)
	}

	bucket, err := impl.cluster.GetBucketCluster(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("getting bucket of ledger '%s': %w", ledgerName, err)
	}

	// Call ledger service
	_, createdTx, err := bucket.CreateTransaction(ctx, ledgerName, params)
	if err != nil {
		return nil, fmt.Errorf("creating transaction: %w", err)
	}

	// Convert response to protobuf
	response := &CreateTransactionResponse{
		Transaction:     transactionToProto(createdTx.Transaction),
		AccountMetadata: metadataMapToProto(createdTx.AccountMetadata),
	}

	return response, nil
}

func (impl *BucketServiceServerImpl) StreamLogs(req *StreamLogsRequest, stream BucketService_StreamLogsServer) error {
	ctx := stream.Context()

	cluster, err := impl.cluster.GetBucketCluster(ctx, req.Bucket)
	if err != nil {
		return err
	}

	cursor, err := cluster.GetAllLogs(ctx, req.FromSequence, req.ToSequence)
	if err != nil {
		return err
	}
	defer func() {
		_ = cursor.Close()
	}()

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

func (impl *BucketServiceServerImpl) GetLedger(ctx context.Context, req *GetLedgerRequest) (*GetLedgerResponse, error) {
	impl.logger.WithFields(map[string]any{"bucket": req.Bucket, "ledger": req.Name}).Debugf("GetLedger request received")

	if req.Bucket == "" {
		return nil, fmt.Errorf("bucket name is required")
	}

	if req.Name == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	bucket, err := impl.cluster.GetBucketCluster(ctx, req.Bucket)
	if err != nil {
		return nil, fmt.Errorf("getting bucket '%s': %w", req.Bucket, err)
	}

	ledgerInfo, err := bucket.GetLedger(ctx, req.Name)
	if err != nil {
		return nil, fmt.Errorf("getting ledger '%s': %w", req.Name, err)
	}

	// Convert metadata.Metadata to protobuf Struct
	var metadataStruct *structpb.Struct
	if len(ledgerInfo.Metadata) > 0 {
		metadataMap := make(map[string]interface{})
		for k, v := range ledgerInfo.Metadata {
			metadataMap[k] = v
		}
		var err error
		metadataStruct, err = structpb.NewStruct(metadataMap)
		if err != nil {
			return nil, fmt.Errorf("converting metadata to protobuf Struct: %w", err)
		}
	}

	return &GetLedgerResponse{
		Id:        ledgerInfo.ID,
		Name:      ledgerInfo.Name,
		CreatedAt: timestamppb.New(ledgerInfo.CreatedAt.Time),
		Metadata:  metadataStruct,
	}, nil
}

func (impl *BucketServiceServerImpl) GetLedgers(ctx context.Context, req *GetLedgersRequest) (*GetLedgersResponse, error) {
	impl.logger.WithFields(map[string]any{"bucket": req.Bucket}).Debugf("GetLedgers request received")

	if req.Bucket == "" {
		return nil, fmt.Errorf("bucket name is required")
	}

	bucket, err := impl.cluster.GetBucketCluster(ctx, req.Bucket)
	if err != nil {
		return nil, fmt.Errorf("getting bucket '%s': %w", req.Bucket, err)
	}

	ledgers, err := bucket.GetLedgers(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting ledgers: %w", err)
	}

	// Convert []ledger.LedgerInfo to []GetLedgerResponse
	ledgersList := make([]*GetLedgerResponse, 0, len(ledgers))
	for _, ledgerInfo := range ledgers {
		// Convert metadata.Metadata to protobuf Struct
		var metadataStruct *structpb.Struct
		if len(ledgerInfo.Metadata) > 0 {
			metadataMap := make(map[string]interface{})
			for k, v := range ledgerInfo.Metadata {
				metadataMap[k] = v
			}
			var err error
			metadataStruct, err = structpb.NewStruct(metadataMap)
			if err != nil {
				return nil, fmt.Errorf("converting metadata for ledger '%s' to protobuf Struct: %w", ledgerInfo.Name, err)
			}
		}

		ledgersList = append(ledgersList, &GetLedgerResponse{
			Id:        ledgerInfo.ID,
			Name:      ledgerInfo.Name,
			CreatedAt: timestamppb.New(ledgerInfo.CreatedAt.Time),
			Metadata:  metadataStruct,
		})
	}

	return &GetLedgersResponse{
		Ledgers: ledgersList,
	}, nil
}

func RegisterBucketService(server *grpc.Server, ledgerServiceServer BucketServiceServer) {
	RegisterBucketServiceServer(server, ledgerServiceServer)
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
	case ledger.CreatedTransaction:
		return &LogPayload{
			Payload: &LogPayload_CreatedTransaction{
				CreatedTransaction: transactionToProto(p.Transaction),
			},
		}, nil
	case ledger.RevertedTransaction:
		return &LogPayload{
			Payload: &LogPayload_RevertedTransaction{
				RevertedTransaction: &RevertedTransaction{
					RevertedTransaction: transactionToProto(p.RevertedTransaction),
					RevertTransaction:   transactionToProto(p.RevertTransaction),
				},
			},
		}, nil
	case ledger.SavedMetadata:
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
	case ledger.DeletedMetadata:
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
