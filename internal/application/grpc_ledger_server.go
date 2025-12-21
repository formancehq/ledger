package application

import (
	"context"
	"fmt"
	"io"
	"math/big"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft/system"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type LedgerServiceServerImpl struct {
	ledgerpb.UnimplementedLedgerServiceServer
	logger     logging.Logger
	systemNode *system.Node
}

func NewLedgerServiceServer(logger logging.Logger, systemNode *system.Node) ledgerpb.LedgerServiceServer {
	return &LedgerServiceServerImpl{
		logger:     logger,
		systemNode: systemNode,
	}
}

func (impl *LedgerServiceServerImpl) Snapshot(ctx context.Context, req *ledgerpb.LedgerSnapshotRequest) (*ledgerpb.LedgerSnapshotResponse, error) {
	ledgerNode, err := impl.systemNode.GetLedgerNode(ctx, req.Ledger)
	if err != nil {
		return nil, err
	}

	if err := ledgerNode.Snapshot(ctx); err != nil {
		return nil, err
	}

	return &ledgerpb.LedgerSnapshotResponse{
		Message: "Snapshotting completed successfully",
	}, nil
}

func (impl *LedgerServiceServerImpl) CreateTransaction(ctx context.Context, req *ledgerpb.CreateTransactionRequest) (*ledgerpb.CreateTransactionResponse, error) {
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
			accountMetadata[addr] = service.StructToMetadata(md)
		}
	}

	// Convert metadata
	var txMetadata metadata.Metadata
	if req.Metadata != nil {
		txMetadata = service.StructToMetadata(req.Metadata)
	}

	// Convert timestamp
	var timestamp *time.Time
	if req.Timestamp != nil {
		t := time.New(req.Timestamp.AsTime())
		timestamp = &t
	}

	// Convert script if provided
	var script *service.TransactionScript
	if req.Script != nil {
		script = &service.TransactionScript{
			Plain: req.Script.Plain,
			Vars:  req.Script.Vars,
		}
	}

	// Create transaction parameters
	params := service.Parameters[service.CreateTransaction]{
		DryRun:         req.DryRun,
		IdempotencyKey: req.IdempotencyKey,
		Input: service.CreateTransaction{
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

	ledgerNode, err := impl.systemNode.GetLedgerNode(ctx, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("getting ledger '%s': %w", ledgerName, err)
	}

	// Call ledger service
	_, createdTx, err := ledgerNode.CreateTransaction(ctx, ledgerName, params)
	if err != nil {
		return nil, fmt.Errorf("creating transaction: %w", err)
	}

	// Convert response to protobuf
	response := &ledgerpb.CreateTransactionResponse{
		Transaction:     transactionToProto(createdTx.Transaction),
		AccountMetadata: metadataMapToProto(createdTx.AccountMetadata),
	}

	return response, nil
}

func (impl *LedgerServiceServerImpl) StreamLogs(req *ledgerpb.StreamLogsRequest, stream ledgerpb.LedgerService_StreamLogsServer) error {
	ctx := stream.Context()

	ledgerNode, err := impl.systemNode.GetLedgerNode(ctx, req.GetLedger())
	if err != nil {
		return err
	}

	cursor, err := ledgerNode.GetAllLogs(ctx, req.FromSequence, req.ToSequence)
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

		logProto, err := logToLedgerProto(log)
		if err != nil {
			return fmt.Errorf("converting log to proto: %w", err)
		}

		if err := stream.Send(&ledgerpb.StreamLogsResponse{
			Log: logProto,
		}); err != nil {
			return fmt.Errorf("sending log: %w", err)
		}
	}

	return nil
}

func (impl *LedgerServiceServerImpl) SaveAccountMetadata(ctx context.Context, req *ledgerpb.SaveAccountMetadataRequest) (*ledgerpb.SaveAccountMetadataResponse, error) {
	impl.logger.WithFields(map[string]any{"address": req.Address}).Debugf("SaveAccountMetadata request received")

	// Validate request
	if req.Address == "" {
		return nil, fmt.Errorf("account address is required")
	}
	if req.Metadata == nil {
		return nil, fmt.Errorf("metadata is required")
	}

	// Convert metadata
	accountMetadata := service.StructToMetadata(req.Metadata)

	// Extract ledger name from request
	ledgerName := req.Ledger
	if ledgerName == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	ledgerNode, err := impl.systemNode.GetLedgerNode(ctx, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("getting ledger '%s': %w", ledgerName, err)
	}

	// Create parameters
	params := service.Parameters[service.SaveAccountMetadata]{
		DryRun:         req.DryRun,
		IdempotencyKey: req.IdempotencyKey,
		Input: service.SaveAccountMetadata{
			Address:  req.Address,
			Metadata: accountMetadata,
		},
	}

	// Call ledger service
	log, err := ledgerNode.SaveAccountMetadata(ctx, ledgerName, params)
	if err != nil {
		return nil, fmt.Errorf("saving account metadata: %w", err)
	}

	// Convert log to protobuf
	logProto, err := logToLedgerProto(*log)
	if err != nil {
		return nil, fmt.Errorf("converting log to proto: %w", err)
	}

	return &ledgerpb.SaveAccountMetadataResponse{
		Log: logProto,
	}, nil
}

func RegisterLedgerService(server *grpc.Server, ledgerServiceServer ledgerpb.LedgerServiceServer) {
	ledgerpb.RegisterLedgerServiceServer(server, ledgerServiceServer)
}

// logToLedgerProto converts a ledger.Log to ledger.proto Log
func logToLedgerProto(l ledger.Log) (*ledgerpb.Log, error) {
	logProto := &ledgerpb.Log{
		Type:            int32(l.Type),
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
	logPayloadProto, err := logPayloadToLedgerProto(l.Data)
	if err != nil {
		return nil, fmt.Errorf("converting log payload to proto: %w", err)
	}
	logProto.Data = logPayloadProto

	return logProto, nil
}

// logPayloadToLedgerProto converts a ledger.LogPayload to ledger.proto LogPayload
func logPayloadToLedgerProto(payload ledger.LogPayload) (*ledgerpb.LogPayload, error) {
	switch p := payload.(type) {
	case *ledger.CreatedTransaction:
		return &ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_CreatedTransaction{
				CreatedTransaction: &ledgerpb.CreatedTransaction{
					Transaction:     transactionToProto(p.Transaction),
					AccountMetadata: metadataMapToProto(p.AccountMetadata),
				},
			},
		}, nil
	case *ledger.RevertedTransaction:
		return &ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_RevertedTransaction{
				RevertedTransaction: &ledgerpb.RevertedTransaction{
					RevertedTransaction: transactionToProto(p.RevertedTransaction),
					RevertTransaction:   transactionToProto(p.RevertTransaction),
				},
			},
		}, nil
	case *ledger.SavedMetadata:
		mdStruct, _ := service.MetadataToStruct(p.Metadata)
		proto := &ledgerpb.SavedMetadata{
			TargetType: p.TargetType,
			Metadata:   mdStruct,
		}
		switch id := p.TargetID.(type) {
		case string:
			proto.TargetId = &ledgerpb.SavedMetadata_AccountId{AccountId: id}
		case uint64:
			proto.TargetId = &ledgerpb.SavedMetadata_TransactionId{TransactionId: id}
		}
		return &ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_SavedMetadata{
				SavedMetadata: proto,
			},
		}, nil
	case *ledger.DeletedMetadata:
		proto := &ledgerpb.DeletedMetadata{
			TargetType: p.TargetType,
			Key:        p.Key,
		}
		switch id := p.TargetID.(type) {
		case string:
			proto.TargetId = &ledgerpb.DeletedMetadata_AccountId{AccountId: id}
		case uint64:
			proto.TargetId = &ledgerpb.DeletedMetadata_TransactionId{TransactionId: id}
		}
		return &ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_DeletedMetadata{
				DeletedMetadata: proto,
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown log payload type: %#T", payload)
	}
}

func transactionToProto(tx ledger.Transaction) *ledgerpb.Transaction {
	postings := make([]*ledgerpb.Posting, 0, len(tx.Postings))
	for _, p := range tx.Postings {
		postings = append(postings, &ledgerpb.Posting{
			Source:      p.Source,
			Destination: p.Destination,
			Amount:      p.Amount.String(),
			Asset:       p.Asset,
		})
	}

	var metadata *structpb.Struct
	if len(tx.Metadata) > 0 {
		if md, err := service.MetadataToStruct(tx.Metadata); err == nil {
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

	return &ledgerpb.Transaction{
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
		if s, err := service.MetadataToStruct(v); err == nil {
			result[k] = s
		}
	}
	return result
}
