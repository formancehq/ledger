package grpc

import (
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ledgerServiceServer implements the LedgerService
type ledgerServiceServer struct {
	service.UnimplementedLedgerServiceServer
	logger        *zap.Logger
	ledgerService service.Ledger
}

// newLedgerServiceServer creates a new ledger service server
func newLedgerServiceServer(logger *zap.Logger, ledgerService service.Ledger) *ledgerServiceServer {
	return &ledgerServiceServer{
		logger:        logger,
		ledgerService: ledgerService,
	}
}

func (l *ledgerServiceServer) CreateTransaction(ctx context.Context, req *service.CreateTransactionRequest) (*service.CreateTransactionResponse, error) {
	l.logger.Debug("CreateTransaction request received", zap.String("reference", req.Reference))

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
	var timestamp time.Time
	if req.Timestamp != nil {
		timestamp = time.New(req.Timestamp.AsTime())
	} else {
		timestamp = time.Now()
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
		},
	}

	// Extract ledger name from request
	ledgerName := req.Ledger
	if ledgerName == "" {
		ledgerName = "default" // Default ledger name for backward compatibility
	}

	// Call ledger service
	_, createdTx, err := l.ledgerService.CreateTransaction(ctx, ledgerName, params)
	if err != nil {
		return nil, err
	}

	// Convert response to protobuf
	response := &service.CreateTransactionResponse{
		Transaction:     transactionToProto(createdTx.Transaction),
		AccountMetadata: metadataMapToProto(createdTx.AccountMetadata),
	}

	return response, nil
}

// Helper functions for conversion
func structToMetadata(s *structpb.Struct) metadata.Metadata {
	if s == nil {
		return metadata.Metadata{}
	}
	md := make(metadata.Metadata)
	for k, v := range s.Fields {
		// Convert protobuf value to string
		// metadata.Metadata is map[string]string
		md[k] = v.GetStringValue()
	}
	return md
}

func metadataToStruct(md metadata.Metadata) (*structpb.Struct, error) {
	if len(md) == 0 {
		return nil, nil
	}
	fields := make(map[string]*structpb.Value)
	for k, v := range md {
		val, err := structpb.NewValue(v)
		if err != nil {
			return nil, err
		}
		fields[k] = val
	}
	return &structpb.Struct{Fields: fields}, nil
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

func transactionToProto(tx ledger.Transaction) *service.Transaction {
	postings := make([]*service.Posting, 0, len(tx.Postings))
	for _, p := range tx.Postings {
		postings = append(postings, &service.Posting{
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

	return &service.Transaction{
		Postings:  postings,
		Metadata:  metadata,
		Timestamp: timestamp,
		Reference: tx.Reference,
		Id:        id,
	}
}
