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
	UnimplementedLedgerServiceServer
	logger        logging.Logger
	ledgerService Ledger
}

func NewLedgerServiceServer(logger logging.Logger, ledgerService Ledger) LedgerServiceServer {
	return &LedgerServiceServerImpl{
		logger:        logger,
		ledgerService: ledgerService,
	}
}

func (l *LedgerServiceServerImpl) CreateTransaction(ctx context.Context, req *CreateTransactionRequest) (*CreateTransactionResponse, error) {
	l.logger.WithFields(map[string]any{"reference": req.Reference}).Debugf("CreateTransaction request received")

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
		},
	}

	// Extract ledger name from request
	ledgerName := req.Ledger
	if ledgerName == "" {
		return nil, fmt.Errorf("ledger name is required")
	}

	// Call ledger service
	_, createdTx, err := l.ledgerService.CreateTransaction(ctx, ledgerName, params)
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

func RegisterLedgerService(server *grpc.Server, ledgerServiceServer LedgerServiceServer) {
	RegisterLedgerServiceServer(server, ledgerServiceServer)
}
