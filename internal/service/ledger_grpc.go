package service

import (
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)


// grpcLedger implements Ledger by forwarding requests via gRPC to the leader
type grpcLedger struct {
	cluster ClusterClient
	logger  *zap.Logger
}

// newGRPCLedger creates a new gRPC-based ledger implementation
func newGRPCLedger(cluster ClusterClient, logger *zap.Logger) *grpcLedger {
	return &grpcLedger{
		cluster: cluster,
		logger:  logger,
	}
}

// CreateTransaction forwards the request via gRPC to the leader
func (g *grpcLedger) CreateTransaction(ctx context.Context, ledgerName string, parameters Parameters[CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	g.logger.Debug("Forwarding transaction creation to leader via gRPC")

	client := g.cluster.GetLeaderGRPCClient()
	if client == nil {
		return nil, nil, fmt.Errorf("not connected to leader gRPC server")
	}

	// Convert service parameters to protobuf request
	req, err := g.createTransactionRequestToProto(ledgerName, parameters)
	if err != nil {
		return nil, nil, fmt.Errorf("converting request to protobuf: %w", err)
	}

	// Call leader via gRPC
	resp, err := client.CreateTransaction(ctx, req)
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

// Helper functions for conversion

func (g *grpcLedger) createTransactionRequestToProto(ledgerName string, params Parameters[CreateTransaction]) (*CreateTransactionRequest, error) {
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

func (g *grpcLedger) createTransactionResponseFromProto(resp *CreateTransactionResponse) (*ledger.Log, *ledger.CreatedTransaction, error) {
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

func structToMetadata(s *structpb.Struct) metadata.Metadata {
	if s == nil {
		return metadata.Metadata{}
	}
	md := make(metadata.Metadata)
	for k, v := range s.Fields {
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

// Stub methods for grpcLedger (other methods not yet implemented via gRPC)
func (g *grpcLedger) RevertTransaction(ctx context.Context, ledgerName string, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	return nil, nil, ErrNotFound
}

func (g *grpcLedger) SaveTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

func (g *grpcLedger) SaveAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

func (g *grpcLedger) DeleteTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

func (g *grpcLedger) DeleteAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

func (g *grpcLedger) Import(ctx context.Context, ledgerName string, stream chan ledger.Log) error {
	return ErrNotFound
}

func (g *grpcLedger) Export(ctx context.Context, ledgerName string, w ExportWriter) error {
	return ErrNotFound
}
