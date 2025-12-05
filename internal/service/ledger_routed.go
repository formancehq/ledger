package service

import (
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/api"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"go.etcd.io/etcd/raft/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RoutedLedger routes requests to the leader, either directly or via gRPC
type RoutedLedger struct {
	cluster       ClusterClient
	nodeID        string
	defaultLedger Ledger
	logger        *zap.Logger
}

// NewRoutedLedger creates a new routed ledger service
func NewRoutedLedger(cluster ClusterClient, nodeID string, defaultLedger Ledger, logger *zap.Logger) *RoutedLedger {
	return &RoutedLedger{
		cluster:       cluster,
		nodeID:        nodeID,
		defaultLedger: defaultLedger,
		logger:        logger,
	}
}

// isLeader checks if the current node is the leader
func (r *RoutedLedger) isLeader() bool {
	raftInstance := r.cluster.GetRaft()
	status := raftInstance.Status()
	return status.RaftState == raft.StateLeader
}

// CreateTransaction creates a new transaction, routing to leader if needed
func (r *RoutedLedger) CreateTransaction(ctx context.Context, ledgerName string, parameters Parameters[CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	if r.isLeader() {
		// We are the leader, call directly
		r.logger.Debug("Node is leader, calling default ledger directly")
		return r.defaultLedger.CreateTransaction(ctx, ledgerName, parameters)
	}

	// We are a follower, forward via gRPC
	r.logger.Debug("Node is follower, forwarding request to leader via gRPC")

	grpcClient := r.cluster.GetGRPCClient()
	if grpcClient == nil {
		return nil, nil, fmt.Errorf("not connected to leader gRPC server")
	}

	client := grpcClient.GetClient()
	if client == nil {
		return nil, nil, fmt.Errorf("gRPC client not available")
	}

	// Convert service parameters to protobuf request
	req, err := r.createTransactionRequestToProto(ledgerName, parameters)
	if err != nil {
		return nil, nil, fmt.Errorf("converting request to protobuf: %w", err)
	}

	// Call leader via gRPC
	resp, err := client.CreateTransaction(ctx, req)
	if err != nil {
		return nil, nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	// Convert protobuf response to service types
	log, createdTx, err := r.createTransactionResponseFromProto(resp)
	if err != nil {
		return nil, nil, fmt.Errorf("converting response from protobuf: %w", err)
	}

	return log, createdTx, nil
}

// Helper functions for conversion

func (r *RoutedLedger) createTransactionRequestToProto(ledgerName string, params Parameters[CreateTransaction]) (*api.CreateTransactionRequest, error) {
	input := params.Input

	// Convert postings
	postings := make([]*api.Posting, 0, len(input.Postings))
	for _, p := range input.Postings {
		postings = append(postings, &api.Posting{
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
	if !input.Timestamp.IsZero() {
		timestamp = timestamppb.New(input.Timestamp.Time)
	}

	return &api.CreateTransactionRequest{
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

func (r *RoutedLedger) createTransactionResponseFromProto(resp *api.CreateTransactionResponse) (*ledger.Log, *ledger.CreatedTransaction, error) {
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

	// Convert ID
	if resp.Transaction.Id != 0 {
		tx = tx.WithID(resp.Transaction.Id)
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

// Stub methods for other Ledger interface methods
func (r *RoutedLedger) RevertTransaction(ctx context.Context, ledgerName string, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	return nil, nil, ErrNotFound
}

func (r *RoutedLedger) SaveTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

func (r *RoutedLedger) SaveAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

func (r *RoutedLedger) DeleteTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

func (r *RoutedLedger) DeleteAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

func (r *RoutedLedger) Import(ctx context.Context, ledgerName string, stream chan ledger.Log) error {
	return ErrNotFound
}

func (r *RoutedLedger) Export(ctx context.Context, ledgerName string, w ExportWriter) error {
	return ErrNotFound
}
