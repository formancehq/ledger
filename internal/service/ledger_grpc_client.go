package service

import (
	"context"
	"fmt"
	"io"
	"math/big"

	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// LedgerGrpcClient implements Ledger by forwarding requests via gRPC to the leader
type LedgerGrpcClient struct {
	client LedgerServiceClient
	name   string
}

// NewLedgerGrpcClient creates a new gRPC-based ledger implementation
func NewLedgerGrpcClient(name string, client LedgerServiceClient) *LedgerGrpcClient {
	return &LedgerGrpcClient{
		client: client,
		name:   name,
	}
}

func (g *LedgerGrpcClient) Snapshot(ctx context.Context) error {
	_, err := g.client.Snapshot(ctx, &LedgerSnapshotRequest{
		Ledger: g.name,
	})
	return err
}

// CreateTransaction forwards the request via gRPC to the leader
func (g *LedgerGrpcClient) CreateTransaction(ctx context.Context, ledgerName string, parameters Parameters[CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, error) {
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

func (g *LedgerGrpcClient) RevertTransaction(ctx context.Context, ledgerName string, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	return nil, nil, ErrNotFound
}

func (g *LedgerGrpcClient) SaveTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

func (g *LedgerGrpcClient) SaveAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

func (g *LedgerGrpcClient) DeleteTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

func (g *LedgerGrpcClient) DeleteAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error) {
	return nil, ErrNotFound
}

func (g *LedgerGrpcClient) Import(ctx context.Context, ledgerName string, stream chan ledger.Log) error {
	return ErrNotFound
}

func (g *LedgerGrpcClient) Export(ctx context.Context, ledgerName string, w ExportWriter) error {
	return ErrNotFound
}

// ledgerChannelLogCursor implements Cursor[ledger.Log] for gRPC stream
type ledgerChannelLogCursor struct {
	logChan <-chan ledger.Log
	closed  bool
}

func (c *ledgerChannelLogCursor) Next(ctx context.Context) (ledger.Log, error) {
	if c.closed {
		return ledger.Log{}, io.EOF
	}

	// Read next log from channel
	select {
	case log, ok := <-c.logChan:
		if !ok {
			c.closed = true
			return ledger.Log{}, io.EOF
		}
		return log, nil
	case <-ctx.Done():
		c.closed = true
		return ledger.Log{}, ctx.Err()
	}
}

func (c *ledgerChannelLogCursor) Close() error {
	c.closed = true
	return nil
}

// GetAllLogs returns a cursor to iterate over all logs (implements LogReader)
func (g *LedgerGrpcClient) GetAllLogs(ctx context.Context, from uint64, to uint64) (Cursor[ledger.Log], error) {
	req := &StreamLogsRequest{
		Ledger:       g.name,
		FromSequence: from,
		ToSequence:   to, // 0 means no limit
	}

	stream, err := g.client.StreamLogs(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	logChan := make(chan ledger.Log)

	go func() {
		defer close(logChan)

		for {
			resp, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				// Error receiving log - channel will be closed
				return
			}

			// Convert protobuf Log to ledger.Log
			log, err := logFromLedgerProto(resp.Log)
			if err != nil {
				// Conversion error - channel will be closed
				panic(err)
			}

			// Send log to channel
			select {
			case logChan <- log:
			case <-ctx.Done():
				return
			}
		}
	}()

	return &ledgerChannelLogCursor{
		logChan: logChan,
	}, nil
}

func (g *LedgerGrpcClient) createTransactionRequestToProto(ledgerName string, params Parameters[CreateTransaction]) (*CreateTransactionRequest, error) {
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
		if s, err := MetadataToStruct(md); err == nil {
			accountMetadata[addr] = s
		}
	}

	// Convert metadata
	var metadata *structpb.Struct
	if len(input.Metadata) > 0 {
		if md, err := MetadataToStruct(input.Metadata); err == nil {
			metadata = md
		}
	}

	// Convert timestamp
	var timestamp *timestamppb.Timestamp
	if input.Timestamp != nil && !input.Timestamp.IsZero() {
		timestamp = timestamppb.New(input.Timestamp.Time)
	}

	// Convert script if provided
	var scriptProto *Script
	if input.Script != nil {
		scriptProto = &Script{
			Plain: input.Script.Plain,
			Vars:  input.Script.Vars,
		}
	}

	return &CreateTransactionRequest{
		Ledger:         g.name,
		AccountMetadata: accountMetadata,
		Timestamp:       timestamp,
		Metadata:        metadata,
		Reference:       input.Reference,
		Postings:        postings,
		DryRun:          params.DryRun,
		IdempotencyKey:  params.IdempotencyKey,
		Script:          scriptProto,
		Runtime:         input.Runtime,
	}, nil
}

func (g *LedgerGrpcClient) createTransactionResponseFromProto(resp *CreateTransactionResponse) (*ledger.Log, *ledger.CreatedTransaction, error) {
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
		tx = tx.WithMetadata(StructToMetadata(resp.Transaction.Metadata))
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
			accountMetadata[addr] = StructToMetadata(md)
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

// logFromLedgerProto converts a ledger.proto Log to ledger.Log
func logFromLedgerProto(l *Log) (ledger.Log, error) {
	log := ledger.Log{
		Type:            ledger.LogType(l.Type),
		IdempotencyKey:  l.IdempotencyKey,
		IdempotencyHash: l.IdempotencyHash,
		Sequence:        l.Sequence,
	}

	if l.Id != 0 {
		id := l.Id
		log.ID = &id
	}

	if l.Date != nil {
		log.Date = time.New(l.Date.AsTime())
	}

	// Convert protobuf LogPayload to ledger.LogPayload
	logPayload, err := logPayloadFromLedgerProto(l.Data)
	if err != nil {
		return log, fmt.Errorf("converting log payload from proto: %w", err)
	}
	log.Data = logPayload

	return log, nil
}

// logPayloadFromLedgerProto converts a ledger.proto LogPayload to ledger.LogPayload
func logPayloadFromLedgerProto(payload *LogPayload) (ledger.LogPayload, error) {
	if payload == nil {
		return nil, fmt.Errorf("log payload is nil")
	}

	switch p := payload.Payload.(type) {
	case *LogPayload_CreatedTransaction:
		tx, err := transactionFromLedgerProto(p.CreatedTransaction)
		if err != nil {
			return nil, err
		}
		return &ledger.CreatedTransaction{
			Transaction: tx,
		}, nil
	case *LogPayload_RevertedTransaction:
		revertedTx, err := transactionFromLedgerProto(p.RevertedTransaction.RevertedTransaction)
		if err != nil {
			return nil, err
		}
		revertTx, err := transactionFromLedgerProto(p.RevertedTransaction.RevertTransaction)
		if err != nil {
			return nil, err
		}
		return &ledger.RevertedTransaction{
			RevertedTransaction: revertedTx,
			RevertTransaction:   revertTx,
		}, nil
	case *LogPayload_SavedMetadata:
		var targetID interface{}
		switch id := p.SavedMetadata.TargetId.(type) {
		case *SavedMetadata_AccountId:
			targetID = id.AccountId
		case *SavedMetadata_TransactionId:
			targetID = id.TransactionId
		default:
			return nil, fmt.Errorf("unknown target ID type")
		}
		return &ledger.SavedMetadata{
			TargetType: p.SavedMetadata.TargetType,
			TargetID:   targetID,
			Metadata:   StructToMetadata(p.SavedMetadata.Metadata),
		}, nil
	case *LogPayload_DeletedMetadata:
		var targetID interface{}
		switch id := p.DeletedMetadata.TargetId.(type) {
		case *DeletedMetadata_AccountId:
			targetID = id.AccountId
		case *DeletedMetadata_TransactionId:
			targetID = id.TransactionId
		default:
			return nil, fmt.Errorf("unknown target ID type")
		}
		return &ledger.DeletedMetadata{
			TargetType: p.DeletedMetadata.TargetType,
			TargetID:   targetID,
			Key:        p.DeletedMetadata.Key,
		}, nil
	default:
		return nil, fmt.Errorf("unknown log payload type: %T", payload.Payload)
	}
}

// transactionFromLedgerProto converts a protobuf Transaction to ledger.Transaction
func transactionFromLedgerProto(tx *Transaction) (ledger.Transaction, error) {
	postings := make(ledger.Postings, 0, len(tx.Postings))
	for _, p := range tx.Postings {
		amount, ok := new(big.Int).SetString(p.Amount, 10)
		if !ok {
			return ledger.Transaction{}, fmt.Errorf("invalid amount: %s", p.Amount)
		}
		postings = append(postings, ledger.NewPosting(p.Source, p.Destination, p.Asset, amount))
	}

	txResult := ledger.NewTransaction()
	txResult = txResult.WithPostings(postings...)

	if tx.Metadata != nil {
		txResult = txResult.WithMetadata(StructToMetadata(tx.Metadata))
	}

	if tx.Timestamp != nil {
		txResult = txResult.WithTimestamp(time.New(tx.Timestamp.AsTime()))
	}

	if tx.Reference != "" {
		txResult = txResult.WithReference(tx.Reference)
	}

	if tx.Id != 0 {
		txResult = txResult.WithID(tx.Id)
	}

	return txResult, nil
}

