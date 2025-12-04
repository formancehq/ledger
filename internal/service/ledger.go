package service

import (
	"context"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

type Ledger interface {
	CreateTransaction(ctx context.Context, ledgerName string, parameters Parameters[CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, error)
	RevertTransaction(ctx context.Context, ledgerName string, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error)
	SaveTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error)
	SaveAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error)
	DeleteTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error)
	DeleteAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error)
	Import(ctx context.Context, ledgerName string, stream chan ledger.Log) error
	Export(ctx context.Context, ledgerName string, w ExportWriter) error
}

type Parameters[INPUT any] struct {
	DryRun         bool
	IdempotencyKey string
	Input          INPUT
}

type CreateTransaction struct {
	AccountMetadata map[string]metadata.Metadata
	Timestamp       time.Time         `json:"timestamp"`
	Metadata        metadata.Metadata `json:"metadata"`
	Reference       string            `json:"reference"`
	Postings        ledger.Postings
}

type RevertTransaction struct {
	Force           bool
	AtEffectiveDate bool
	TransactionID   uint64
	Metadata        metadata.Metadata
}

type SaveTransactionMetadata struct {
	TransactionID uint64
	Metadata      metadata.Metadata
}

type SaveAccountMetadata struct {
	Address  string
	Metadata metadata.Metadata
}

type DeleteTransactionMetadata struct {
	TransactionID uint64
	Key           string
}

type DeleteAccountMetadata struct {
	Address string
	Key     string
}

type ExportWriter interface {
	Write(ctx context.Context, log ledger.Log) error
}

type ExportWriterFn func(ctx context.Context, log ledger.Log) error

func (fn ExportWriterFn) Write(ctx context.Context, log ledger.Log) error {
	return fn(ctx, log)
}
