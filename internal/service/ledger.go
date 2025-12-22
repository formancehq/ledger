package service

import (
	"context"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

type Ledger interface {
	CreateTransaction(ctx context.Context, ledgerName string, parameters Parameters[*ledgerpb.CreateTransactionRequestPayload]) (*ledgerpb.Log, *ledgerpb.CreatedTransaction, error)
	RevertTransaction(ctx context.Context, ledgerName string, parameters Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*ledgerpb.Log, *ledgerpb.RevertedTransaction, error)
	SaveTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]) (*ledgerpb.Log, error)
	SaveAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]) (*ledgerpb.Log, error)
	DeleteTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*ledgerpb.Log, error)
	DeleteAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*ledgerpb.Log, error)
	Import(ctx context.Context, ledgerName string, stream chan *ledgerpb.Log) error
	Export(ctx context.Context, ledgerName string, w ExportWriter) error
	GetAllLogs(ctx context.Context, from uint64, to uint64) (Cursor[*ledgerpb.Log], error)
}

type Parameters[INPUT any] struct {
	DryRun         bool   `json:"dryRun,omitempty"`
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
	Input          INPUT  `json:"-"`
}

type ExportWriter interface {
	Write(ctx context.Context, log *ledgerpb.Log) error
}

type ExportWriterFn func(ctx context.Context, log *ledgerpb.Log) error

func (fn ExportWriterFn) Write(ctx context.Context, log *ledgerpb.Log) error {
	return fn(ctx, log)
}
