package service

import (
	"context"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

type Ledger interface {
	CreateTransaction(ctx context.Context, parameters Parameters[*ledgerpb.CreateTransactionRequestPayload]) (*ledgerpb.Log, error)
	RevertTransaction(ctx context.Context, parameters Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*ledgerpb.Log, error)
	SaveTransactionMetadata(ctx context.Context, parameters Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]) (*ledgerpb.Log, error)
	SaveAccountMetadata(ctx context.Context, parameters Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]) (*ledgerpb.Log, error)
	DeleteTransactionMetadata(ctx context.Context, parameters Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*ledgerpb.Log, error)
	DeleteAccountMetadata(ctx context.Context, parameters Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*ledgerpb.Log, error)
	Import(ctx context.Context, stream chan *ledgerpb.Log) error
	Export(ctx context.Context, w ExportWriter) error
	GetAllLogs(ctx context.Context, from uint64, to uint64) (Cursor[*ledgerpb.Log], error)
}

type Parameters[INPUT any] struct {
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
