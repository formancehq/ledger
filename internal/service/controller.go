package service

import (
	"context"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"google.golang.org/protobuf/proto"
)

type Controller interface {
	CreateLedger(ctx context.Context, req *ledgerpb.CreateLedgerCommand) (*ledgerpb.LedgerInfo, error)
	DeleteLedger(ctx context.Context, id uint32) error
	GetAllLedgersInfo(ctx context.Context) (map[string]*ledgerpb.LedgerInfo, error)
	CreateTransaction(ctx context.Context, id uint32, parameters Parameters[*ledgerpb.CreateTransactionRequestPayload]) (*ledgerpb.Log, error)
	GetTransaction(ctx context.Context, id uint32, transactionID uint64) (*ledgerpb.Transaction, error)
	RevertTransaction(ctx context.Context, id uint32, parameters Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*ledgerpb.Log, error)
	SaveTransactionMetadata(ctx context.Context, id uint32, parameters Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]) (*ledgerpb.Log, error)
	SaveAccountMetadata(ctx context.Context, id uint32, parameters Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]) (*ledgerpb.Log, error)
	DeleteTransactionMetadata(ctx context.Context, id uint32, parameters Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*ledgerpb.Log, error)
	DeleteAccountMetadata(ctx context.Context, id uint32, parameters Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*ledgerpb.Log, error)
	Import(ctx context.Context, id uint32, stream chan *ledgerpb.Log) error
	Export(ctx context.Context, id uint32, w ExportWriter) error
	GetAllLogs(ctx context.Context, id uint32, from uint64, to uint64) (store.Cursor[*ledgerpb.Log], error)
	GetLedgerByName(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error)
}

type Parameters[INPUT proto.Message] struct {
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
	Input          INPUT  `json:"-"`
}

type ExportWriter interface {
	Write(ctx context.Context, log *ledgerpb.Log) error
}
