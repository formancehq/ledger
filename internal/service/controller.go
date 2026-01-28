package service

import (
	"context"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"google.golang.org/protobuf/proto"
)

type Controller interface {
	CreateLedger(ctx context.Context, req *raftpb.CreateLedgerCommand) (*commonpb.LedgerInfo, error)
	DeleteLedger(ctx context.Context, id uint32) error
	GetAllLedgersInfo(ctx context.Context) (map[string]*commonpb.LedgerInfo, error)
	CreateTransaction(ctx context.Context, id uint32, parameters Parameters[*ledgerpb.CreateTransactionRequestPayload]) (*commonpb.Log, error)
	GetTransaction(ctx context.Context, id uint32, transactionID uint64) (*commonpb.Transaction, error)
	RevertTransaction(ctx context.Context, id uint32, parameters Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*commonpb.Log, error)
	SaveTransactionMetadata(ctx context.Context, id uint32, parameters Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]) (*commonpb.Log, error)
	SaveAccountMetadata(ctx context.Context, id uint32, parameters Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]) (*commonpb.Log, error)
	DeleteTransactionMetadata(ctx context.Context, id uint32, parameters Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*commonpb.Log, error)
	DeleteAccountMetadata(ctx context.Context, id uint32, parameters Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*commonpb.Log, error)
	Import(ctx context.Context, id uint32, stream chan *commonpb.Log) error
	Export(ctx context.Context, id uint32, w ExportWriter) error
	GetAllLogs(ctx context.Context, id uint32, from uint64, to uint64) (store.Cursor[*commonpb.Log], error)
	GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error)
}

type Parameters[INPUT proto.Message] struct {
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
	Input          INPUT  `json:"-"`
}

type ExportWriter interface {
	Write(ctx context.Context, log *commonpb.Log) error
}
