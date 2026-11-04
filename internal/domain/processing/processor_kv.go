package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func processPutKV(ledger string, order *raftcmdpb.PutKVOrder, ctx *Context) (*commonpb.LogPayload, domain.SerializableError) {
	if _, err := loadLiveLedgerReader(ctx.Scope, ledger); err != nil {
		return nil, err
	}
	ctx.Scope.PutKV(ledger, order.GetKey(), order.GetValue())

	return &commonpb.LogPayload{Type: &commonpb.LogPayload_PutKv{PutKv: &commonpb.PutKVLog{
		Ledger: ledger, Key: order.GetKey(), Value: order.GetValue(),
	}}}, nil
}

func processDeleteKV(ledger string, order *raftcmdpb.DeleteKVOrder, ctx *Context) (*commonpb.LogPayload, domain.SerializableError) {
	if _, err := loadLiveLedgerReader(ctx.Scope, ledger); err != nil {
		return nil, err
	}
	ctx.Scope.DeleteKV(ledger, order.GetKey())

	return &commonpb.LogPayload{Type: &commonpb.LogPayload_DeleteKv{DeleteKv: &commonpb.DeleteKVLog{
		Ledger: ledger, Key: order.GetKey(),
	}}}, nil
}
