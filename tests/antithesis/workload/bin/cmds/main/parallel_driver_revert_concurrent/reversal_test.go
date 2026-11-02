package main

import (
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func reversalFixture() (*servicepb.ApplyResponse, []*commonpb.Posting, *commonpb.Transaction) {
	// Keep zero, self-transfer, repeated accounts, multiple assets and the
	// full uint64 amount domain valid. The original transaction ID can be zero.
	original := []*commonpb.Posting{
		commonpb.NewPosting("users:1", "users:1", "USD/2", big.NewInt(0)),
		commonpb.NewPosting("world", "users:1", "COIN", new(big.Int).SetUint64(math.MaxUint64)),
	}
	tx := &commonpb.Transaction{Id: 5, RevertsTransaction: 0, Postings: []*commonpb.Posting{
		commonpb.NewPosting("users:1", "users:1", "USD/2", big.NewInt(0)),
		commonpb.NewPosting("users:1", "world", "COIN", new(big.Int).SetUint64(math.MaxUint64)),
	}}
	resp := &servicepb.ApplyResponse{Logs: []*commonpb.Log{{
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
			LedgerName: "default", Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{
				Payload: &commonpb.LedgerLogPayload_RevertedTransaction{RevertedTransaction: &commonpb.RevertedTransaction{
					RevertedTransactionId: 0, RevertTransaction: tx,
				}},
			}},
		}}},
	}}}

	return resp, original, tx
}

func TestValidateReversal(t *testing.T) {
	t.Parallel()
	resp, original, _ := reversalFixture()
	require.NoError(t, validateReversal(resp, "default", 0, original))

	for _, tc := range []struct {
		name   string
		mutate func(*servicepb.ApplyResponse, *commonpb.Transaction)
		want   string
	}{
		{"missing log", func(r *servicepb.ApplyResponse, _ *commonpb.Transaction) { r.Logs = nil }, "expected one revert log"},
		{"duplicate effect", func(r *servicepb.ApplyResponse, _ *commonpb.Transaction) { r.Logs = append(r.Logs, r.GetLogs()[0]) }, "expected one revert log"},
		{"missing payload", func(r *servicepb.ApplyResponse, _ *commonpb.Transaction) { r.Logs[0] = nil }, "missing apply log"},
		{"wrong ledger", func(r *servicepb.ApplyResponse, _ *commonpb.Transaction) {
			r.Logs[0].GetPayload().GetApply().LedgerName = "other"
		}, "missing apply log"},
		{"wrong target", func(r *servicepb.ApplyResponse, _ *commonpb.Transaction) {
			r.Logs[0].GetPayload().GetApply().GetLog().GetData().GetRevertedTransaction().RevertedTransactionId = 9
		}, "missing reversal"},
		{"missing transaction", func(r *servicepb.ApplyResponse, _ *commonpb.Transaction) {
			r.Logs[0].GetPayload().GetApply().GetLog().GetData().GetRevertedTransaction().RevertTransaction = nil
		}, "missing reversal"},
		{"reused identity", func(_ *servicepb.ApplyResponse, tx *commonpb.Transaction) { tx.Id = 0 }, "invalid reversal identity"},
		{"wrong reverse link", func(_ *servicepb.ApplyResponse, tx *commonpb.Transaction) { tx.RevertsTransaction = 8 }, "invalid reversal identity"},
		{"missing posting", func(_ *servicepb.ApplyResponse, tx *commonpb.Transaction) { tx.Postings = tx.GetPostings()[:1] }, "expected 2 reversed postings"},
		{"missing amount", func(_ *servicepb.ApplyResponse, tx *commonpb.Transaction) { tx.Postings[1].Amount = nil }, "posting 1 is incomplete"},
		{"wrong direction", func(_ *servicepb.ApplyResponse, tx *commonpb.Transaction) { tx.Postings[1].Source = "world" }, "posting 1 does not exactly reverse"},
		{"wrong amount", func(_ *servicepb.ApplyResponse, tx *commonpb.Transaction) {
			tx.Postings[1].Amount = commonpb.NewPosting("a", "b", "COIN", big.NewInt(1)).GetAmount()
		}, "posting 1 does not exactly reverse"},
		{"wrong asset", func(_ *servicepb.ApplyResponse, tx *commonpb.Transaction) { tx.Postings[1].Asset = "USD/2" }, "posting 1 does not exactly reverse"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp, original, tx := reversalFixture()
			tc.mutate(resp, tx)
			require.ErrorContains(t, validateReversal(resp, "default", 0, original), tc.want)
		})
	}
	t.Run("nil response", func(t *testing.T) {
		t.Parallel()
		require.ErrorContains(t, validateReversal(nil, "default", 0, original), "expected one revert log")
	})
}
