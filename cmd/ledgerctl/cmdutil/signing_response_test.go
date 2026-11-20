package cmdutil

import (
	"bytes"
	"crypto/ed25519"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestVerifyResponseSignaturesBindsVisibleLog(t *testing.T) {
	t.Parallel()

	signer := signing.NewResponseSigner(make([]byte, ed25519.SeedSize))
	keyPath := filepath.Join(t.TempDir(), "response.pub")
	require.NoError(t, os.WriteFile(keyPath, signer.PublicKey(), 0o600))
	cmd := &cobra.Command{}
	cmd.Flags().String("response-verify-key", keyPath, "")

	newLog := func(id, amount, sequence uint64) *commonpb.Log {
		return &commonpb.Log{Sequence: sequence, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{LedgerName: "ledger", Log: &commonpb.LedgerLog{
				Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
					CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{
						Id: id, Postings: []*commonpb.Posting{{Source: "world", Destination: "bank", Asset: "USD", Amount: &commonpb.Uint256{V0: amount}}},
					}},
				}},
			}},
		}}}
	}

	original := newLog(11, 100, 7)
	original.ResponseSignature = signer.SignLog(original)
	require.NoError(t, VerifyResponseSignatures(cmd, []*commonpb.Log{original}))

	for _, tc := range []struct {
		name   string
		mutate func(*commonpb.Log)
	}{
		{"transaction ID", func(log *commonpb.Log) {
			log.GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().Id = 12
		}},
		{"posting amount", func(log *commonpb.Log) {
			log.GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().Postings[0].Amount.V0 = 101
		}},
		{"sequence", func(log *commonpb.Log) { log.Sequence = 8 }},
		{"different visible log", func(log *commonpb.Log) { log.Payload = newLog(99, 999, 99).GetPayload() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			altered := original.CloneVT()
			tc.mutate(altered)
			require.ErrorContains(t, VerifyResponseSignatures(cmd, []*commonpb.Log{altered}), "does not match signed payload")
		})
	}

	t.Run("corrupt signed payload", func(t *testing.T) {
		altered := original.CloneVT()
		altered.ResponseSignature.Payload[0] ^= 1
		require.ErrorContains(t, VerifyResponseSignatures(cmd, []*commonpb.Log{altered}), "response signature verification failed")
	})
	t.Run("corrupt signature", func(t *testing.T) {
		altered := original.CloneVT()
		altered.ResponseSignature.Signature[0] ^= 1
		require.ErrorContains(t, VerifyResponseSignatures(cmd, []*commonpb.Log{altered}), "response signature verification failed")
	})
	t.Run("wrong key", func(t *testing.T) {
		otherPath := filepath.Join(t.TempDir(), "other.pub")
		other := signing.NewResponseSigner(bytes.Repeat([]byte{1}, ed25519.SeedSize))
		require.NoError(t, os.WriteFile(otherPath, other.PublicKey(), 0o600))
		otherCmd := &cobra.Command{}
		otherCmd.Flags().String("response-verify-key", otherPath, "")
		require.ErrorContains(t, VerifyResponseSignatures(otherCmd, []*commonpb.Log{original}), "response signature verification failed")
	})
}

func TestVerifyApplyResponseBindsOrderedBatch(t *testing.T) {
	t.Parallel()
	signer := signing.NewResponseSigner(make([]byte, ed25519.SeedSize))
	keyPath := filepath.Join(t.TempDir(), "response.pub")
	require.NoError(t, os.WriteFile(keyPath, signer.PublicKey(), 0o600))
	cmd := &cobra.Command{}
	cmd.Flags().String("response-verify-key", keyPath, "")
	request := servicepb.UnsignedApplyRequest("", &servicepb.Request{Type: &servicepb.Request_DeletePreparedQuery{DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{Ledger: "one", Name: "first"}}}, &servicepb.Request{Type: &servicepb.Request_DeletePreparedQuery{DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{Ledger: "two", Name: "second"}}})
	payload, err := request.GetUnsigned().MarshalVT()
	require.NoError(t, err)
	request.ResponseBindingPayload = payload
	newLog := func(sequence uint64, ledger, name string) *commonpb.Log {
		log := &commonpb.Log{Sequence: sequence, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_DeletedPreparedQuery{DeletedPreparedQuery: &commonpb.DeletedPreparedQueryLog{Ledger: ledger, Name: name}}}}
		log.ResponseSignature = signer.SignLog(log)
		return log
	}
	first, second := newLog(7, "one", "first"), newLog(8, "two", "second")
	receipt, err := signer.SignResponseReceipt(request, []*commonpb.Log{first, second})
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		logs []*commonpb.Log
		want string
	}{
		{name: "matching", logs: []*commonpb.Log{first, second}},
		{name: "missing", logs: []*commonpb.Log{first}, want: "expected 2 response logs"},
		{name: "extra", logs: []*commonpb.Log{first, second, newLog(9, "three", "third")}, want: "expected 2 response logs"},
		{name: "reordered", logs: []*commonpb.Log{newLog(8, "one", "first"), newLog(7, "two", "second")}, want: "out of order"},
		{name: "unrelated signed order", logs: []*commonpb.Log{first, newLog(8, "other", "second")}, want: "does not match submitted order"},
		{name: "different signed log with same fields", logs: []*commonpb.Log{newLog(9, "one", "first"), newLog(10, "two", "second")}, want: "response receipt does not match log"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := VerifyApplyResponse(cmd, request, tc.logs, receipt)
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
	requestSigningKey := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	signed, err := signing.Sign(request.GetUnsigned(), "client", requestSigningKey)
	require.NoError(t, err)
	signedRequest := servicepb.SignedApplyRequest(signed)
	signedReceipt, err := signer.SignResponseReceipt(signedRequest, []*commonpb.Log{first, second})
	require.NoError(t, err)
	require.NoError(t, VerifyApplyResponse(cmd, signedRequest, []*commonpb.Log{first, second}, signedReceipt))
}

func TestVerifyApplyResponseBindsScriptWithoutVisibleIdentity(t *testing.T) {
	t.Parallel()
	signer := signing.NewResponseSigner(make([]byte, ed25519.SeedSize))
	keyPath := filepath.Join(t.TempDir(), "response.pub")
	require.NoError(t, os.WriteFile(keyPath, signer.PublicKey(), 0o600))
	cmd := &cobra.Command{}
	cmd.Flags().String("response-verify-key", keyPath, "")
	newRequest := func(script string) *servicepb.ApplyRequest {
		request := servicepb.UnsignedApplyRequest("", &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
			Ledger: "ledger", Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: &servicepb.CreateTransactionPayload{Script: &commonpb.Script{Plain: script}}}},
		}}})
		var err error
		request.ResponseBindingPayload, err = request.GetUnsigned().MarshalVT()
		require.NoError(t, err)
		return request
	}
	log := &commonpb.Log{Sequence: 7, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{LedgerName: "ledger", Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: 42}}}}}}}}}
	log.ResponseSignature = signer.SignLog(log)
	other := newRequest("send [USD 1] (source = @world destination = @bank)")
	receipt, err := signer.SignResponseReceipt(other, []*commonpb.Log{log})
	require.NoError(t, err)
	require.NoError(t, VerifyApplyResponse(cmd, other, []*commonpb.Log{log}, receipt))
	requested := newRequest("send [USD 2] (source = @world destination = @bank)")
	require.ErrorContains(t, VerifyApplyResponse(cmd, requested, []*commonpb.Log{log}, receipt), "response receipt does not match submitted batch")
	require.ErrorContains(t, VerifyApplyResponse(cmd, other, []*commonpb.Log{log}, nil), "missing signed response receipt")
	corrupt := receipt.CloneVT()
	corrupt.Signature[0] ^= 1
	require.ErrorContains(t, VerifyApplyResponse(cmd, other, []*commonpb.Log{log}, corrupt), "response receipt signature verification failed")
}

func TestBuildApplyRequestAddsBindingOnlyWhenVerifying(t *testing.T) {
	t.Parallel()
	cmd := &cobra.Command{}
	cmd.Flags().String("response-verify-key", "", "")
	order := &servicepb.Request{Type: &servicepb.Request_DeletePreparedQuery{DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{Ledger: "test", Name: "old"}}}
	request, err := BuildApplyRequest(cmd, order)
	require.NoError(t, err)
	require.Empty(t, request.GetResponseBindingPayload())
	require.NoError(t, cmd.Flags().Set("response-verify-key", "public-key-path"))
	request, err = BuildApplyRequest(cmd, order)
	require.NoError(t, err)
	require.NotEmpty(t, request.GetResponseBindingPayload())
	var decoded servicepb.ApplyBatch
	require.NoError(t, decoded.UnmarshalVT(request.GetResponseBindingPayload()))
	require.True(t, decoded.EqualVT(request.GetUnsigned()))
}
