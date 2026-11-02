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
