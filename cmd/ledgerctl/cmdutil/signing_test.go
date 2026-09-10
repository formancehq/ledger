package cmdutil

import (
	"crypto/ed25519"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestBuildApplyRequestWithIdempotencyKey(t *testing.T) {
	t.Parallel()
	for _, signed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unsigned", true: "signed"}[signed], func(t *testing.T) {
			t.Parallel()
			cmd := &cobra.Command{}
			cmd.Flags().String("signing-key", "", "")
			seed := make([]byte, ed25519.SeedSize)
			if signed {
				path := filepath.Join(t.TempDir(), "seed")
				require.NoError(t, os.WriteFile(path, seed, 0600))
				require.NoError(t, cmd.Flags().Set("signing-key", path))
			}
			req, err := BuildApplyRequestWithIdempotencyKey(cmd, "operator/uid/attempt", &servicepb.Request{Type: &servicepb.Request_CreateIndex{CreateIndex: &servicepb.CreateIndexRequest{Ledger: "main"}}})
			require.NoError(t, err)
			batch := req.GetUnsigned()
			if signed {
				require.NoError(t, signing.Verify(req.GetSigned(), ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)))
				batch, err = signing.ExtractBatch(req.GetSigned())
				require.NoError(t, err)
			}
			require.Equal(t, "operator/uid/attempt", batch.GetIdempotencyKey())
			require.Len(t, batch.GetRequests(), 1)
		})
	}
}
