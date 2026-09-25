package cmdutil

import (
	"crypto/ed25519"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestBuildApplyRequestBindsIdempotencyKey(t *testing.T) {
	t.Parallel()
	for _, signed := range []bool{false, true} {
		name := "unsigned"
		if signed {
			name = "signed"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cmd := &cobra.Command{}
			cmd.Flags().String("idempotency-key", "creation-uid", "")
			cmd.Flags().String("signing-key", "", "")
			privateKey := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
			if signed {
				path := filepath.Join(t.TempDir(), "seed")
				require.NoError(t, os.WriteFile(path, privateKey.Seed(), 0600))
				require.NoError(t, cmd.Flags().Set("signing-key", path))
			}
			request, err := BuildApplyRequest(cmd, &servicepb.Request{Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{Name: "L"},
			}})
			require.NoError(t, err)
			batch := request.GetUnsigned()
			if signed {
				require.NoError(t, signing.Verify(request.GetSigned(), privateKey.Public().(ed25519.PublicKey)))
				batch = new(servicepb.ApplyBatch)
				require.NoError(t, proto.Unmarshal(request.GetSigned().GetPayload(), batch))
			}
			require.Equal(t, "creation-uid", batch.GetIdempotencyKey())
			require.Len(t, batch.GetRequests(), 1)
		})
	}
}
