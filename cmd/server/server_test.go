package server

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestLoadBloomConfigIncludesLedgerMetadata(t *testing.T) {
	t.Parallel()

	cmd := &cobra.Command{}
	registerBloomFlags(cmd)

	require.NoError(t, cmd.Flags().Set("bloom-ledger-metadata-expected-keys", "42"))

	cfg := &commonpb.ClusterConfig{}
	loadBloomConfig(cmd, cfg)

	require.Equal(t, uint64(42), cfg.GetBloomLedgerMetadata().GetExpectedKeys())
	require.Equal(t, 0.01, cfg.GetBloomLedgerMetadata().GetFpRate())
}
