package replication

import (
	"testing"

	"github.com/stretchr/testify/require"

	ledger "github.com/formancehq/ledger/internal"
)

func TestMapPipelineConfigurationRoundTrip(t *testing.T) {
	t.Parallel()

	cfg := ledger.PipelineConfiguration{
		ExporterID: "exporter-1",
		Ledger:     "ledger-1",
		AddressRewriteRules: []ledger.AddressRewriteRule{
			{Pattern: `(:worker:\d+)`, Replacement: ""},
			{Pattern: `^payments:`, Replacement: "psp:"},
		},
	}

	require.Equal(t, cfg, mapPipelineConfigurationFromGRPC(mapPipelineConfiguration(cfg)))
}

func TestMapPipelineConfigurationRoundTripNoRules(t *testing.T) {
	t.Parallel()

	cfg := ledger.PipelineConfiguration{
		ExporterID: "exporter-1",
		Ledger:     "ledger-1",
	}

	// No rules must round-trip as nil (not an empty slice) so whole-struct
	// equality holds across storage and transport.
	got := mapPipelineConfigurationFromGRPC(mapPipelineConfiguration(cfg))
	require.Nil(t, got.AddressRewriteRules)
	require.Equal(t, cfg, got)
}
