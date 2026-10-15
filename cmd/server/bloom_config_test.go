package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/infra/bloom"
)

func TestLoadConfigBloomRates(t *testing.T) {
	t.Parallel()

	// Explicit inventory: a newly added CLI type must have the same startup
	// validation as the original volumes regression, including the final slot.
	filters := []string{
		"volumes", "metadata", "references", "ledgers", "boundaries", "transactions",
		"sink-configs", "numscript-versions", "numscript-contents", "ledger-metadata",
		"prepared-queries", "indexes",
	}
	for _, filter := range filters {
		t.Run(filter, func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				rate  string
				valid bool
			}{
				{rate: "-0.1"},
				{rate: "1.1"},
				{rate: "1"},
				{rate: "NaN"},
				{rate: "+Inf"},
				{rate: "-Inf"},
				{rate: "0", valid: true},
				{rate: "0.001", valid: true},
				{rate: "0.01", valid: true},
				{rate: "0.999", valid: true},
			} {
				t.Run(tc.rate, func(t *testing.T) {
					t.Parallel()
					for _, enabled := range []bool{true, false} {
						cmd := NewRunCommand()
						require.NoError(t, cmd.Flags().Set("node-id", "1"))
						require.NoError(t, cmd.Flags().Set("cluster-id", "bloom-validation"))
						if enabled {
							require.NoError(t, cmd.Flags().Set("bloom-"+filter+"-expected-keys", "1"))
						}
						flag := "bloom-" + filter + "-fp-rate"
						require.NoError(t, cmd.Flags().Set(flag, tc.rate))
						cfg, err := LoadConfig(context.Background(), cmd)
						require.NoError(t, err)
						err = cfg.Validate()
						if enabled && !tc.valid {
							require.ErrorContains(t, err, "--"+flag+" must be finite and in (0,1)")

							continue
						}
						require.NoError(t, err)
						require.NotPanics(t, func() {
							bloom.NewFilterSet(cfg.BloomConfig, noop.NewMeterProvider().Meter("test"))
						})
					}
				})
			}
		})
	}
}
