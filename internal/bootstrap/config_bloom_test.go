package bootstrap

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
)

func TestConfigValidateBloom(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		bloom   *ledgerpb.ClusterConfig
		invalid bool
	}{
		{name: "nil configuration"},
		{name: "absent filters", bloom: &ledgerpb.ClusterConfig{}},
		{
			name:  "disabled filter ignores rate",
			bloom: &ledgerpb.ClusterConfig{BloomVolumes: &ledgerpb.BloomTypeConfig{FpRate: math.NaN()}},
		},
		{
			name:    "direct config must supply enabled rate",
			bloom:   &ledgerpb.ClusterConfig{BloomVolumes: &ledgerpb.BloomTypeConfig{ExpectedKeys: 1}},
			invalid: true,
		},
		{
			name:  "valid enabled rate",
			bloom: &ledgerpb.ClusterConfig{BloomVolumes: &ledgerpb.BloomTypeConfig{ExpectedKeys: 1, FpRate: 0.01}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := validBaseConfig()
			cfg.BloomConfig = tc.bloom
			err := cfg.Validate()
			if tc.invalid {
				require.ErrorContains(t, err, "--bloom-volumes-fp-rate must be finite and in (0,1)")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
