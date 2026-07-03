package ledger

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPipelineConfigurationValidate(t *testing.T) {
	t.Parallel()

	t.Run("no rules", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, PipelineConfiguration{}.Validate())
	})

	t.Run("valid rules", func(t *testing.T) {
		t.Parallel()
		cfg := PipelineConfiguration{
			AddressRewriteRules: []AddressRewriteRule{
				{Pattern: `(:worker:\d+)`, Replacement: ""},
			},
		}
		require.NoError(t, cfg.Validate())
	})

	t.Run("invalid regexp", func(t *testing.T) {
		t.Parallel()
		cfg := PipelineConfiguration{
			AddressRewriteRules: []AddressRewriteRule{
				{Pattern: `(unbalanced`, Replacement: ""},
			},
		}
		require.Error(t, cfg.Validate())
	})
}
