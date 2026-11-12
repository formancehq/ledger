package ledger

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaEnforcementModeValidate(t *testing.T) {
	t.Parallel()

	for _, mode := range []SchemaEnforcementMode{SchemaEnforcementAudit, SchemaEnforcementStrict} {
		require.NoError(t, mode.Validate(), "mode %q", mode)
	}

	for _, mode := range []SchemaEnforcementMode{"", "Strict", "STRICT", "Audit", "stric", "permissive", " strict"} {
		err := mode.Validate()
		require.Error(t, err, "mode %q", mode)
		require.Contains(t, err.Error(), `expected "audit" or "strict"`)
	}
}
