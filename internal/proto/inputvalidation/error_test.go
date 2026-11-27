package inputvalidation

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolve(t *testing.T) {
	t.Parallel()
	fieldErr := &FieldError{Message: "raft.CreateTransactionOrder", Path: "reference", Rule: "max_bytes"}
	mappings := map[Key]string{{Message: "raft.CreateTransactionOrder", Path: "reference", Rule: "max_bytes"}: "TRANSACTION_REFERENCE_TOO_LONG"}
	require.Equal(t, "TRANSACTION_REFERENCE_TOO_LONG", Resolve(fieldErr, mappings))
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"different message", &FieldError{Message: "raft.ExecuteNumscriptOrder", Path: "reference", Rule: "max_bytes"}},
		{"different field", &FieldError{Message: fieldErr.Message, Path: "name", Rule: "max_bytes"}},
		{"different rule", &FieldError{Message: fieldErr.Message, Path: "reference", Rule: "exact_bytes"}},
		{"different error", errors.New("invalid")},
		{"missing error", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Panics(t, func() { Resolve(tc.err, mappings) })
		})
	}
}
