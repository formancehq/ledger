package inputvalidation

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMustMatch(t *testing.T) {
	t.Parallel()
	fieldErr := &FieldError{Path: "reference", Rule: "max_bytes"}
	require.NotPanics(t, func() { MustMatch(fieldErr, "reference", "max_bytes") })
	for _, tc := range []struct {
		name string
		err  error
		path string
		rule string
	}{
		{"different field", fieldErr, "name", "max_bytes"},
		{"different rule", fieldErr, "reference", "exact_bytes"},
		{"different error", errors.New("invalid"), "reference", "max_bytes"},
		{"missing error", nil, "reference", "max_bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Panics(t, func() { MustMatch(tc.err, tc.path, tc.rule) })
		})
	}
}
