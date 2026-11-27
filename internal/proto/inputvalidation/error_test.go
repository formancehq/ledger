package inputvalidation

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFieldErrorUsesGeneratedPublicMessage(t *testing.T) {
	t.Parallel()
	err := &FieldError{Message: "common.Posting", Path: "amount", Rule: "required", PublicMessage: "posting amount is required"}
	require.EqualError(t, err, "posting amount is required")
}
