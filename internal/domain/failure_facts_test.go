package domain

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFailureFactsBoundUntrustedValues(t *testing.T) {
	err := &ErrNumscriptExecution{
		Detail: "diagnostic only",
		Code:   "NUMSCRIPT_INVALID_ACCOUNT_NAME",
		Facts:  map[string]string{"name": strings.Repeat("a", 5000)},
	}
	facts := FailureFactsOf(err)
	require.NoError(t, ValidateFailureFacts(facts))
	require.Regexp(t, `^sha256:[0-9a-f]{64}$`, facts.Facts["name"])
	require.NotContains(t, facts.Facts["name"], "diagnostic")
}
