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

func TestFailureFactsMetadataValidationWrappers(t *testing.T) {
	causes := []struct {
		name string
		err  SerializableError
	}{
		{"limit exceeded", &ErrMetadataLimitExceeded{Dimension: MetadataLimitDimensionEntries, Limit: 1, Actual: 2}},
		{"limits unconfigured", ErrMetadataLimitsUnconfigured},
	}

	for _, cause := range causes {
		t.Run(cause.name, func(t *testing.T) {
			cases := []struct {
				name string
				err  SerializableError
			}{
				{"direct", cause.err},
				{"account", &ErrAccountValidation{Account: "a", Cause: cause.err}},
				{"key", &ErrMetadataKeyValidation{Key: "k", Cause: cause.err}},
				{"account and key", &ErrAccountValidation{Account: "a", Cause: &ErrMetadataKeyValidation{Key: "k", Cause: cause.err}}},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					facts := FailureFactsOf(tc.err)
					require.NoError(t, ValidateFailureFacts(facts))
					if tc.name == "account" || tc.name == "account and key" {
						require.Equal(t, "a", facts.Facts["account"])
					}
					if tc.name == "key" || tc.name == "account and key" {
						require.Equal(t, "k", facts.Facts["key"])
					}
				})
			}
		})
	}
}

func TestFailureFactsExcludeDiagnosticText(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  SerializableError
	}{
		{"parse", &ErrNumscriptParse{Details: "unexpected token at /private/source"}},
		{"filter", &ErrFilterCompilation{Detail: "parameter secret: parse failed"}},
		{"storage", &ErrStorageOperation{Operation: "reading /private/ledger"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.NotEmpty(t, tc.err.Error())
			facts := FailureFactsOf(tc.err)
			require.NoError(t, ValidateFailureFacts(facts))
			require.Empty(t, facts.Facts)
			message, metadata, _ := PublicErrorDetails(tc.err)
			require.NotContains(t, message, "private")
			require.NotContains(t, message, "secret")
			require.Empty(t, metadata)
		})
	}
}
