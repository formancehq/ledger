package domain

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestTypedFailureDetailsCoverEveryStableCode(t *testing.T) {
	t.Parallel()
	codes := make(map[string]struct{})
	for reason := range commonpb.ErrorReason_value {
		code := strings.TrimPrefix(reason, "ERROR_REASON_")
		if code != "UNSPECIFIED" {
			codes[code] = struct{}{}
		}
	}
	for code := range validationFailureCodes {
		codes[code] = struct{}{}
	}
	for code := range numscriptFailureCodes {
		codes[code] = struct{}{}
	}
	for code := range numscriptCompileFailureCodes {
		codes[code] = struct{}{}
	}
	codes["METADATA_LIMITS_UNCONFIGURED"] = struct{}{}
	for code := range codes {
		details, err := commonpb.NewFailureDetails(code, nil)
		require.NoError(t, err, code)
		require.Equal(t, code, commonpb.FailureDetailsCode(details))
	}
}

func TestTypedFailureDetailsRoundTripAndRejectMismatches(t *testing.T) {
	t.Parallel()
	f := NewFailureFacts(ErrReasonTransactionReferenceConflict, "TRANSACTION_REFERENCE_CONFLICT", map[string]string{
		"ledger": "L", "reference": "ref", "existingTransactionId": "42",
	})
	details, err := FailureDetailsOf(f)
	require.NoError(t, err)
	actual, err := FailureFactsFromDetails(f.Reason, f.Code, details)
	require.NoError(t, err)
	require.Equal(t, f, actual)
	require.Equal(t, uint64(42), details.GetTransactionReferenceConflict().GetExistingTransactionId())

	_, err = FailureFactsFromDetails(f.Reason, "TRANSACTION_NOT_FOUND", details)
	require.ErrorContains(t, err, "does not match")
	_, err = FailureFactsFromDetails(f.Reason, f.Code, nil)
	require.ErrorContains(t, err, "missing")
	_, err = commonpb.NewFailureDetails(f.Code, map[string]string{"arbitrary": "text"})
	require.ErrorContains(t, err, "not part of")
}

func TestFailureFactsPreserveValues(t *testing.T) {
	err := &ErrNumscriptExecution{
		Detail: "diagnostic only",
		Code:   "NUMSCRIPT_INVALID_ACCOUNT_NAME",
		Facts:  map[string]string{"name": "account"},
	}
	facts := FailureFactsOf(err)
	require.NoError(t, ValidateFailureFacts(facts))
	require.Equal(t, "account", facts.Facts["name"])
	require.NotContains(t, facts.Facts["name"], "diagnostic")
}

func TestFailureFactsPreserveLongProducerValue(t *testing.T) {
	t.Parallel()
	facts := NewFailureFacts(ErrReasonNumscriptExecutionError, "NUMSCRIPT_INVALID_ACCOUNT_NAME",
		map[string]string{"name": strings.Repeat("a", 4097)})
	require.Equal(t, strings.Repeat("a", 4097), facts.Facts["name"])
	require.NoError(t, ValidateFailureFacts(facts))
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
