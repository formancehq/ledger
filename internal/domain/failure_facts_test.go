package domain

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestTypedFailureDetailsCoverEveryStableCode(t *testing.T) {
	t.Parallel()
	fields := (&commonpb.FailureDetails{}).ProtoReflect().Descriptor().Oneofs().ByName("kind").Fields()
	for i := range fields.Len() {
		code := strings.ToUpper(string(fields.Get(i).Name()))
		details, err := commonpb.NewFailureDetails(code, nil)
		require.NoError(t, err, code)
		require.Equal(t, code, commonpb.FailureDetailsCode(details))
		require.NotEqual(t, commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED, commonpb.FailureDetailsReason(details), code)
	}
}

func TestFailureDetailsTrackEveryScalarFactPresence(t *testing.T) {
	t.Parallel()
	arms := (&commonpb.FailureDetails{}).ProtoReflect().Descriptor().Oneofs().ByName("kind").Fields()
	for i := range arms.Len() {
		arm := arms.Get(i)
		fields := arm.Message().Fields()
		for j := range fields.Len() {
			field := fields.Get(j)
			require.True(t, field.HasPresence(), "%s.%s must track presence", arm.Name(), field.Name())
		}
	}
}

func TestTypedFailureDetailsRoundTrip(t *testing.T) {
	t.Parallel()
	details := TypedFailureOf(&ErrTransactionReferenceConflict{
		Ledger: "L", Reference: "ref", ExistingTransactionID: 42,
	})
	f := FailureFactsFromTyped(details)
	actual, err := FailureFactsFromDetails(details)
	require.NoError(t, err)
	require.Equal(t, f, actual)
	require.Equal(t, commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT, f.Reason)
	require.Equal(t, uint64(42), details.GetTransactionReferenceConflict().GetExistingTransactionId())

	_, err = FailureFactsFromDetails(nil)
	require.ErrorContains(t, err, "missing")
	_, err = commonpb.NewFailureDetails(f.Code, map[string]string{"arbitrary": "text"})
	require.ErrorContains(t, err, "not part of")
	_, err = commonpb.NewFailureDetails(f.Code, map[string]string{"existingTransactionId": "042"})
	require.ErrorContains(t, err, "canonical")
}

func TestFailureFactPresenceFollowsSchema(t *testing.T) {
	t.Parallel()

	missingAmount := commonpb.MustFailureDetails("NUMSCRIPT_NEGATIVE_AMOUNT", nil)
	_, err := FailureFactsFromDetails(missingAmount)
	require.ErrorContains(t, err, "missing required fact amount")
	require.Panics(t, func() {
		TypedFailureOf(&ErrNumscriptExecution{Details: missingAmount})
	}, "an incomplete producer must fail before its outcome is audited")

	withZero := commonpb.MustFailureDetails("NUMSCRIPT_POSTING_AMOUNT_OVERFLOW", map[string]string{
		"postingIndex": "0", "amount": "1",
	})
	require.NoError(t, commonpb.ValidateRequiredFailureFacts(withZero))

	withoutFacts := commonpb.MustFailureDetails("NUMSCRIPT_DIVIDE_BY_ZERO", nil)
	require.NoError(t, commonpb.ValidateRequiredFailureFacts(withoutFacts))
}

func TestFailureFactsPreserveValues(t *testing.T) {
	err := &ErrNumscriptExecution{
		Detail:  "diagnostic only",
		Details: commonpb.MustFailureDetails("NUMSCRIPT_INVALID_ACCOUNT_NAME", map[string]string{"name": "account"}),
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
					require.Equal(t, map[string]string{
						"direct": "METADATA_LIMIT_EXCEEDED", "account": "ACCOUNT_METADATA_LIMIT_EXCEEDED",
						"key": "METADATA_KEY_LIMIT_EXCEEDED", "account and key": "ACCOUNT_METADATA_KEY_LIMIT_EXCEEDED",
					}[tc.name], facts.Code)
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
			if tc.name == "parse" {
				require.NoError(t, ValidateFailureFacts(facts))
			} else {
				require.Empty(t, facts.Code, "technical failures have no audited code")
			}
			require.Empty(t, facts.Facts)
			message, metadata, _ := PublicErrorDetails(tc.err)
			require.NotContains(t, message, "private")
			require.NotContains(t, message, "secret")
			require.Empty(t, metadata)
		})
	}
}

// A validation sentinel without a VALIDATION FailureDetails arm cannot be
// rendered by the API or projected into the audit chain. Construction must fail
// at package init rather than panic on the first rejected request (EN-2945).
func TestNewValidationSentinelRequiresValidationArm(t *testing.T) {
	t.Parallel()
	require.PanicsWithError(t, `invalid failure details for NOT_A_FAILURE_CODE: unknown failure detail code "NOT_A_FAILURE_CODE"`, func() {
		_ = NewValidationSentinel("NOT_A_FAILURE_CODE", "unused")
	})
	require.PanicsWithError(t, "validation sentinel TRANSACTION_NOT_FOUND maps to failure reason ERROR_REASON_TRANSACTION_NOT_FOUND", func() {
		_ = NewValidationSentinel("TRANSACTION_NOT_FOUND", "unused")
	})

	facts := FailureFactsOf(ErrPostingAmountRequired)
	require.Equal(t, commonpb.ErrorReason_ERROR_REASON_VALIDATION, facts.Reason)
	require.Equal(t, "POSTING_AMOUNT_REQUIRED", facts.Code)
	require.Empty(t, facts.Facts)
}
