package domain

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

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
		if code == "VALIDATION" {
			continue // A coarse validation reason must use a concrete subcode.
		}
		foundReason := false
		for _, reason := range commonpb.ErrorReason_value {
			if commonpb.FailureDetailsAllowsReason(details, commonpb.ErrorReason(reason)) {
				foundReason = true
			}
		}
		require.True(t, foundReason, "variant %s must declare a coarse reason", code)
	}
}

func TestFailureDetailsHaveNoConditionalFacts(t *testing.T) {
	t.Parallel()
	arms := (&commonpb.FailureDetails{}).ProtoReflect().Descriptor().Oneofs().ByName("kind").Fields()
	for i := range arms.Len() {
		arm := arms.Get(i)
		fields := arm.Message().Fields()
		for j := range fields.Len() {
			field := fields.Get(j)
			options := field.Options().(*descriptorpb.FieldOptions)
			require.True(t, proto.HasExtension(options, commonpb.E_FailureFactRequired), "%s.%s must declare required presence", arm.Name(), field.Name())
			require.True(t, proto.GetExtension(options, commonpb.E_FailureFactRequired).(bool), "%s.%s cannot be conditional", arm.Name(), field.Name())
		}
	}
}

func TestTypedFailureDetailsRoundTripAndRejectMismatches(t *testing.T) {
	t.Parallel()
	reason, code, details := TypedFailureOf(&ErrTransactionReferenceConflict{
		Ledger: "L", Reference: "ref", ExistingTransactionID: 42,
	})
	f := FailureFactsFromTyped(ErrReasonTransactionReferenceConflict, details)
	actual, err := FailureFactsFromDetails(reason, code, details)
	require.NoError(t, err)
	require.Equal(t, f, actual)
	require.Equal(t, uint64(42), details.GetTransactionReferenceConflict().GetExistingTransactionId())

	_, err = FailureFactsFromDetails(f.Reason, "TRANSACTION_NOT_FOUND", details)
	require.ErrorContains(t, err, "does not match")
	_, err = FailureFactsFromDetails(commonpb.ErrorReason_ERROR_REASON_LEDGER_NOT_FOUND, f.Code, details)
	require.ErrorContains(t, err, "not allowed")
	_, err = FailureFactsFromDetails(f.Reason, f.Code, nil)
	require.ErrorContains(t, err, "missing")
	_, err = commonpb.NewFailureDetails(f.Code, map[string]string{"arbitrary": "text"})
	require.ErrorContains(t, err, "not part of")
	_, err = commonpb.NewFailureDetails(f.Code, map[string]string{"existingTransactionId": "042"})
	require.ErrorContains(t, err, "canonical")
}

func TestFailureFactPresenceFollowsSchema(t *testing.T) {
	t.Parallel()

	missingAmount := commonpb.MustFailureDetails("NUMSCRIPT_NEGATIVE_AMOUNT", nil)
	_, err := FailureFactsFromDetails(
		commonpb.ErrorReason_ERROR_REASON_NUMSCRIPT_EXECUTION_ERROR,
		"NUMSCRIPT_NEGATIVE_AMOUNT", missingAmount,
	)
	require.ErrorContains(t, err, "missing required fact amount")
	require.Panics(t, func() {
		TypedFailureOf(&ErrNumscriptExecution{Details: missingAmount})
	}, "an incomplete producer must fail before its outcome is audited")

	withZero := commonpb.MustFailureDetails("NUMSCRIPT_POSTING_AMOUNT_OVERFLOW", map[string]string{
		"postingIndex": "0", "amount": "1",
	})
	require.NoError(t, commonpb.ValidateRequiredFailureFacts(withZero))

	withoutConditional := commonpb.MustFailureDetails("NUMSCRIPT_DIVIDE_BY_ZERO", nil)
	require.NoError(t, commonpb.ValidateRequiredFailureFacts(withoutConditional))
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
				if cause.name == "limits unconfigured" && (tc.name == "key" || tc.name == "account and key") {
					continue // An unconfigured policy is detected before entry validation selects a key.
				}
				t.Run(tc.name, func(t *testing.T) {
					facts := FailureFactsOf(tc.err)
					require.NoError(t, ValidateFailureFacts(facts))
					if cause.name == "limits unconfigured" {
						require.Equal(t, map[string]string{
							"direct": "METADATA_LIMITS_UNCONFIGURED", "account": "ACCOUNT_METADATA_LIMITS_UNCONFIGURED",
						}[tc.name], facts.Code)
					} else {
						require.Equal(t, map[string]string{
							"direct": "METADATA_LIMIT_EXCEEDED", "account": "ACCOUNT_METADATA_LIMIT_EXCEEDED",
							"key": "METADATA_KEY_LIMIT_EXCEEDED", "account and key": "ACCOUNT_METADATA_KEY_LIMIT_EXCEEDED",
						}[tc.name], facts.Code)
					}
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
