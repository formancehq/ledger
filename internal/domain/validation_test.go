package domain

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/invariants"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestValidateWrapping pins the sentinel contract: these Validate* functions
// call invariants.Validate* and map the primitive sentinel
// back to the matching local Describable sentinel by pointer identity. The
// exhaustive case coverage lives in github.com/formancehq/invariants; here
// we only verify that the bridge is wired correctly.
func TestValidateWrapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  Describable
		want Describable
	}{
		{name: "ledger name empty", got: ValidateLedgerName(""), want: ErrLedgerNameRequired},
		{name: "ledger name null byte", got: ValidateLedgerName("a\x00b"), want: ErrLedgerNameInvalidChar},
		{name: "ledger name space", got: ValidateLedgerName("a b"), want: ErrLedgerNameInvalidChar},
		{name: "ledger name newline", got: ValidateLedgerName("a\nb"), want: ErrLedgerNameInvalidChar},
		{name: "numscript name empty", got: ValidateNumscriptName(""), want: ErrNumscriptNameRequired},
		{name: "numscript name newline", got: ValidateNumscriptName("a\nb"), want: ErrNumscriptNameInvalidChar},
		{name: "signing key id empty", got: ValidateSigningKeyID(""), want: ErrSigningKeyIDRequired},
		{name: "signing key id newline", got: ValidateSigningKeyID("a\nb"), want: ErrSigningKeyIDInvalidChar},
		{name: "account address empty", got: ValidateAccountAddress(""), want: ErrAccountAddressEmpty},
		{name: "account address invalid char", got: ValidateAccountAddress("a b"), want: ErrAccountAddressInvalidChar},
		{name: "asset invalid", got: ValidateAsset("lowercase"), want: ErrAssetInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Same(t, tt.want, tt.got, "wrapper must return the pre-instantiated sentinel")
			require.Equal(t, KindValidation, tt.got.Kind())
			require.Equal(t, ErrReasonValidation, tt.got.Reason())
			require.Empty(t, FailureFactsOf(tt.got.(SerializableError)).Facts)
		})
	}
}

// TestValidateWrapping_NilOnValidInput pins the soft path: a valid input must
// return a nil Describable, not a non-nil sentinel that happens to have an
// empty message.
func TestValidateWrapping_NilOnValidInput(t *testing.T) {
	t.Parallel()

	require.Nil(t, ValidateLedgerName("default"))
	require.Nil(t, ValidateNumscriptName("transfer"))
	require.Nil(t, ValidateSigningKeyID("admin-key-1"))
	require.Nil(t, ValidateAccountAddress("users:alice"))
	require.Nil(t, ValidateMetadataKey("category"))
	require.Nil(t, ValidateMetadataString("category", "admin"))
	require.Nil(t, ValidateAsset("EUR/2"))
}

func TestMetadataValidationRetainsKey(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, key, code string
		err             SerializableError
		cause           error
	}{
		{"invalid key", "bad key", "METADATA_KEY_INVALID_CHAR", ValidateMetadataKey("bad key"), ErrMetadataKeyInvalidChar},
		{"invalid value", "invoice", "METADATA_VALUE_CONTAINS_NULL_BYTE", ValidateMetadataString("invoice", "bad\x00value"), ErrMetadataValueContainsNullByte},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, tc.err, tc.cause)
			details := TypedFailureOf(tc.err)
			require.Equal(t, tc.code, commonpb.FailureDetailsCode(details))
			require.Equal(t, tc.key, commonpb.FailureDetailsFacts(details)["key"])
		})
	}

	details := TypedFailureOf(ValidateMetadataKey(""))
	require.Equal(t, "METADATA_KEY_EMPTY", commonpb.FailureDetailsCode(details))
	require.Empty(t, commonpb.FailureDetailsFacts(details))
}

// TestValidateMetadataValue exercises the *commonpb.MetadataValue dispatch,
// which stays local to this package because it depends on the internal proto.
func TestValidateMetadataValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   *commonpb.MetadataValue
		wantErr error
	}{
		{name: "nil value"},
		{name: "valid string", input: commonpb.NewStringValue("admin")},
		{name: "empty string", input: commonpb.NewStringValue("")},
		{name: "string contains null byte", input: commonpb.NewStringValue("admin\x00evil"), wantErr: ErrMetadataValueContainsNullByte},
		{name: "valid null original", input: commonpb.NewNullValue("not-a-number")},
		{name: "null original contains null byte", input: commonpb.NewNullValue("not\x00safe"), wantErr: ErrMetadataValueContainsNullByte},
		{name: "nil null original", input: &commonpb.MetadataValue{Type: &commonpb.MetadataValue_NullValue{}}},
		{name: "int64", input: commonpb.NewIntValue(-42)},
		{name: "uint64", input: commonpb.NewUintValue(42)},
		{name: "bool", input: commonpb.NewBoolValue(true)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := ValidateMetadataValue("category", tt.input)
			if tt.wantErr != nil {
				require.ErrorIs(t, got, tt.wantErr)
				require.Equal(t, "category", FailureFactsOf(got).Facts["key"])
			} else {
				require.Nil(t, got)
			}
		})
	}
}

// TestValidateWrapping_UnwrapsToPrimitive ensures the local Describable
// sentinel still satisfies errors.Is against the invariants primitive —
// important for any caller that wants to react on the cross-service contract
// rather than on the in-process identity.
func TestValidateWrapping_UnwrapsToPrimitive(t *testing.T) {
	t.Parallel()

	require.ErrorIs(t, ValidateLedgerName(""), invariants.ErrLedgerNameRequired)
	require.ErrorIs(t, ValidateAsset("lowercase"), invariants.ErrAssetInvalid)
}

func TestValidateColor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{name: "empty is the uncolored bucket", input: ""},
		{name: "single letter", input: "R"},
		{name: "typical tag", input: "GRANTS"},
		{name: "max length (32)", input: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		{name: "contains null byte", input: "A\x00B", wantErr: ErrColorInvalid},
		{name: "lowercase rejected", input: "red", wantErr: ErrColorInvalid},
		{name: "digit rejected", input: "RED2", wantErr: ErrColorInvalid},
		{name: "hyphen rejected", input: "R-G", wantErr: ErrColorInvalid},
		{name: "space rejected", input: "R G", wantErr: ErrColorInvalid},
		{name: "high byte rejected", input: "RÉD", wantErr: ErrColorInvalid},
		{name: "over max length (33)", input: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", wantErr: ErrColorTooLong},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateColor(tt.input)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
