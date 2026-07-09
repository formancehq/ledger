package domain

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/invariants"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestValidateWrapping pins the wrapper contract: each Validate* function in
// this package calls invariants.Validate* and maps the primitive sentinel
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
		{name: "metadata key empty", got: ValidateMetadataKey(""), want: ErrMetadataKeyEmpty},
		{name: "metadata key null byte", got: ValidateMetadataKey("a\x00b"), want: ErrMetadataKeyInvalidChar},
		{name: "metadata key space", got: ValidateMetadataKey("a b"), want: ErrMetadataKeyInvalidChar},
		{name: "metadata key non-ASCII", got: ValidateMetadataKey("étape"), want: ErrMetadataKeyInvalidChar},
		{name: "metadata string null byte", got: ValidateMetadataString("a\x00b"), want: ErrMetadataValueContainsNullByte},
		{name: "asset invalid", got: ValidateAsset("lowercase"), want: ErrAssetInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Same(t, tt.want, tt.got, "wrapper must return the pre-instantiated sentinel")
			require.Equal(t, KindValidation, Kind(tt.got))
			require.Equal(t, ErrReasonValidation, tt.got.Reason())
			require.Nil(t, tt.got.Metadata())
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
	require.Nil(t, ValidateMetadataString("admin"))
	require.Nil(t, ValidateAsset("EUR/2"))
}

// TestValidateMetadataValue exercises the *commonpb.MetadataValue dispatch,
// which stays local to this package because it depends on the internal proto.
func TestValidateMetadataValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   *commonpb.MetadataValue
		wantErr Describable
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

			got := ValidateMetadataValue(tt.input)
			if tt.wantErr != nil {
				require.Same(t, tt.wantErr, got)
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
