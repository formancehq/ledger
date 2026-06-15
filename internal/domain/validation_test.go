package domain

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestValidateLedgerName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{name: "valid simple name", input: "default"},
		{name: "valid with hyphens", input: "my-ledger-123"},
		{name: "valid with dots", input: "ledger.prod"},
		{name: "empty", input: "", wantErr: ErrLedgerNameRequired},
		{name: "contains null byte", input: "ledger\x00evil", wantErr: ErrLedgerNameContainsNullByte},
		{name: "null byte only", input: "\x00", wantErr: ErrLedgerNameContainsNullByte},
		// Names that flow through `x-next-cursor` gRPC trailers must survive
		// the HTTP/2-header value charset. Anything outside printable ASCII
		// (newlines, CR, multibyte UTF-8) would either be stripped or fail
		// the stream — so the validator rejects them up-front instead of
		// admitting a name we cannot paginate.
		{name: "contains newline", input: "ledger\nevil", wantErr: ErrLedgerNameInvalidChar},
		{name: "contains carriage return", input: "ledger\revil", wantErr: ErrLedgerNameInvalidChar},
		{name: "contains tab", input: "ledger\tevil", wantErr: ErrLedgerNameInvalidChar},
		{name: "contains DEL", input: "ledger\x7Fevil", wantErr: ErrLedgerNameInvalidChar},
		{name: "contains non-ASCII utf8", input: "ledgér", wantErr: ErrLedgerNameInvalidChar},
		{name: "too long", input: strings.Repeat("a", maxLedgerNameLength+1), wantErr: ErrLedgerNameTooLong},
		{name: "max length", input: strings.Repeat("a", maxLedgerNameLength)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateLedgerName(tt.input)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateNumscriptName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{name: "valid simple", input: "transfer"},
		{name: "valid with hyphens", input: "user-transfer-v2"},
		{name: "valid with dots", input: "transfers.v1"},
		{name: "empty", input: "", wantErr: ErrNumscriptNameRequired},
		// Same charset rationale as ledger names: numscript names become
		// resume cursors on the `numscripts list` stream, so they must be
		// safe for gRPC metadata trailers.
		{name: "contains newline", input: "trans\nfer", wantErr: ErrNumscriptNameInvalidChar},
		{name: "contains null byte", input: "trans\x00fer", wantErr: ErrNumscriptNameInvalidChar},
		{name: "contains non-ASCII utf8", input: "transférer", wantErr: ErrNumscriptNameInvalidChar},
		{name: "too long", input: strings.Repeat("a", maxNumscriptNameLength+1), wantErr: ErrNumscriptNameTooLong},
		{name: "max length", input: strings.Repeat("a", maxNumscriptNameLength)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateNumscriptName(tt.input)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateSigningKeyID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{name: "valid simple", input: "admin-key-1"},
		{name: "valid with dots", input: "kms.prod.2026"},
		{name: "valid with slashes", input: "team/treasury/v2"},
		{name: "empty", input: "", wantErr: ErrSigningKeyIDRequired},
		// Key IDs end up in the `x-next-cursor` trailer for the
		// `signing keys list` stream — same HTTP/2-header charset constraint
		// as ledger and numscript names.
		{name: "contains newline", input: "key\nid", wantErr: ErrSigningKeyIDInvalidChar},
		{name: "contains null byte", input: "key\x00id", wantErr: ErrSigningKeyIDInvalidChar},
		{name: "contains non-ASCII utf8", input: "clé", wantErr: ErrSigningKeyIDInvalidChar},
		{name: "too long", input: strings.Repeat("a", maxSigningKeyIDLength+1), wantErr: ErrSigningKeyIDTooLong},
		{name: "max length", input: strings.Repeat("a", maxSigningKeyIDLength)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateSigningKeyID(tt.input)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateMetadataKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{name: "valid key", input: "category"},
		{name: "valid with dots", input: "user.role"},
		{name: "empty", input: "", wantErr: ErrMetadataKeyEmpty},
		{name: "contains null byte", input: "key\x00value", wantErr: ErrMetadataKeyContainsNullByte},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateMetadataKey(tt.input)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

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

			err := ValidateMetadataValue(tt.input)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateAccountAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{name: "valid simple", input: "world"},
		{name: "valid with colon segments", input: "users:alice:checking"},
		{name: "valid uppercase", input: "USD"},
		{name: "valid mixed", input: "platform:fees"},
		{name: "valid digits", input: "user123"},
		{name: "empty", input: "", wantErr: ErrAccountAddressEmpty},
		{name: "contains null byte", input: "account\x00evil", wantErr: ErrAccountAddressInvalidChar},
		{name: "contains space", input: "my account", wantErr: ErrAccountAddressInvalidChar},
		{name: "valid hyphen", input: "my-account"},
		{name: "valid underscore", input: "my_account"},
		{name: "contains dot", input: "my.account", wantErr: ErrAccountAddressInvalidChar},
		{name: "contains slash", input: "a/b", wantErr: ErrAccountAddressInvalidChar},
		{name: "too long", input: strings.Repeat("a", maxAccountAddressLength+1), wantErr: ErrAccountAddressTooLong},
		{name: "max length", input: strings.Repeat("a", maxAccountAddressLength)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateAccountAddress(tt.input)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateAsset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{name: "simple", input: "USD"},
		{name: "with precision", input: "EUR/2"},
		{name: "long precision", input: "BTC/8"},
		{name: "max base length", input: "ABCDEFGHIJKLMNOPQ"},
		{name: "with underscore", input: "CUSTOM_TOKEN"},
		{name: "underscore and precision", input: "CUSTOM_TOKEN/6"},
		{name: "single char", input: "A"},
		{name: "alphanumeric base", input: "USD2"},
		{name: "empty", input: "", wantErr: ErrAssetInvalid},
		{name: "lowercase", input: "usd", wantErr: ErrAssetInvalid},
		{name: "starts with digit", input: "1USD", wantErr: ErrAssetInvalid},
		{name: "contains hyphen", input: "US-D", wantErr: ErrAssetInvalid},
		{name: "contains space", input: "US D", wantErr: ErrAssetInvalid},
		{name: "base too long", input: "ABCDEFGHIJKLMNOPQR", wantErr: ErrAssetInvalid},
		{name: "precision too long", input: "USD/1234567", wantErr: ErrAssetInvalid},
		{name: "underscore suffix lowercase", input: "USD_eur", wantErr: ErrAssetInvalid},
		{name: "double slash", input: "USD//2", wantErr: ErrAssetInvalid},
		{name: "trailing slash", input: "USD/", wantErr: ErrAssetInvalid},
		{name: "leading underscore", input: "_USD", wantErr: ErrAssetInvalid},
		// #303: precision must fit in uint8 (the volume-key byte) and use a
		// single canonical form per (base, precision) pair.
		{name: "precision overflows uint8", input: "USD/256", wantErr: ErrAssetInvalid},
		{name: "precision way over uint8", input: "USD/999999", wantErr: ErrAssetInvalid},
		{name: "precision boundary 255", input: "USD/255"},
		{name: "precision min 1", input: "USD/1"},
		{name: "precision zero aliases bare base", input: "USD/0", wantErr: ErrAssetInvalid},
		{name: "precision leading zero", input: "USD/02", wantErr: ErrAssetInvalid},
		{name: "precision multi leading zero", input: "USD/007", wantErr: ErrAssetInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateAsset(tt.input)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestValidateAsset_CanonicalRoundTrip pins the contract used by the
// volume-key encoding (#303): every input ValidateAsset accepts must
// survive the ParseAssetPrecision → FormatAsset round trip unchanged.
// If two valid inputs collapsed onto the same (base, precision) pair,
// the canonical form returned by FormatAsset would not match one of
// them and consensus-deterministic asset aliasing would already exist
// in production.
func TestValidateAsset_CanonicalRoundTrip(t *testing.T) {
	t.Parallel()

	valid := []string{
		"USD",
		"EUR/2",
		"BTC/8",
		"USD/1",
		"USD/255",
		"CUSTOM_TOKEN",
		"CUSTOM_TOKEN/6",
		"A",
		"USD2",
		"ABCDEFGHIJKLMNOPQ",
	}

	for _, asset := range valid {
		t.Run(asset, func(t *testing.T) {
			t.Parallel()

			require.NoError(t, ValidateAsset(asset))

			base, precision := ParseAssetPrecision(asset)
			require.Equal(t, asset, FormatAsset(base, precision),
				"every accepted asset must round-trip through Parse/Format unchanged (#303)")
		})
	}
}
