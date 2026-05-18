package domain

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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
