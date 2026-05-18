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
