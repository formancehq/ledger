package domain

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// maxLedgerNameLength is the maximum allowed length for a ledger name.
// Pebble keys must stay reasonable; 256 bytes is generous for a human-readable identifier.
const maxLedgerNameLength = 256

// maxAccountAddressLength is the maximum allowed length for an account address.
const maxAccountAddressLength = 1024

var (
	ErrLedgerNameContainsNullByte  = errors.New("ledger name must not contain null bytes")
	ErrLedgerNameTooLong           = fmt.Errorf("ledger name exceeds maximum length of %d bytes", maxLedgerNameLength)
	ErrMetadataKeyContainsNullByte = errors.New("metadata key must not contain null bytes")
	ErrMetadataKeyEmpty            = errors.New("metadata key must not be empty")
	ErrAccountAddressEmpty         = errors.New("account address must not be empty")
	ErrAccountAddressInvalidChar   = errors.New("account address must contain only letters, digits, colons, underscores, and hyphens")
	ErrAccountAddressTooLong       = fmt.Errorf("account address exceeds maximum length of %d bytes", maxAccountAddressLength)

	ErrAssetInvalid = errors.New("asset must match [A-Z][A-Z0-9]{0,16}(_[A-Z]{1,16})?(/\\d{1,6})?")
)

// ValidateLedgerName checks that a ledger name is safe for use in Pebble key encoding.
// Null bytes would corrupt null-terminated key layouts; length is capped to prevent
// oversized keys.
func ValidateLedgerName(name string) error {
	if name == "" {
		return ErrLedgerNameRequired
	}

	if strings.ContainsRune(name, 0) {
		return ErrLedgerNameContainsNullByte
	}

	if len(name) > maxLedgerNameLength {
		return ErrLedgerNameTooLong
	}

	return nil
}

// isAccountAddressChar returns true if the rune is allowed in an account address.
// Segments are [a-zA-Z0-9_-]+, separated by colons.
func isAccountAddressChar(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == ':' || r == '_' || r == '-'
}

// ValidateAccountAddress checks that an account address contains only allowed characters
// (letters, digits, colons, underscores, hyphens) and is within length limits.
func ValidateAccountAddress(address string) error {
	if address == "" {
		return ErrAccountAddressEmpty
	}

	if len(address) > maxAccountAddressLength {
		return ErrAccountAddressTooLong
	}

	for _, r := range address {
		if !isAccountAddressChar(r) {
			return ErrAccountAddressInvalidChar
		}
	}

	return nil
}

// ValidateMetadataKey checks that a metadata key is safe for use in Pebble key encoding.
// Null bytes would corrupt null-terminated key layouts used in canonical keys and the read index.
func ValidateMetadataKey(key string) error {
	if key == "" {
		return ErrMetadataKeyEmpty
	}

	if strings.ContainsRune(key, 0) {
		return ErrMetadataKeyContainsNullByte
	}

	return nil
}

// ValidateAsset checks that an asset string matches the expected format:
// [A-Z][A-Z0-9]{0,16}(_[A-Z]{1,16})?(/\d{1,6})?
// Examples: "USD", "EUR/2", "BTC/8", "CUSTOM_TOKEN/6".
func ValidateAsset(asset string) error {
	if len(asset) == 0 {
		return ErrAssetInvalid
	}

	base, precisionStr, hasPrecision := strings.Cut(asset, "/")

	if !validateAssetBase(base) {
		return ErrAssetInvalid
	}

	if hasPrecision && !isDigits(precisionStr, 1, 6) {
		return ErrAssetInvalid
	}

	return nil
}

// validateAssetBase checks the base part: [A-Z][A-Z0-9]{0,16}(_[A-Z]{1,16})?
func validateAssetBase(base string) bool {
	if len(base) == 0 {
		return false
	}

	head, tail, hasUnderscore := strings.Cut(base, "_")

	if !isUpperAlphaStart(head, 17) {
		return false
	}

	if hasUnderscore && !isUpperAlpha(tail, 1, 16) {
		return false
	}

	return true
}

func isUpperAlphaStart(s string, maxLen int) bool {
	if len(s) == 0 || len(s) > maxLen {
		return false
	}

	if s[0] < 'A' || s[0] > 'Z' {
		return false
	}

	for i := 1; i < len(s); i++ {
		c := s[i]
		if !((c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
			return false
		}
	}

	return true
}

func isUpperAlpha(s string, minLen, maxLen int) bool {
	if len(s) < minLen || len(s) > maxLen {
		return false
	}

	for i := range s {
		if s[i] < 'A' || s[i] > 'Z' {
			return false
		}
	}

	return true
}

func isDigits(s string, minLen, maxLen int) bool {
	if len(s) < minLen || len(s) > maxLen {
		return false
	}

	for i := range s {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}

	return true
}

// ParseAssetPrecision splits an asset string into its base name and precision.
// "USD/4" → ("USD", 4), "EUR" → ("EUR", 0).
func ParseAssetPrecision(asset string) (string, uint8) {
	base, precStr, found := strings.Cut(asset, "/")
	if !found {
		return asset, 0
	}

	prec, _ := strconv.ParseUint(precStr, 10, 8)

	return base, uint8(prec)
}

// FormatAsset reconstructs an asset string from base and precision.
// ("USD", 4) → "USD/4", ("EUR", 0) → "EUR".
func FormatAsset(base string, precision uint8) string {
	if precision == 0 {
		return base
	}

	return fmt.Sprintf("%s/%d", base, precision)
}
