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

var (
	ErrLedgerNameContainsNullByte = errors.New("ledger name must not contain null bytes")
	ErrLedgerNameTooLong          = fmt.Errorf("ledger name exceeds maximum length of %d bytes", maxLedgerNameLength)
	ErrMetadataKeyContainsNullByte = errors.New("metadata key must not contain null bytes")
	ErrMetadataKeyEmpty            = errors.New("metadata key must not be empty")
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
