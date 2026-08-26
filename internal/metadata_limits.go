package ledger

import (
	"fmt"
	"sort"

	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
)

const (
	MaxMetadataEntries     = 128
	MaxMetadataKeySize     = 256
	MaxMetadataValueSize   = 16 << 10
	MaxMetadataSize        = 64 << 10
	MaxCommandMetadataSize = 256 << 10
)

// ErrMetadataLimitExceeded reports which metadata constraint rejected a write.
// Sizes are measured in UTF-8 bytes, not runes.
type ErrMetadataLimitExceeded struct {
	Constraint string
	Maximum    int
	Actual     int
}

func (e ErrMetadataLimitExceeded) Error() string {
	unit := "bytes"
	if e.Constraint == "entry count" {
		unit = "entries"
	}
	return fmt.Sprintf("metadata %s exceeds maximum of %d %s (got %d)", e.Constraint, e.Maximum, unit, e.Actual)
}

func (e ErrMetadataLimitExceeded) Is(err error) bool {
	_, ok := err.(ErrMetadataLimitExceeded)
	return ok
}

func metadataSize(m metadata.Metadata) int {
	size := 0
	for key, value := range m {
		size += len(key) + len(value)
	}
	return size
}

// ValidateMetadata enforces the limits for one transaction or account metadata
// object. It intentionally does not validate key syntax, which would be a
// backwards-incompatible change for Ledger v2.
func ValidateMetadata(m metadata.Metadata) error {
	if len(m) > MaxMetadataEntries {
		return ErrMetadataLimitExceeded{
			Constraint: "entry count",
			Maximum:    MaxMetadataEntries,
			Actual:     len(m),
		}
	}

	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		value := m[key]
		if len(key) > MaxMetadataKeySize {
			return ErrMetadataLimitExceeded{
				Constraint: "key size",
				Maximum:    MaxMetadataKeySize,
				Actual:     len(key),
			}
		}
		if len(value) > MaxMetadataValueSize {
			return ErrMetadataLimitExceeded{
				Constraint: "value size",
				Maximum:    MaxMetadataValueSize,
				Actual:     len(value),
			}
		}
	}

	size := metadataSize(m)
	if size > MaxMetadataSize {
		return ErrMetadataLimitExceeded{
			Constraint: "entity size",
			Maximum:    MaxMetadataSize,
			Actual:     size,
		}
	}

	return nil
}

// ValidateCommandMetadata validates every metadata object produced by a
// transaction command, then bounds their combined size. Sorting account names
// keeps the selected validation error stable when several accounts are invalid.
func ValidateCommandMetadata(transactionMetadata metadata.Metadata, accountMetadata AccountMetadata) error {
	if err := ValidateMetadata(transactionMetadata); err != nil {
		return err
	}

	accounts := make([]string, 0, len(accountMetadata))
	for account := range accountMetadata {
		accounts = append(accounts, account)
	}
	sort.Strings(accounts)

	totalSize := metadataSize(transactionMetadata)
	for _, account := range accounts {
		m := accountMetadata[account]
		if err := ValidateMetadata(m); err != nil {
			return err
		}
		totalSize += metadataSize(m)
	}

	if totalSize > MaxCommandMetadataSize {
		return ErrMetadataLimitExceeded{
			Constraint: "command size",
			Maximum:    MaxCommandMetadataSize,
			Actual:     totalSize,
		}
	}

	return nil
}
