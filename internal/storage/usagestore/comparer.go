package usagestore

import "github.com/formancehq/ledger/v3/internal/storage/dal"

// The persisted RocksDB comparator name and ordering must remain stable.
// Pebble files are a different format; the derived projection is rebuilt from
// the audit log when switching engines.
const usageStoreComparerName = "formance.usagestore.v1"

// CappedPrefix uses the whole key for the short internal singleton keys and
// the prefix byte plus padded ledger name for ledger-scoped keys.
const ledgerScopedPrefixLen = 1 + dal.LedgerNameFixedSize
