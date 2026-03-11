package readstore

import "github.com/formancehq/ledger-v3-poc/internal/storage/pebblecfg"

// Config is the Pebble configuration for the read index store.
// It uses the same tunables as the primary store (pebblecfg.Config).
type Config = pebblecfg.Config

// DefaultConfig returns the default Pebble configuration for the read index.
// These defaults are intentionally smaller than the primary DAL store because
// the read index is a derived view that can be rebuilt from the Raft log.
func DefaultConfig() Config {
	return Config{
		MemTableSize:                64 << 20, // 64MB
		MemTableStopWritesThreshold: 4,
		L0CompactionThreshold:       4,
		L0StopWritesThreshold:       12,
		LBaseMaxBytes:               512 << 20, // 512MB
		CacheSize:                   64 << 20,  // 64MB
		TargetFileSize:              64 << 20,  // 64MB
		BytesPerSync:                512 << 10, // 512KB
		MaxConcurrentCompactions:    1,
	}
}
