//go:build rocksdb

package dal

import (
	"github.com/formancehq/ledger/v3/internal/storage/engine"
	"github.com/formancehq/ledger/v3/internal/storage/engine/rocksengine"
)

// EngineRocksDB selects the grocksdb-backed engine (RocksDB POC).
const EngineRocksDB = "rocksdb"

func init() {
	RegisterEngine(EngineRocksDB, func(dir string, cfg Config) (engine.DB, error) {
		return rocksengine.Open(dir, engine.Options{
			CacheBytes:               cfg.CacheSize,
			MemTableBytes:            int64(cfg.MemTableSize),
			BloomBitsPerKey:          10,
			MaxConcurrentCompactions: cfg.MaxConcurrentCompactions,
			ReadOnly:                 cfg.readOnly,
		})
	})
}
