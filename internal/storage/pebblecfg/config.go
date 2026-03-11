package pebblecfg

// Config contains the common Pebble tunables shared by both the primary
// DAL store and the read index store.
type Config struct {
	// MemTableSize is the size of a single memtable in bytes.
	MemTableSize uint64 `yaml:"memTableSize"`

	// MemTableStopWritesThreshold is the number of memtables before writes are stopped.
	MemTableStopWritesThreshold int `yaml:"memTableStopWritesThreshold"`

	// L0CompactionThreshold triggers L0→L1 compactions when L0 files reach this count.
	L0CompactionThreshold int `yaml:"l0CompactionThreshold"`

	// L0StopWritesThreshold is the L0 file count at which writes are stopped.
	L0StopWritesThreshold int `yaml:"l0StopWritesThreshold"`

	// LBaseMaxBytes is the maximum size of the base level (L1) in bytes.
	LBaseMaxBytes int64 `yaml:"lBaseMaxBytes"`

	// CacheSize is the size of the block cache in bytes.
	CacheSize int64 `yaml:"cacheSize"`

	// TargetFileSize is the target size for SST files in bytes.
	TargetFileSize int64 `yaml:"targetFileSize"`

	// BytesPerSync is the number of bytes written before syncing to disk.
	BytesPerSync int `yaml:"bytesPerSync"`

	// MaxConcurrentCompactions is the maximum number of concurrent compaction operations.
	MaxConcurrentCompactions int `yaml:"maxConcurrentCompactions"`
}
