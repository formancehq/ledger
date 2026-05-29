package store

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// NewCacheStatsCommand creates the store cache-stats command.
func NewCacheStatsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "cache-stats <data-dir>",
		Short: "Show cache zone (0xFF) statistics from a Pebble store (offline)",
		Long: `Open a Pebble data directory in read-only mode and report:
- Cache generation metadata (current generation, gen0/gen1 base indices)
- Number of entries per generation byte and cache type
- Number of attribute entries per type for comparison
- Keys present in attributes but missing from cache (potential cache gaps)

This is an offline operation — the server must not be running, or the data-dir
must be a checkpoint/snapshot copy.`,
		Args: cobra.ExactArgs(1),
		RunE: runCacheStats,
	}
}

func runCacheStats(cmd *cobra.Command, args []string) error {
	dataDir := args[0]

	db, err := pebble.Open(dataDir, &pebble.Options{
		Logger:   dal.DiscardPebbleLogger(),
		ReadOnly: true,
	})
	if err != nil {
		return fmt.Errorf("opening pebble at %s: %w", dataDir, err)
	}

	defer func() { _ = db.Close() }()

	// Read cache metadata
	currentGen := uint64(0)

	if val, closer, getErr := db.Get([]byte{dal.ZoneCache, dal.SubCacheMeta}); getErr == nil {
		meta := &raftcmdpb.CacheSnapshotMeta{}
		if unmarshalErr := meta.UnmarshalVT(val); unmarshalErr == nil {
			currentGen = meta.GetCurrentGeneration()
		}

		_ = closer.Close()
	}

	gen0Byte := byte(currentGen % 2)
	gen1Byte := byte((currentGen + 1) % 2)

	gen0Base := readGenBase(db, gen0Byte)
	gen1Base := readGenBase(db, gen1Byte)

	fmt.Printf("Cache Metadata:\n")
	fmt.Printf("  currentGeneration: %d\n", currentGen)
	fmt.Printf("  gen0: byte=%d, base=%d\n", gen0Byte, gen0Base)
	fmt.Printf("  gen1: byte=%d, base=%d\n", gen1Byte, gen1Base)
	fmt.Println()

	cacheTypes := []struct {
		name string
		code byte
	}{
		{"volumes", dal.SubAttrVolume},
		{"metadata", dal.SubAttrMetadata},
		{"references", dal.SubAttrReference},
		{"ledgers", dal.SubAttrLedger},
		{"boundaries", dal.SubAttrBoundary},
		{"transactions", dal.SubAttrTransaction},
	}

	fmt.Printf("0xFF Cache Zone Entries:\n")
	fmt.Printf("  %-15s %8s %8s\n", "Type", "Gen0", "Gen1")

	totalGen0 := uint64(0)
	totalGen1 := uint64(0)

	for _, ct := range cacheTypes {
		g0 := countPebbleEntries(db, dal.ZoneCache, gen0Byte, ct.code)
		g1 := countPebbleEntries(db, dal.ZoneCache, gen1Byte, ct.code)
		totalGen0 += g0
		totalGen1 += g1
		fmt.Printf("  %-15s %8d %8d\n", ct.name, g0, g1)
	}

	fmt.Printf("  %-15s %8d %8d\n", "TOTAL", totalGen0, totalGen1)
	fmt.Println()

	fmt.Printf("Attribute Zone Entries:\n")

	totalAttr := uint64(0)

	for _, ct := range cacheTypes {
		n := countPebbleAttrEntries(db, ct.code)
		totalAttr += n
		fmt.Printf("  %-15s %8d\n", ct.name, n)
	}

	fmt.Printf("  %-15s %8d\n", "TOTAL", totalAttr)
	fmt.Println()

	// Find volume keys in attributes but missing from BOTH cache gens
	hasher := attributes.NewKeyHasher(attributes.DefaultSeeds)
	missing := findVolumesNotInCache(db, dal.SubAttrVolume, gen0Byte, gen1Byte, hasher)

	fmt.Printf("Volume keys in attributes but NOT in cache (either gen): %d\n", len(missing))

	for i, key := range missing {
		if i >= 20 {
			fmt.Printf("  ... and %d more\n", len(missing)-20)

			break
		}

		fmt.Printf("  %x\n", key)
	}

	return nil
}

func readGenBase(db *pebble.DB, genByte byte) uint64 {
	val, closer, err := db.Get([]byte{dal.ZoneCache, genByte, dal.SubCacheGenMeta})
	if err != nil {
		return 0
	}

	defer func() { _ = closer.Close() }()

	meta := &raftcmdpb.CacheGenerationMeta{}
	if unmarshalErr := meta.UnmarshalVT(val); unmarshalErr != nil {
		return 0
	}

	return meta.GetBaseIndex()
}

func countPebbleEntries(db *pebble.DB, zone, genByte, cacheType byte) uint64 {
	lower := []byte{zone, genByte, cacheType}
	upper := []byte{zone, genByte, cacheType + 1}

	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return 0
	}

	defer func() { _ = iter.Close() }()

	var count uint64
	for iter.First(); iter.Valid(); iter.Next() {
		if len(iter.Key()) >= 3+16 {
			count++
		}
	}

	return count
}

func countPebbleAttrEntries(db *pebble.DB, attrType byte) uint64 {
	lower := []byte{dal.ZoneAttributes, attrType}
	upper := []byte{dal.ZoneAttributes, attrType + 1}

	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return 0
	}

	defer func() { _ = iter.Close() }()

	var count uint64
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	return count
}

func findVolumesNotInCache(db *pebble.DB, attrType, gen0Byte, gen1Byte byte, hasher *attributes.KeyHasher) [][]byte {
	lower := []byte{dal.ZoneAttributes, attrType}
	upper := []byte{dal.ZoneAttributes, attrType + 1}

	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil
	}

	defer func() { _ = iter.Close() }()

	var missing [][]byte

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < 2 {
			continue
		}

		canonicalKey := key[2:]
		u128, _ := hasher.MakeKey(canonicalKey)

		if !hasCacheEntry(db, gen0Byte, attrType, u128) && !hasCacheEntry(db, gen1Byte, attrType, u128) {
			missing = append(missing, append([]byte(nil), canonicalKey...))
		}
	}

	return missing
}

func hasCacheEntry(db *pebble.DB, genByte, cacheType byte, u128 attributes.U128) bool {
	var key [3 + 16]byte
	key[0] = dal.ZoneCache
	key[1] = genByte
	key[2] = cacheType
	copy(key[3:], u128[:])

	_, closer, err := db.Get(key[:])
	if err != nil {
		return false
	}

	_ = closer.Close()

	return true
}
