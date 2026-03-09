package attributes

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
)

const (
	cleanupInterval = 30 * time.Second

	// cleanupSafeMargin is subtracted from LastIndexedRaftIndex before cleanup.
	// This prevents a race where a concurrent query reads LastIndexedRaftIndex=N,
	// but the cleaner (having read LastIndexedRaftIndex=N+k) deletes entries
	// that the query still needs. The margin ensures the cleaner only removes
	// entries well below any in-flight read's maxRaftIndex.
	cleanupSafeMargin = 1000
)

// CleanupOldEntries scans the entire attributes zone and deletes stale entries
// that are older than the latest value at or below maxRaftIndex, per group.
//
// A group is defined by everything except the trailing 8-byte raft index:
// [KeyPrefixAttributes][CanonicalKey][AttrType]. Within each group, only the
// latest entry with raftIndex <= maxRaftIndex is kept; all older ones are
// deleted via batch DeleteRange.
//
// Returns the number of delete-range operations issued.
func CleanupOldEntries(store *dal.Store, maxRaftIndex uint64) (int, error) {
	iter, err := store.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.ZoneAttributesStart},
		UpperBound: []byte{dal.ZoneAttributesEnd},
	})
	if err != nil {
		return 0, fmt.Errorf("creating iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	batch := store.NewBatch()
	minKeyLen := 1 + SuffixLen // prefix byte + attrType + raftIndex
	deleteOps := 0

	var (
		prevGroup string
		latestIdx uint64
		count     int
	)

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) <= minKeyLen {
			continue
		}

		raftIdx := binary.BigEndian.Uint64(key[len(key)-8:])
		group := string(key[:len(key)-8])

		if group != prevGroup {
			// Flush previous group
			if count > 1 {
				if err := deleteGroupOldEntries(batch, prevGroup, latestIdx); err != nil {
					_ = batch.Cancel()

					return 0, err
				}

				deleteOps++
			}

			prevGroup = group
			latestIdx = 0
			count = 0
		}

		if raftIdx <= maxRaftIndex {
			latestIdx = raftIdx // ascending order → last assignment is the latest
			count++
		}
	}

	if err := iter.Error(); err != nil {
		_ = batch.Cancel()

		return 0, fmt.Errorf("iterating attributes: %w", err)
	}

	// Flush final group
	if count > 1 {
		if err := deleteGroupOldEntries(batch, prevGroup, latestIdx); err != nil {
			_ = batch.Cancel()

			return 0, err
		}

		deleteOps++
	}

	if deleteOps > 0 {
		if err := batch.Commit(); err != nil {
			return 0, fmt.Errorf("committing cleanup batch: %w", err)
		}
	} else {
		_ = batch.Cancel()
	}

	return deleteOps, nil
}

// deleteGroupOldEntries issues a DeleteRange for all entries in a group
// with raft index strictly less than latestIdx.
func deleteGroupOldEntries(batch *dal.Batch, group string, latestIdx uint64) error {
	lower := []byte(group)

	upper := make([]byte, len(group)+8)
	copy(upper, group)
	binary.BigEndian.PutUint64(upper[len(group):], latestIdx)

	return batch.DeleteRange(lower, upper, pebble.NoSync)
}

// Cleaner periodically removes stale attribute entries from Pebble.
// It runs on every node since each node has its own Pebble + bbolt stores.
type Cleaner struct {
	store     *dal.Store
	readStore *readstore.Store
	logger    logging.Logger
	w         worker.Worker
}

// NewCleaner creates a new Cleaner that will periodically clean up old attribute entries.
func NewCleaner(store *dal.Store, readStore *readstore.Store, logger logging.Logger) *Cleaner {
	return &Cleaner{
		store:     store,
		readStore: readStore,
		logger:    logger,
		w:         worker.New(),
	}
}

// Start begins the periodic cleanup loop.
func (c *Cleaner) Start() {
	c.w.Run(func(stop <-chan struct{}) {
		worker.RunTicker(stop, cleanupInterval, c.tick)
	})
}

// Stop signals the cleanup loop to stop and waits for it to finish.
func (c *Cleaner) Stop() {
	c.w.Stop()
}

func (c *Cleaner) tick() {
	maxRaftIndex, err := c.readStore.LastIndexedRaftIndex()
	if err != nil {
		c.logger.Errorf("Attribute cleaner: reading last indexed raft index: %v", err)

		return
	}

	if maxRaftIndex <= cleanupSafeMargin {
		return // too early to clean
	}

	safeIndex := maxRaftIndex - cleanupSafeMargin

	deleteOps, err := CleanupOldEntries(c.store, safeIndex)
	if err != nil {
		c.logger.Errorf("Attribute cleaner: cleanup failed: %v", err)

		return
	}

	if deleteOps > 0 {
		c.logger.Infof("Attribute cleaner: issued %d delete-range operations", deleteOps)
	}
}
