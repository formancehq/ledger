package grpc

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// BenchmarkCheckpointMillionKeyScan isolates the open cost paid by a paged
// checkpoint reader. It uses one million account-shaped keys and 1000-key
// pages; it does not include the API's account decoding or network transfer.
func BenchmarkCheckpointMillionKeyScan(b *testing.B) {
	const keys = 1_000_000
	const pageSize = 1000
	mainPath := filepath.Join(b.TempDir(), "main")
	indexRoot := filepath.Join(b.TempDir(), "index")
	indexPath := filepath.Join(indexRoot, "readindex")

	db, err := pebble.Open(mainPath, nil)
	if err != nil {
		b.Fatal(err)
	}
	for start := 0; start < keys; start += 10_000 {
		batch := db.NewBatch()
		for n := start; n < start+10_000; n++ {
			if err := batch.Set(fmt.Appendf(nil, "account/%08d", n), make([]byte, 32), nil); err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.Commit(nil); err != nil {
			b.Fatal(err)
		}
		if err := batch.Close(); err != nil {
			b.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		b.Fatal(err)
	}
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	index, err := readstore.New(indexRoot, testLogger(), readstore.DefaultConfig())
	if err != nil {
		b.Fatal(err)
	}
	if err := index.Close(); err != nil {
		b.Fatal(err)
	}

	scanPage := func(main *dal.Store, offset int) {
		h, err := main.NewReadHandle()
		if err != nil {
			b.Fatal(err)
		}
		iter, err := h.NewIter(nil)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for ok := iter.SeekGE(fmt.Appendf(nil, "account/%08d", offset)); ok && count < pageSize; ok = iter.Next() {
			count++
		}
		if count != pageSize {
			b.Fatalf("page %d: got %d keys", offset, count)
		}
		if err := iter.Close(); err != nil {
			b.Fatal(err)
		}
		if err := h.Close(); err != nil {
			b.Fatal(err)
		}
	}

	b.Run("reopen-every-page", func(b *testing.B) {
		for range b.N {
			for offset := 0; offset < keys; offset += pageSize {
				main, index, err := openCheckpointDirs(mainPath, indexPath, testLogger())
				if err != nil {
					b.Fatal(err)
				}
				scanPage(main, offset)
				if err := index.Close(); err != nil {
					b.Fatal(err)
				}
				if err := main.Close(); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("persistent-open", func(b *testing.B) {
		for range b.N {
			main, err := dal.OpenDirect(mainPath, testLogger())
			if err != nil {
				b.Fatal(err)
			}
			for offset := 0; offset < keys; offset += pageSize {
				scanPage(main, offset)
			}
			if err := main.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("cached-between-pages", func(b *testing.B) {
		var cache checkpointStoreCache
		for range b.N {
			for offset := 0; offset < keys; offset += pageSize {
				main, _, release, err := cache.acquire(b.Context(), 1, testLogger(), func() (*dal.Store, *readstore.Store, error) {
					return openCheckpointDirs(mainPath, indexPath, testLogger())
				})
				if err != nil {
					b.Fatal(err)
				}
				scanPage(main, offset)
				release()
			}
		}
		cache.evict(1)
	})
}
