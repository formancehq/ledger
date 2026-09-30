package kv

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/linxGnu/grocksdb"
)

func TestRocksDBStorageContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live")
	db, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()

	if _, _, err := db.Get([]byte("absent")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing key: got %v", err)
	}
	if err := db.Set([]byte("empty"), nil, Sync); err != nil {
		t.Fatal(err)
	}
	empty, emptyCloser, err := db.Get([]byte("empty"))
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("empty value has length %d", len(empty))
	}
	if err := emptyCloser.Close(); err != nil {
		t.Fatal(err)
	}

	batch := db.NewBatch()
	if err := batch.Set([]byte("ledger/a"), []byte("first"), NoSync); err != nil {
		t.Fatal(err)
	}
	if err := batch.Set([]byte("ledger/b"), []byte("second"), NoSync); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(Sync); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}

	snapshot := db.NewSnapshot()
	defer func() {
		if err := snapshot.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := db.Set([]byte("ledger/a"), []byte("updated"), Sync); err != nil {
		t.Fatal(err)
	}
	value, closer, err := snapshot.Get([]byte("ledger/a"))
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "first" {
		t.Fatalf("snapshot changed: %q", value)
	}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}

	iter, err := db.NewIter(&IterOptions{LowerBound: []byte("ledger/"), UpperBound: []byte("ledger0")})
	if err != nil {
		t.Fatal(err)
	}
	if !iter.SeekGE([]byte("ledger/a")) || string(iter.Key()) != "ledger/a" {
		t.Fatal("forward seek failed")
	}
	if !iter.Next() || string(iter.Key()) != "ledger/b" {
		t.Fatal("forward iteration failed")
	}
	if !iter.SeekLT([]byte("ledger/b")) || string(iter.Key()) != "ledger/a" {
		t.Fatal("reverse seek failed")
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}

	checkpoint := filepath.Join(t.TempDir(), "checkpoint")
	if err := db.Checkpoint(checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := db.SyncWAL(); err != nil {
		t.Fatal(err)
	}
	copyDB, err := Open(checkpoint, Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := copyDB.Close(); err != nil {
			t.Error(err)
		}
	}()
	value, closer, err = copyDB.Get([]byte("ledger/a"))
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "updated" {
		t.Fatalf("checkpoint value: %q", value)
	}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNamedBytewiseComparatorReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "named")
	options := Options{ComparatorName: "formance.test.bytewise"}
	keys := [][]byte{{0xff}, {0x00, 0xff}, {0x00}, {}}

	// Seed with the former Go callback comparator to prove the existing
	// comparator name and key order remain readable without a migration.
	oldOptions := grocksdb.NewDefaultOptions()
	oldOptions.SetCreateIfMissing(true)
	oldOptions.SetComparator(grocksdb.NewComparator(options.ComparatorName, bytes.Compare))
	seed, err := grocksdb.OpenDb(oldOptions, path)
	if err != nil {
		t.Fatal(err)
	}
	writeOptions := grocksdb.NewDefaultWriteOptions()
	for _, key := range keys {
		if err := seed.Put(writeOptions, key, []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	seed.Close()
	writeOptions.Destroy()
	oldOptions.Destroy()

	for attempt := range 5 {
		db, err := Open(path, options)
		if err != nil {
			t.Fatalf("open %d: %v", attempt, err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}

	db, err := Open(path, Options{ReadOnly: true, ComparatorName: options.ComparatorName})
	if err != nil {
		t.Fatal(err)
	}
	iter, err := db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]byte{{}, {0x00}, {0x00, 0xff}, {0xff}}
	i := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		if i >= len(want) || !bytes.Equal(iter.Key(), want[i]) {
			t.Fatalf("key %d: got %x", i, iter.Key())
		}
		i++
	}
	if i != len(want) {
		t.Fatalf("got %d keys, want %d", i, len(want))
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path, Options{ReadOnly: true, ComparatorName: "formance.test.other"}); err == nil {
		t.Fatal("opening with a different comparator name succeeded")
	}
}
