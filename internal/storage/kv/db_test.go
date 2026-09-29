package kv

import (
	"errors"
	"path/filepath"
	"testing"
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
