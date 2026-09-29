package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

func TestMigrationRoundTrip(t *testing.T) {
	for _, kind := range []string{"main", "read", "usage"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source")
			dest := filepath.Join(root, "destination")
			cmp, err := sourceComparer(kind)
			if err != nil {
				t.Fatal(err)
			}
			db, err := pebble.Open(source, &pebble.Options{Comparer: cmp})
			if err != nil {
				t.Fatal(err)
			}
			keys := [][]byte{{0xFE, 1}, append(bytes.Repeat([]byte{'a'}, 65), 0, 1), {0, 0xff, 0}, {0xff, 0}}
			for i, key := range keys {
				valueSize := i*31 + 1
				if i == 3 {
					valueSize = batchLimit + 1
				}
				if err := db.Set(key, bytes.Repeat([]byte{byte(i)}, valueSize), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if kind == "main" {
				if err := os.WriteFile(filepath.Join(source, ".ready"), nil, 0o640); err != nil {
					t.Fatal(err)
				}
			}
			src, err := pebble.Open(source, &pebble.Options{ReadOnly: true, Comparer: cmp})
			if err != nil {
				t.Fatal(err)
			}
			before, err := hashPebble(src)
			if err != nil {
				t.Fatal(err)
			}
			if err := src.Close(); err != nil {
				t.Fatal(err)
			}
			if err := migrate(config{source: source, destination: dest, kind: kind, stopped: true}); err != nil {
				t.Fatal(err)
			}
			got, err := hashRocks(dest, kind)
			if err != nil {
				t.Fatal(err)
			}
			if got != before || got.count != uint64(len(keys)) {
				t.Fatalf("destination digest: %+v; source: %+v", got, before)
			}
			if _, err := os.Stat(dest + ".incomplete"); !os.IsNotExist(err) {
				t.Fatalf("staging should be gone: %v", err)
			}
			if kind == "main" {
				if _, err := os.Stat(filepath.Join(dest, ".ready")); err != nil {
					t.Fatal(err)
				}
			}
			src, err = pebble.Open(source, &pebble.Options{ReadOnly: true, Comparer: cmp})
			if err != nil {
				t.Fatal(err)
			}
			after, err := hashPebble(src)
			if err != nil {
				t.Fatal(err)
			}
			if err := src.Close(); err != nil {
				t.Fatal(err)
			}
			if after != before {
				t.Fatal("source changed")
			}
			if err := migrate(config{source: source, destination: dest, kind: kind, stopped: true}); err == nil {
				t.Fatal("existing destination accepted")
			}
		})
	}
}

func TestEmptyDatabase(t *testing.T) {
	root := t.TempDir()
	source, dest := filepath.Join(root, "source"), filepath.Join(root, "destination")
	db, err := pebble.Open(source, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := migrate(config{source: source, destination: dest, kind: "main", stopped: true}); err != nil {
		t.Fatal(err)
	}
	got, err := hashRocks(dest, "main")
	if err != nil {
		t.Fatal(err)
	}
	if got.count != 0 {
		t.Fatalf("empty database has %d keys", got.count)
	}
}

func TestFailureLeavesClearIncompleteState(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	dest := filepath.Join(root, "destination")
	cmp, err := sourceComparer("read")
	if err != nil {
		t.Fatal(err)
	}
	db, err := pebble.Open(source, &pebble.Options{Comparer: cmp})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("key"), []byte("value"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// A wrong source kind must fail Pebble's persisted-comparer check.
	cfg := config{source: source, destination: dest, kind: "usage", stopped: true}
	if err := migrate(cfg); err == nil {
		t.Fatal("wrong comparer accepted")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("destination should be absent: %v", err)
	}
	if info, err := os.Stat(dest + ".incomplete"); err != nil || !info.IsDir() {
		t.Fatalf("incomplete state missing: %v", err)
	}
	if err := migrate(cfg); err == nil {
		t.Fatal("incomplete state overwritten")
	}
}

func TestRequiresStoppedSourceAndSeparateDestination(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []config{
		{source: source, destination: filepath.Join(root, "dest"), kind: "main"},
		{source: source, destination: filepath.Join(source, "nested"), kind: "main", stopped: true},
		{source: source, destination: source, kind: "main", stopped: true},
	} {
		if err := migrate(cfg); err == nil {
			t.Fatalf("unsafe config accepted: %+v", cfg)
		}
	}
}

func TestExclusivePublishPreservesExistingDestination(t *testing.T) {
	root := t.TempDir()
	stage := filepath.Join(root, "stage")
	dest := filepath.Join(root, "destination")
	if err := os.Mkdir(stage, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dest, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dest, "marker")
	if err := os.WriteFile(marker, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := publishExclusive(stage, dest); err == nil {
		t.Fatal("existing destination replaced")
	}
	if got, err := os.ReadFile(marker); err != nil || string(got) != "keep" {
		t.Fatalf("destination changed: %q, %v", got, err)
	}
	if _, err := os.Stat(stage); err != nil {
		t.Fatalf("staging lost: %v", err)
	}
}
