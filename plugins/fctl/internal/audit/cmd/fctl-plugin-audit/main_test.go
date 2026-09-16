package main

import (
	"os"
	"path/filepath"
	"testing"
)

const preparationRoot = "../../../.."

func TestLoadReadsTheCommittedPreparation(t *testing.T) {
	in, err := load(preparationRoot)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if in.V2Inventory.Plugin != "ledger-v2" {
		t.Errorf("v2 plugin = %q", in.V2Inventory.Plugin)
	}

	if in.V3Inventory.Plugin != "ledger-v3" {
		t.Errorf("v3 plugin = %q", in.V3Inventory.Plugin)
	}

	for name, raw := range map[string][]byte{
		"v2 inventory": in.V2InvRaw,
		"v2 manifest":  in.V2ManRaw,
		"v3 inventory": in.V3InvRaw,
		"v3 manifest":  in.V3ManRaw,
	} {
		if len(raw) == 0 {
			t.Errorf("%s raw bytes are empty", name)
		}
	}
}

func TestLoadReportsAMissingDocument(t *testing.T) {
	if _, err := load(t.TempDir()); err == nil {
		t.Fatal("expected an error for a root with no documents")
	}
}

func TestCanonicalizeAllRewritesOnlyNonCanonicalFiles(t *testing.T) {
	dir := t.TempDir()

	messy := filepath.Join(dir, "messy.json")
	if err := os.WriteFile(messy, []byte(`{"b":1,"a":2}`), 0o600); err != nil {
		t.Fatal(err)
	}

	tidy := filepath.Join(dir, "tidy.json")
	tidyWant := "{\n  \"a\": 2,\n  \"b\": 1\n}\n"

	if err := os.WriteFile(tidy, []byte(tidyWant), 0o600); err != nil {
		t.Fatal(err)
	}

	tidyBefore, err := os.Stat(tidy)
	if err != nil {
		t.Fatal(err)
	}

	if err := canonicalizeAll([]string{messy, tidy}); err != nil {
		t.Fatalf("canonicalizeAll: %v", err)
	}

	got, err := os.ReadFile(filepath.Clean(messy))
	if err != nil {
		t.Fatal(err)
	}

	if string(got) != tidyWant {
		t.Errorf("messy.json = %q, want %q", got, tidyWant)
	}

	tidyAfter, err := os.Stat(tidy)
	if err != nil {
		t.Fatal(err)
	}

	if !tidyAfter.ModTime().Equal(tidyBefore.ModTime()) {
		t.Error("an already-canonical file was rewritten; the recipe is not idempotent")
	}
}

func TestCanonicalizeAllRejectsInvalidJSON(t *testing.T) {
	dir := t.TempDir()

	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte(`{not json`), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := canonicalizeAll([]string{bad}); err == nil {
		t.Fatal("expected an error for invalid JSON")
	}
}
