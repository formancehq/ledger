// Command fctl-plugin-audit verifies the fctl Ledger plugin inventories and manifests.
//
// Output discipline: the audit report is the only thing on stdout, as one JSON
// document, so it composes with jq and shell redirection. Progress and failure
// diagnostics go to stderr.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/formancehq/ledger/plugins/fctl/internal/audit"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fctl-plugin-audit:", err)
		os.Exit(1)
	}
}

func run() error {
	root := flag.String("root", ".", "path to plugins/fctl")
	write := flag.Bool("write", false, "rewrite the JSON documents in canonical form instead of verifying")
	flag.Parse()

	docs := []string{
		filepath.Join(*root, "ledger-v2", "inventory.json"),
		filepath.Join(*root, "ledger-v2", "manifest.json"),
		filepath.Join(*root, "ledger-v3", "inventory.json"),
		filepath.Join(*root, "ledger-v3", "manifest.json"),
	}

	if *write {
		return canonicalizeAll(docs)
	}

	in, err := load(*root)
	if err != nil {
		return err
	}

	report := audit.Run(in)

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	if err := enc.Encode(report); err != nil {
		return fmt.Errorf("encode report: %w", err)
	}

	if !report.OK() {
		fmt.Fprintf(os.Stderr, "%d of %d checks failed\n", report.Errors, report.Checks)
		os.Exit(2)
	}

	fmt.Fprintf(os.Stderr, "all %d checks passed\n", report.Checks)

	return nil
}

func load(root string) (audit.Inputs, error) {
	var in audit.Inputs
	var err error

	if in.V2Inventory, in.V2InvRaw, err = audit.LoadInventory(
		filepath.Join(root, "ledger-v2", "inventory.json")); err != nil {
		return in, err
	}

	if in.V2Manifest, in.V2ManRaw, err = audit.LoadManifest(
		filepath.Join(root, "ledger-v2", "manifest.json")); err != nil {
		return in, err
	}

	if in.V3Inventory, in.V3InvRaw, err = audit.LoadInventory(
		filepath.Join(root, "ledger-v3", "inventory.json")); err != nil {
		return in, err
	}

	if in.V3Manifest, in.V3ManRaw, err = audit.LoadManifest(
		filepath.Join(root, "ledger-v3", "manifest.json")); err != nil {
		return in, err
	}

	return in, nil
}

func canonicalizeAll(docs []string) error {
	for _, p := range docs {
		raw, err := os.ReadFile(filepath.Clean(p))
		if err != nil {
			return fmt.Errorf("read %s: %w", p, err)
		}

		ok, want, err := audit.IsCanonical(raw)
		if err != nil {
			return fmt.Errorf("canonicalize %s: %w", p, err)
		}

		if ok {
			continue
		}

		if err := os.WriteFile(p, want, 0o600); err != nil {
			return fmt.Errorf("write %s: %w", p, err)
		}

		fmt.Fprintf(os.Stderr, "canonicalized %s\n", p)
	}

	return nil
}
