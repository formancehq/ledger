// Command assetscan dumps the account-by-asset index rows of one ledger from a
// read-index store — a diagnostic for has-asset findings.
//
// Usage: assetscan <read-index-dir> <ledger> [assetBase]
package main

import (
	"fmt"
	"os"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: assetscan <read-index-dir> <ledger> [assetBase]")
		os.Exit(2)
	}

	dir, ledger := os.Args[1], os.Args[2]
	wantAsset := ""
	if len(os.Args) > 3 {
		wantAsset = os.Args[3]
	}

	db, err := pebble.Open(dir, &pebble.Options{ReadOnly: true, Comparer: readstore.ReadStoreComparer})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer db.Close()

	lo := make([]byte, 1+dal.LedgerNameFixedSize)
	lo[0] = readstore.PrefixAccountByAsset
	copy(lo[1:], ledger)
	up := readstore.IncrementBytes(lo)

	it, err := db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: up})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer it.Close()

	n := 0
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		rest := k[1+dal.LedgerNameFixedSize:]
		// [assetBase\x00][precision 1B][account]
		var asset, account string
		var prec byte
		for i := 0; i < len(rest); i++ {
			if rest[i] == 0 {
				asset = string(rest[:i])
				if i+1 < len(rest) {
					prec = rest[i+1]
					account = string(rest[i+2:])
				}

				break
			}
		}

		if wantAsset != "" && asset != wantAsset {
			continue
		}

		n++
		fmt.Printf("%-8s p%-3d %q\n", asset, prec, account)
	}

	fmt.Printf("total rows: %d\n", n)
}
