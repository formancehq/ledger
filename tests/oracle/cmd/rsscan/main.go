// Command rsscan scans a read-index store for the account→tx mapping rows of
// given transaction ids, printing each (role prefix, account, txID) hit — a
// diagnostic for address-on-transactions index findings.
//
// Usage: rsscan <read-index-dir> <ledger> <txID>...
package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "usage: rsscan <read-index-dir> <ledger> <txID>...")
		os.Exit(2)
	}

	dir, ledger := os.Args[1], os.Args[2]
	want := map[uint64]bool{}
	for _, a := range os.Args[3:] {
		id, err := strconv.ParseUint(a, 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad txID %q: %v\n", a, err)
			os.Exit(2)
		}
		want[id] = true
	}

	db, err := pebble.Open(dir, &pebble.Options{ReadOnly: true, Comparer: readstore.ReadStoreComparer})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	countRows(db, ledger)

	for _, prefix := range []byte{readstore.PrefixAccountTx, readstore.PrefixSourceAccountTx, readstore.PrefixDestinationAccountTx} {
		lo := append([]byte{prefix}, make([]byte, 64)...)
		copy(lo[1:], ledger)
		up := []byte{prefix + 1}

		it, err := db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: up})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		for it.First(); it.Valid(); it.Next() {
			k := it.Key()
			// [prefix][ledger 64B][account\x00][txID 8B]
			if len(k) < 1+64+1+8 || string(k[1:1+len(ledger)]) != ledger {
				continue
			}

			txID := binary.BigEndian.Uint64(k[len(k)-8:])
			if !want[txID] {
				continue
			}

			fmt.Printf("prefix=0x%02x acct=%q tx=%d\n", prefix, k[1+64:len(k)-9], txID)
		}
		_ = it.Close()
	}
}
