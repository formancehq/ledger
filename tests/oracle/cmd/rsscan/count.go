package main

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

// countRows prints per-prefix row counts and a txID histogram sample for one
// ledger's account→tx mappings.
func countRows(db *pebble.DB, ledger string) {
	for _, prefix := range []byte{0x04, 0x05, 0x06} {
		lo := append([]byte{prefix}, make([]byte, 64)...)
		copy(lo[1:], ledger)
		up := []byte{prefix + 1}

		it, err := db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: up})
		if err != nil {
			panic(err)
		}

		n, minTx, maxTx := 0, uint64(0), uint64(0)
		for it.First(); it.Valid(); it.Next() {
			k := it.Key()
			if len(k) < 1+64+1+8 || string(k[1:1+len(ledger)]) != ledger {
				continue
			}
			id := binary.BigEndian.Uint64(k[len(k)-8:])
			if n == 0 || id < minTx {
				minTx = id
			}
			if id > maxTx {
				maxTx = id
			}
			n++
		}
		_ = it.Close()
		fmt.Printf("prefix=0x%02x rows=%d txRange=[%d,%d]\n", prefix, n, minTx, maxTx)
	}
}
