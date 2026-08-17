package main

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

// countPrefixes prints how many keys live under each top-level prefix byte.
func countPrefixes(db *pebble.DB) {
	it, err := db.NewIter(nil)
	if err != nil {
		panic(err)
	}
	defer func() { _ = it.Close() }()

	counts := map[byte]int{}
	for it.First(); it.Valid(); it.Next() {
		counts[it.Key()[0]]++
	}
	for b, n := range counts {
		fmt.Printf("prefix 0x%02X: %d keys\n", b, n)
	}
}
