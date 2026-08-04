// Command pebblescan dumps primary-store attribute rows (ZoneAttributes 0x01)
// for one account address straight from a Pebble data dir, read-only. Used to
// verify what the server actually persisted for an account a model finding
// flagged — independent of the server's cache layer.
//
// Usage: pebblescan <data-dir> <ledger> <address>
package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

const (
	zoneAttributes      = 0x01
	subAttrVolume       = 0x01
	subAttrMetadata     = 0x02
	ledgerNameFixedSize = 64
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: pebblescan <data-dir> <ledger> <address>")
		os.Exit(2)
	}
	dir, ledger, addr := os.Args[1], os.Args[2], os.Args[3]

	db, err := pebble.Open(dir, &pebble.Options{ReadOnly: true})
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	for _, sub := range []struct {
		name string
		b    byte
	}{{"volume", subAttrVolume}, {"metadata", subAttrMetadata}} {
		prefix := make([]byte, 2+ledgerNameFixedSize)
		prefix[0] = zoneAttributes
		prefix[1] = sub.b
		copy(prefix[2:], ledger)

		lower := append(append([]byte{}, prefix...), addr...)
		upper := append(append([]byte{}, lower...), 0xff)

		iter, err := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		if err != nil {
			fmt.Fprintln(os.Stderr, "iter:", err)
			os.Exit(1)
		}

		n := 0
		for iter.First(); iter.Valid(); iter.Next() {
			n++
			suffix := iter.Key()[len(prefix):]
			fmt.Printf("%s key-suffix=%q\n", sub.name, suffix)
			if sub.b == subAttrVolume {
				vp := &raftcmdpb.VolumePair{}
				if err := vp.UnmarshalVT(iter.Value()); err != nil {
					fmt.Printf("  value (raw %d bytes): %s\n", len(iter.Value()), hexdump(iter.Value()))
				} else {
					fmt.Printf("  input=%v output=%v\n", vp.GetInput(), vp.GetOutput())
				}
			} else {
				fmt.Printf("  value (%d bytes): %s\n", len(iter.Value()), hexdump(iter.Value()))
			}
		}
		_ = iter.Close()
		fmt.Printf("%s rows for %q: %d\n", sub.name, addr, n)
	}

	_ = bytes.MinRead // keep bytes imported if unused paths change
}

func hexdump(b []byte) string {
	if len(b) > 96 {
		b = b[:96]
	}
	return fmt.Sprintf("% x", b)
}
