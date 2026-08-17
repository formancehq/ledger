// Command vstates dumps every per-replica IndexVersionState record in a
// read-index store: ledger, canonical index ID, and the decoded state.
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: vstates <read-index-dir>")
		os.Exit(1)
	}

	rs, err := readstore.New(os.Args[1], logging.NopZap(), readstore.DefaultConfig())
	if err != nil {
		panic(err)
	}
	defer rs.Close()

	if os.Getenv("VSTATES_COUNT") != "" {
		countPrefixes(rs.DB())
	}

	prefix := []byte{readstore.PrefixInternal, readstore.SubInternalIndexVersion}
	it, err := rs.DB().NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: readstore.IncrementBytes(prefix)})
	if err != nil {
		panic(err)
	}
	defer func() { _ = it.Close() }()

	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		if len(k) < 2+64 {
			continue
		}
		ledger := strings.TrimRight(string(k[2:2+64]), "\x00")
		canonical := string(k[2+64:])
		st, ok, err := readstore.ReadIndexVersionStateFrom(rs.DB(), ledger, canonical)
		fmt.Printf("%s %s cur=%d pend=%d act=%d hw=%d curType=%d/%v pendType=%d/%v ok=%v err=%v\n",
			ledger, canonical, st.CurrentVersion, st.PendingVersion, st.ActivationSequence, st.HighWater,
			st.CurrentType, st.CurrentTypeDeclared, st.PendingType, st.PendingTypeDeclared, ok, err)
	}
}
