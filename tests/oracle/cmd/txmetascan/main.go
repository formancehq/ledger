// Command txmetascan dumps, for one (ledger, metadata key, transaction id),
// the readstore's complete view: index version state, every midx event row at
// every version, every eidx event row, and the per-version rmap rows — the
// transactions twin of phantomscan.
package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"

	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintln(os.Stderr, "usage: txmetascan <read-index-dir> <ledger> <metaKey> <txID>")
		os.Exit(1)
	}

	dir, ledger, metaKey := os.Args[1], os.Args[2], os.Args[3]
	txID, err := strconv.ParseUint(os.Args[4], 10, 64)
	if err != nil {
		panic(err)
	}

	entityWant := make([]byte, 8)
	binary.BigEndian.PutUint64(entityWant, txID)

	rs, err := readstore.New(dir, logging.NopZap(), readstore.DefaultConfig())
	if err != nil {
		panic(err)
	}
	defer rs.Close()

	canonical := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, metaKey))
	st, ok, err := readstore.ReadIndexVersionStateFrom(rs.DB(), ledger, canonical)
	fmt.Printf("versionState ok=%v err=%v cur=%d pend=%d act=%d hw=%d curType=%d/%v pendType=%d/%v\n",
		ok, err, st.CurrentVersion, st.PendingVersion, st.ActivationSequence, st.HighWater,
		st.CurrentType, st.CurrentTypeDeclared, st.PendingType, st.PendingTypeDeclared)

	kb := dal.NewKeyBuilder()
	suffix := 1 + 8 + 1 // terminator + seq + op

	for v := uint32(1); v <= max(st.CurrentVersion, st.PendingVersion, st.HighWater)+2; v++ {
		prefix := append([]byte(nil), readstore.MetadataIndexPrefixV(kb, ledger, readstore.NamespaceTransaction, metaKey, v)...)
		it, err := rs.DB().NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: readstore.IncrementBytes(prefix)})
		if err != nil {
			panic(err)
		}

		for it.First(); it.Valid(); it.Next() {
			k := it.Key()
			if len(k) < len(prefix)+suffix {
				continue
			}
			tail := k[len(k)-9:]
			seq := uint64(0)
			for _, b := range tail[:8] {
				seq = seq<<8 | uint64(b)
			}
			op := tail[8]
			val, consumed, derr := readstore.DecodeValue(k[len(prefix):])
			if derr != nil {
				continue
			}
			entity := k[len(prefix)+consumed : len(k)-suffix+1]
			if string(entity) != string(entityWant) {
				continue
			}
			fmt.Printf("midx v%d value=%v seq=%d op=%d\n", v, val, seq, op)
		}
		_ = it.Close()

		for _, null := range []bool{false, true} {
			var eprefix []byte
			if null {
				eprefix = append([]byte(nil), readstore.EntityExistsNullPrefixV(kb, ledger, readstore.NamespaceTransaction, metaKey, v)...)
			} else {
				eprefix = append([]byte(nil), readstore.EntityExistsNonNullPrefixV(kb, ledger, readstore.NamespaceTransaction, metaKey, v)...)
			}
			it, err := rs.DB().NewIter(&pebble.IterOptions{LowerBound: eprefix, UpperBound: readstore.IncrementBytes(eprefix)})
			if err != nil {
				panic(err)
			}
			for it.First(); it.Valid(); it.Next() {
				k := it.Key()
				if len(k) < len(eprefix)+suffix {
					continue
				}
				entity := k[len(eprefix) : len(k)-suffix+1]
				if string(entity) != string(entityWant) {
					continue
				}
				tail := k[len(k)-9:]
				seq := uint64(0)
				for _, b := range tail[:8] {
					seq = seq<<8 | uint64(b)
				}
				fmt.Printf("eidx v%d null=%v seq=%d op=%d\n", v, null, seq, tail[8])
			}
			_ = it.Close()
		}

		rk := readstore.TransactionReverseMapKeyV(kb, ledger, txID, metaKey, v)
		if val, closer, err := rs.DB().Get(rk); err == nil {
			dv, _, derr := readstore.DecodeValue(val)
			fmt.Printf("rmap v%d value=%v (decodeErr=%v raw=%x)\n", v, dv, derr, val)
			_ = closer.Close()
		}
	}
}
