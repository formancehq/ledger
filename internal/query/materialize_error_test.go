package query

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// A range leaf reports a failure in band — Next returns false for an error
// exactly as it does for exhaustion — and materialising it into a slice
// discards the distinction: SliceIterator.Err is nil by construction, so
// every later check reads a truncated range as a complete answer. The drain
// must therefore refuse to produce an iterator at all.
func TestMaterializeIterator_PropagatesADrainFailure(t *testing.T) {
	t.Parallel()

	rs, err := readstore.New(t.TempDir(), logging.NopZap(), readstore.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = rs.Close() })

	const (
		ledger  = "l"
		metaKey = "amount"
		version = uint32(1)
	)

	kb := dal.NewKeyBuilder()
	prefix := append([]byte(nil), readstore.MetadataIndexPrefixV(kb, ledger, readstore.NamespaceAccount, metaKey, version)...)

	// Two readable members of the range, and one event carrying an op byte
	// this package never writes.
	encoded := func(v int64) []byte {
		return readstore.EncodeInt64(nil, v)
	}

	for _, e := range []struct {
		entity string
		value  int64
		seq    uint64
		op     byte
	}{
		{"a", 5, 10, readstore.MetadataEventAdd},
		{"b", 10, 10, 0x7f},
		{"c", 15, 10, readstore.MetadataEventAdd},
	} {
		key := readstore.MetadataIndexEventKeyV(kb, ledger, readstore.NamespaceAccount, metaKey, version,
			encoded(e.value), []byte(e.entity), e.seq, e.op)
		require.NoError(t, rs.DB().Set(append([]byte(nil), key...), nil, pebble.NoSync))
	}

	lower := append(append([]byte(nil), prefix...), readstore.TypeTagInt)
	upper := append(append([]byte(nil), prefix...), readstore.TypeTagInt+1)

	iter, err := readstore.NewEventResolveRangeIterator(rs.DB(), lower, upper, len(prefix), 1+8, 50)
	require.NoError(t, err)

	matIter, err := materializeIterator(iter, nil, nil)
	require.Error(t, err, "a drain that failed must not be presented as a complete range")
	require.Nil(t, matIter, "a SliceIterator cannot carry the failure, so none may be returned")
}
