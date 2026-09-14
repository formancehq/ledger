package query_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// Descending pagination cost (EN-1966).
//
// The A/B runs in ONE binary against ONE seeded store: "drain" reproduces the
// algorithm listDescFiltered used to run (compile ascending, drain every
// match, reverse, then apply the cursor and page size) and "stream" is the
// compiled descending tree. Same data, same process, so the difference is the
// algorithm and not machine noise or a cross-branch build.
//
// Two metrics are reported separately, because they answer different
// questions and conflating them is how a ring-buffer-shaped change gets
// mistaken for a traversal fix:
//
//	rows_visited/op — raw event work: entities the tree actually walked.
//	rows_out/op     — output collection: entities the page returned.
//
// The ticket is satisfied only when rows_visited stops tracking the match
// count for streaming shapes. A change that only lowers allocations while
// rows_visited stays at O(M) has not done the job.

const benchPageSize = 100

// benchStore seeds n accounts carrying a streaming-friendly equality leaf.
func benchStore(b *testing.B, n int) *readstore.Store {
	b.Helper()

	logger := logging.FromContext(logging.TestingContext())

	store, err := readstore.New(b.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(b, err)

	b.Cleanup(func() { _ = store.Close() })

	kb := dal.NewKeyBuilder()
	batch := store.NewBatch()

	for i := range n {
		account := fmt.Appendf(nil, "accounts:%09d", i)

		require.NoError(b, batch.SetBytes(readstore.MetadataIndexEventKeyV(
			kb, parityLedger, readstore.NamespaceAccount, "colour", 1,
			readstore.EncodeString(nil, "red"), account,
			// Every row folds at seq 1, well below parityPin: the whole
			// dataset must be visible, or the benchmark silently measures
			// the pin gate instead of the traversal.
			1, readstore.MetadataEventAdd), nil))
	}

	require.NoError(b, batch.Commit())

	return store
}

// drainPage is the pre-EN-1966 algorithm, kept here as the benchmark baseline.
func drainPage(b *testing.B, store *readstore.Store, after []byte) (rowsVisited, rowsOut int) {
	b.Helper()

	reader := store.DB()

	iter, err := query.Compile(
		reader, dal.NewKeyBuilder(), stringFieldFilter("colour", "red"),
		commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, parityLedger,
		nil, paritySchema(), parityInfo(), parityRegistry(), parityResolver(), nil, reader, parityPin)
	require.NoError(b, err)

	defer iter.Close()

	var all [][]byte

	for iter.Next() {
		cp := make([]byte, len(iter.Current()))
		copy(cp, iter.Current())
		all = append(all, cp)
	}

	require.NoError(b, iter.Err())

	rowsVisited = len(all)

	for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
		all[i], all[j] = all[j], all[i]
	}

	if after != nil {
		skip := 0

		for _, id := range all {
			if bytes.Compare(id, after) >= 0 {
				skip++
			} else {
				break
			}
		}

		all = all[skip:]
	}

	if len(all) > benchPageSize {
		all = all[:benchPageSize]
	}

	return rowsVisited, len(all)
}

// streamPage is the compiled descending path.
func streamPage(b *testing.B, store *readstore.Store, before []byte) (rowsVisited, rowsOut int) {
	b.Helper()

	reader := store.DB()
	profile := &query.QueryProfile{}

	iter, err := query.CompileReverse(
		reader, dal.NewKeyBuilder(), stringFieldFilter("colour", "red"),
		commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, parityLedger,
		nil, paritySchema(), parityInfo(), parityRegistry(), parityResolver(), profile, reader, parityPin)
	require.NoError(b, err)

	defer iter.Close()

	items, _, pErr := readstore.PaginateReverse(iter, benchPageSize, before)
	require.NoError(b, pErr)

	// The tracked root counts every entity the tree emitted upward, which for
	// a single leaf is exactly the raw event work.
	if profile.Root != nil {
		rowsVisited = int(profile.Root.ItemsEmitted)
	}

	return rowsVisited, len(items)
}

func benchmarkDescending(b *testing.B, n int, late bool) {
	store := benchStore(b, n)

	// The late-page cursor sits one page in from the top, so the "drain"
	// arm cannot look cheap by stopping early.
	var cursor []byte
	if late {
		cursor = fmt.Appendf(nil, "accounts:%09d", n-benchPageSize)
	}

	for _, arm := range []struct {
		name string
		run  func(*testing.B, *readstore.Store, []byte) (int, int)
	}{
		{"drain", drainPage},
		{"stream", streamPage},
	} {
		b.Run(arm.name, func(b *testing.B) {
			b.ReportAllocs()

			var visited, out int

			b.ResetTimer()

			for b.Loop() {
				visited, out = arm.run(b, store, cursor)
			}

			b.StopTimer()
			b.ReportMetric(float64(visited), "rows_visited/op")
			b.ReportMetric(float64(out), "rows_out/op")
		})
	}
}

func BenchmarkDescendingFirstPage(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("matches=%d", n), func(b *testing.B) {
			benchmarkDescending(b, n, false)
		})
	}
}

func BenchmarkDescendingLatePage(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("matches=%d", n), func(b *testing.B) {
			benchmarkDescending(b, n, true)
		})
	}
}
