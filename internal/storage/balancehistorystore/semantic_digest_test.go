package balancehistorystore

import (
	"context"
	"math/big"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/domain/balancehistory"
	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistoryarchive"
)

func TestSemanticDigestIgnoresPublicationBatchingAndCompaction(t *testing.T) {
	t.Parallel()

	separate := newTestStore(t)
	combined := newTestStore(t)

	publications := [][]balancehistory.Effect{
		{
			inputEffect(1, 1, 20, 100, 7, "assets:cash", 2),
			outputEffect(1, 1, 20, 100, 7, "world", 2),
		},
		{
			inputEffect(2, 2, 20, 200, 7, "assets:cash", 3),
			outputEffect(2, 2, 20, 200, 7, "world", 3),
		},
		{
			inputEffect(3, 3, 10, 300, 7, "assets:cash", 5),
			outputEffect(3, 3, 10, 300, 7, "world", 5),
		},
		{
			inputEffect(4, 4, 40, 400, 8, "assets:other", 7),
			outputEffect(4, 4, 40, 400, 8, "world", 7),
		},
	}

	all := make([]balancehistory.Effect, 0, 8)
	for index, effects := range publications {
		sequence := uint64(index + 1)
		_, err := separate.Publish(Publication{
			Effects: effects,
			Coverage: Coverage{
				AuditSequence:  sequence,
				LogSequence:    sequence,
				AuditHash:      []byte{byte(sequence)},
				SourceComplete: true,
			},
		})
		require.NoError(t, err)
		// Deliberately reverse the logical ingestion order inside the combined
		// publication; buildRunRecords must normalize it independently.
		all = append(effects, all...)
	}
	_, err := combined.Publish(Publication{
		Effects: all,
		Coverage: Coverage{
			AuditSequence:  4,
			LogSequence:    4,
			AuditHash:      []byte{4},
			SourceComplete: true,
		},
	})
	require.NoError(t, err)

	separateView, err := separate.OpenView(4)
	require.NoError(t, err)
	separateDigest, err := separateView.SemanticDigest(context.Background())
	require.NoError(t, err)
	require.NoError(t, separateView.Close())

	combinedView, err := combined.OpenView(4)
	require.NoError(t, err)
	combinedDigest, err := combinedView.SemanticDigest(context.Background())
	require.NoError(t, err)
	require.NoError(t, combinedView.Close())
	require.Equal(t, combinedDigest, separateDigest)

	compacted, err := separate.Compact(4)
	require.NoError(t, err)
	require.True(t, compacted)
	compactedView, err := separate.OpenView(4)
	require.NoError(t, err)
	compactedDigest, err := compactedView.SemanticDigest(context.Background())
	require.NoError(t, err)
	require.NoError(t, compactedView.Close())
	require.Equal(t, separateDigest, compactedDigest)
}

func TestSemanticDigestDetectsValidPhysicalValueAlteration(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	manifest := publishBalanced(t, store, 1, 1, 10, 100, 5)

	view, err := store.OpenView(1)
	require.NoError(t, err)
	want, err := view.SemanticDigest(context.Background())
	require.NoError(t, err)
	before, err := view.ReadVolumes(7, AxisEffective, 10, []string{"assets:cash"})
	require.NoError(t, err)
	require.Equal(t, "5", before[0].Input.String())
	require.NoError(t, view.Close())

	identity := recordIdentity{
		Axis:           AxisEffective,
		Scope:          scopeVolume,
		LedgerID:       7,
		Account:        "assets:cash",
		AssetBase:      "USD",
		AssetPrecision: 2,
	}
	key, err := dataKey(manifest.Runs[0].ID, identity, 10)
	require.NoError(t, err)
	encoded, closer, err := store.db.Get(key)
	require.NoError(t, err)
	value, err := decodeCumulative(append([]byte(nil), encoded...))
	require.NoError(t, err)
	require.NoError(t, closer.Close())
	value.input.Add(value.input, big.NewInt(1))
	require.NoError(t, store.db.Set(key, encodeCumulative(value), pebble.Sync))

	tamperedView, err := store.OpenView(1)
	require.NoError(t, err)
	got, err := tamperedView.SemanticDigest(context.Background())
	require.NoError(t, err)
	after, err := tamperedView.ReadVolumes(7, AxisEffective, 10, []string{"assets:cash"})
	require.NoError(t, err)
	require.Equal(t, "6", after[0].Input.String())
	require.NoError(t, tamperedView.Close())
	require.NotEqual(t, want, got)
}

func TestSemanticDigestReadsOnlyMultipartColdDataRuns(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := newTestStore(t)
	for sequence := uint64(1); sequence <= 2; sequence++ {
		publishBalanced(t, store, sequence, sequence, sequence*10, sequence*100, sequence)
	}

	hotView, err := store.OpenView(2)
	require.NoError(t, err)
	hotDigest, err := hotView.SemanticDigest(context.Background())
	require.NoError(t, err)
	require.NoError(t, hotView.Close())

	archive, err := balancehistoryarchive.New(
		coldstorage.NewFilesystemStorage(filepath.Join(root, "cold")),
		balancehistoryarchive.Config{
			BaseBucketID:  "semantic-digest",
			OwnerID:       "node-1",
			CacheDir:      filepath.Join(root, "cache"),
			CacheMaxBytes: 16 << 20,
		},
		noop.NewMeterProvider().Meter("semantic-digest-test"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, archive.Close()) })
	require.NoError(t, store.ConfigureTiering(TieringConfig{
		Archive:         archive,
		MaxSegmentBytes: 4 << 10,
	}))

	tiered, err := store.Tier(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, tiered)
	manifest, err := store.Manifest()
	require.NoError(t, err)
	for _, run := range manifest.Runs {
		require.True(t, run.LocalRemoved)
		require.True(t, run.Archived)
		require.Greater(t, len(run.ArchiveParts), 1)
	}

	coldView, err := store.OpenView(2)
	require.NoError(t, err)
	for _, cold := range coldView.coldRuns {
		cold.mu.Lock()
		for _, part := range cold.parts {
			require.Nil(t, part.lease)
			require.Nil(t, part.reader)
		}
		cold.mu.Unlock()
	}
	coldDigest, err := coldView.SemanticDigest(context.Background())
	require.NoError(t, err)
	require.Equal(t, hotDigest, coldDigest)

	loadedDataParts := 0
	untouchedCatalogParts := 0
	for _, run := range manifest.Runs {
		cold := coldView.coldRuns[run.ID]
		require.NotNil(t, cold)
		dataPrefix := runPrefix(prefixRunData, run.ID)
		dataUpper := prefixEnd(dataPrefix)
		cold.mu.Lock()
		for _, part := range cold.parts {
			if partIntersects(part.meta, dataPrefix, dataUpper) {
				require.NotNil(t, part.lease)
				require.NotNil(t, part.reader)
				loadedDataParts++

				continue
			}
			require.Nil(t, part.lease)
			require.Nil(t, part.reader)
			untouchedCatalogParts++
		}
		cold.mu.Unlock()
	}
	require.Positive(t, loadedDataParts)
	require.Positive(t, untouchedCatalogParts)
	require.NoError(t, coldView.Close())
}
