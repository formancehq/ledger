package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
	"hegel.dev/go/hegel"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

func TestGeneratedPostings(t *testing.T) {
	t.Parallel()
	hegel.Test(t, func(ht *hegel.T) {
		src := generation.NewSource(ht)
		postings := GeneratedPostings(src)
		if len(postings) < 1 || len(postings) > 2 {
			ht.Fatalf("posting count %d outside original domain", len(postings))
		}
		for _, posting := range postings {
			if posting.GetAmount().ToBigInt().BitLen() > 64 {
				ht.Fatalf("amount exceeds uint64 domain")
			}
			switch posting.GetAsset() {
			case "USD/2", "EUR/2", "COIN":
			default:
				ht.Fatalf("unexpected asset %q", posting.GetAsset())
			}
		}
	}, hegel.WithTestCases(100), hegel.WithDerandomize(true), hegel.WithDatabase(""))
}

// The cloud episode must retain the existing 5000-request workload capacity.
// A native draw budget that stops halfway through must not silently reduce it.
func TestGeneratedMaximumBulk(t *testing.T) {
	t.Parallel()
	completed := 0
	err := generation.RunOnce(func(src *generation.Source) {
		for range 5000 {
			_ = GeneratedPostings(src)
			_ = GeneratedMetadata(src)
			completed++
		}
	})
	require.NoError(t, err)
	require.Equal(t, 5000, completed)
}

func BenchmarkGeneratedPostingsEpisode(b *testing.B) {
	for b.Loop() {
		if err := generation.RunOnce(func(src *generation.Source) {
			_ = GeneratedPostings(src)
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLegacyRandomPostings(b *testing.B) {
	for b.Loop() {
		_ = RandomPostings()
	}
}
