package internal

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOwnedLedgerPrefixes_NoOverlap pins the invariant that no
// driver-owned prefix is a prefix of another. The hyphen-aware match in
// GetRandomLedger uses `<token>-`, so a constant like "lrec" silently
// fails to match "lrecreate-N" (char 5 is 'r', not '-'), letting the
// driver-owned ledger leak into the generic random pool. This test
// catches the class at test time.
func TestOwnedLedgerPrefixes_NoOverlap(t *testing.T) {
	t.Parallel()

	for i, a := range ownedLedgerPrefixes {
		for j, b := range ownedLedgerPrefixes {
			if i == j {
				continue
			}

			require.False(t, strings.HasPrefix(string(a), string(b)+"-"),
				"prefix %q is a prefix of %q — random-ledger filter would catch one with the other",
				b, a)
			require.NotEqual(t, a, b, "duplicate prefix %q in ownedLedgerPrefixes (indices %d and %d)", a, i, j)
		}
	}
}

// TestRestrictedPrefixes_HaveTrailingHyphen pins the contract that every
// prefix returned by RestrictedPrefixes() ends with a hyphen. Without
// the hyphen, "lrec" would match "lrecreate-…" (substring) — but with
// the hyphen, the match is exact-token + dash, which is what every
// driver naming convention uses.
func TestRestrictedPrefixes_HaveTrailingHyphen(t *testing.T) {
	t.Parallel()

	for _, p := range RestrictedPrefixes() {
		require.True(t, strings.HasSuffix(p, "-"),
			"restricted prefix %q must end with a hyphen (otherwise sub-token matches leak)", p)
		require.False(t, strings.HasSuffix(p, "--"),
			"restricted prefix %q has a duplicate hyphen — the constant already includes one", p)
	}
}

// TestOwnedLedgerPrefix_New pins the format the chaos-workload assumes:
// `<prefix>-<6-digit-number>`. Drivers and the audit team rely on this
// shape to parse ledger names from logs.
func TestOwnedLedgerPrefix_New(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		prefix OwnedLedgerPrefix
	}{
		{"insuf", PrefixInsufficientFunds},
		{"lrecreate", PrefixLedgerRecreate},
		{"sentinel", PrefixSentinel},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ledger := tt.prefix.WithSeed(123456)
			require.Equal(t, string(tt.prefix)+"-123456", ledger)

			// New() draws a random suffix, so just shape-check.
			n := tt.prefix.New()
			require.True(t, strings.HasPrefix(n, string(tt.prefix)+"-"),
				"New() must produce <prefix>-<suffix>, got %q", n)
		})
	}
}

// TestOwnedLedgerPrefix_WithSuffix pins the manual-suffix path used by
// the operational singletons (sentinel-scaling-structured etc.).
func TestOwnedLedgerPrefix_WithSuffix(t *testing.T) {
	t.Parallel()

	require.Equal(t, "sentinel-scaling-structured",
		PrefixSentinel.WithSuffix("scaling-structured"))
}
