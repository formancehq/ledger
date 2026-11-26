package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestSortNumscriptVersions pins the deterministic order: stored semver versions
// highest-first.
func TestSortNumscriptVersions(t *testing.T) {
	t.Parallel()

	entries := []*commonpb.NumscriptVersionEntry{
		{Version: "1.0.2"},
		{Version: "1.0.10"},
		{Version: "2.0.0"},
		{Version: "1.0.0"},
	}

	sortNumscriptVersions(entries)

	got := make([]string, len(entries))
	for i, e := range entries {
		got[i] = e.GetVersion()
	}

	require.Equal(t, []string{"2.0.0", "1.0.10", "1.0.2", "1.0.0"}, got)
}
