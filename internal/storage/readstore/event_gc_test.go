package readstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The pruning rule: below the watermark, only a group's latest event may
// survive, and only as an ADD. Resolution at any pin >= watermark must be
// identical before and after.
func TestGCMetadataEvents(t *testing.T) {
	t.Parallel()

	s, prefix := eventFixture(t, "v",
		ev{"dead", 10, MetadataEventAdd}, // add@10 del@20: whole pair reclaimed
		ev{"dead", 20, MetadataEventDel},
		ev{"live", 10, MetadataEventAdd}, // superseded by re-add
		ev{"live", 20, MetadataEventDel},
		ev{"live", 25, MetadataEventAdd}, // latest below watermark, ADD: survives
		ev{"old", 10, MetadataEventAdd},  // single old ADD: survives
		ev{"span", 10, MetadataEventAdd}, // ADD below + DEL above: both survive
		ev{"span", 40, MetadataEventDel},
	)

	const watermark = 30

	before := map[uint64][]string{}
	for _, pin := range []uint64{30, 35, 45} {
		before[pin] = collect(t, s, prefix, pin)
	}

	pruned, err := GCMetadataEvents(s.DB(), prefix, watermark)
	require.NoError(t, err)
	require.Equal(t, 4, pruned, "dead's pair + live's superseded pair")

	for _, pin := range []uint64{30, 35, 45} {
		require.Equal(t, before[pin], collect(t, s, prefix, pin), "pin %d verdicts unchanged", pin)
	}

	// The survivors are exactly: live@25(ADD), old@10(ADD), span@10(ADD), span@40(DEL).
	remaining := 0
	it, err := s.DB().NewIter(nil)
	require.NoError(t, err)
	defer func() { _ = it.Close() }()
	for it.First(); it.Valid(); it.Next() {
		remaining++
	}
	require.Equal(t, 4, remaining)
}
