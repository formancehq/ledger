package readstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The pruning rule: below the watermark, only a group's latest event may
// survive, and only as an ADD. Resolution at any pin >= watermark must be
// identical before and after.
func TestGCEventZone(t *testing.T) {
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

	pruned, next, err := GCEventZone(s.DB(), PrefixMetadataIndex, nil, watermark, 1<<20)
	require.NoError(t, err)
	require.Equal(t, 4, pruned, "dead's pair + live's superseded pair")
	require.Nil(t, next, "single pass covers the whole zone")

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

// A budget-bounded walk resumes at a group boundary and converges to the
// same end state as one unbounded pass.
func TestGCEventZone_BudgetedResume(t *testing.T) {
	t.Parallel()

	s, prefix := eventFixture(t, "v",
		ev{"a", 10, MetadataEventAdd},
		ev{"a", 20, MetadataEventDel},
		ev{"b", 10, MetadataEventAdd},
		ev{"b", 20, MetadataEventDel},
		ev{"c", 10, MetadataEventAdd},
	)

	const watermark = 30

	totalPruned := 0

	var resume []byte

	rounds := 0
	for {
		rounds++
		require.Less(t, rounds, 10, "walk must terminate")

		pruned, next, err := GCEventZone(s.DB(), PrefixMetadataIndex, resume, watermark, 2)
		require.NoError(t, err)

		totalPruned += pruned
		if next == nil {
			break
		}

		resume = next
	}

	require.Greater(t, rounds, 1, "budget of 2 keys must split the walk")
	require.Equal(t, 4, totalPruned, "a's pair + b's pair reclaimed, c's ADD kept")
	require.Equal(t, []string{"c"}, collect(t, s, prefix, 35))
}
