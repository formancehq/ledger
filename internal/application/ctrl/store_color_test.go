package ctrl

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// volEntry is a small helper to build an attributes.ComputedEntry against
// a (account, asset, color) tuple with concrete input/output amounts.
func volEntry(t *testing.T, ledgerName string, account, asset, color string, in, out int64) attributes.ComputedEntry[*raftcmdpb.VolumePair] {
	t.Helper()
	vk := domain.NewVolumeKey(ledgerName, account, asset, color)

	return attributes.ComputedEntry[*raftcmdpb.VolumePair]{
		CanonicalKey: vk.Bytes(),
		Value: &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(uint64(in)),
			Output: commonpb.NewUint256FromUint64(uint64(out)),
		},
	}
}

// Default mode (collapseColors=false) keeps every (asset, color) bucket as
// its own entry, sorted by (asset, color) ascending.
func TestAssembleAccount_SegregatesColorsByDefault(t *testing.T) {
	t.Parallel()

	entries := []attributes.ComputedEntry[*raftcmdpb.VolumePair]{
		volEntry(t, "test", "alice", "USD/2", "OPS", 25, 0),
		volEntry(t, "test", "alice", "USD/2", "", 100, 0),
		volEntry(t, "test", "alice", "USD/2", "GRANTS", 50, 0),
		volEntry(t, "test", "alice", "EUR/2", "", 10, 0),
	}

	acct := assembleAccount("alice", entries, nil, false)

	// Order: (EUR/2,""), (USD/2,""), (USD/2,"GRANTS"), (USD/2,"OPS")
	got := acct.GetVolumes()
	require.Len(t, got, 4)

	require.Equal(t, "EUR/2", got[0].GetAsset())
	require.Equal(t, "", got[0].GetColor())

	require.Equal(t, "USD/2", got[1].GetAsset())
	require.Equal(t, "", got[1].GetColor())
	require.Equal(t, "100", got[1].GetVolumes().GetInput())
	require.Equal(t, "100", got[1].GetVolumes().GetBalance())

	require.Equal(t, "USD/2", got[2].GetAsset())
	require.Equal(t, "GRANTS", got[2].GetColor())
	require.Equal(t, "50", got[2].GetVolumes().GetBalance())

	require.Equal(t, "USD/2", got[3].GetAsset())
	require.Equal(t, "OPS", got[3].GetColor())
	require.Equal(t, "25", got[3].GetVolumes().GetBalance())
}

// Collapse mode sums every (asset, *) bucket into a single entry with
// color = "" and amounts summed.
func TestAssembleAccount_CollapseColors(t *testing.T) {
	t.Parallel()

	entries := []attributes.ComputedEntry[*raftcmdpb.VolumePair]{
		volEntry(t, "test", "alice", "USD/2", "", 100, 0),
		volEntry(t, "test", "alice", "USD/2", "GRANTS", 50, 10),
		volEntry(t, "test", "alice", "USD/2", "OPS", 25, 5),
	}

	acct := assembleAccount("alice", entries, nil, true)

	require.Len(t, acct.GetVolumes(), 1)
	entry := acct.GetVolumes()[0]
	require.Equal(t, "USD/2", entry.GetAsset())
	require.Equal(t, "", entry.GetColor(), "collapsed entries are produced under the empty color")
	require.Equal(t, "175", entry.GetVolumes().GetInput())   // 100 + 50 + 25
	require.Equal(t, "15", entry.GetVolumes().GetOutput())   // 0 + 10 + 5
	require.Equal(t, "160", entry.GetVolumes().GetBalance()) // 175 - 15
}

// FindVolume helper round-trip: drilling into the returned Account by
// (asset, color) must return the correct entry both colored and uncolored.
func TestAssembleAccount_FindVolume(t *testing.T) {
	t.Parallel()

	entries := []attributes.ComputedEntry[*raftcmdpb.VolumePair]{
		volEntry(t, "test", "alice", "USD/2", "", 100, 0),
		volEntry(t, "test", "alice", "USD/2", "GRANTS", 50, 0),
	}
	acct := assembleAccount("alice", entries, nil, false)

	require.Equal(t, "100", acct.FindVolume("USD/2", "").GetBalance())
	require.Equal(t, "50", acct.FindVolume("USD/2", "GRANTS").GetBalance())
	require.Nil(t, acct.FindVolume("USD/2", "MISSING"))
	require.Nil(t, acct.FindVolume("EUR/2", ""))
}
