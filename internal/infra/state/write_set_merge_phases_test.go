package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// The replay occupies order slot zero but contributes no new log. The fresh
// log must still receive the annotation from slot one, not slot zero.
func TestAnnotateLogsKeepsOrderSlotsAcrossReplay(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)
	replayed := domain.NewVolumeKey("test", "replayed", "USD", "")
	created := domain.NewVolumeKey("test", "created", "USD", "")

	buf.BeginOrder(0)
	buf.Volumes().Put(replayed, &raftcmdpb.VolumePair{})
	buf.BeginOrder(1)
	buf.Volumes().Put(created, &raftcmdpb.VolumePair{})

	fresh := sentinelLog(false)
	logs, err := buf.annotateLogs(volumePartitionResult{
		kept: []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{
			{Key: replayed}, {Key: created},
		},
	}, []*raftcmdpb.CreatedLogOrReference{
		{Type: &raftcmdpb.CreatedLogOrReference_ReferenceSequence{ReferenceSequence: 7}},
		{Type: &raftcmdpb.CreatedLogOrReference_CreatedLog{CreatedLog: fresh}},
	})
	require.NoError(t, err)
	require.Equal(t, []*commonpb.Log{fresh}, logs)
	require.Equal(t, []*commonpb.TouchedVolume{{Account: "created", Asset: "USD"}}, fresh.GetPayload().GetApply().GetLog().GetNewKeptVolumes())
}
