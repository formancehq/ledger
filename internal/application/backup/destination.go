package backup

import (
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func admissionDestinationKey(destination *raftcmdpb.BackupDestination) string {
	return state.PhysicalDestinationKey(destination)
}
