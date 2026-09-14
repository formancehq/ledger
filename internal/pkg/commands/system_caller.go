package commands

import (
	"github.com/formancehq/ledger/v3/internal/domain/attribution"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// System component identifiers recorded on system/internal proposals, so their
// audit entries name the acting subsystem.
const (
	ComponentQueryCheckpoint  = attribution.ComponentQueryCheckpoint
	ComponentMirror           = attribution.ComponentMirror
	ComponentEventsSink       = attribution.ComponentEventsSink
	ComponentClusterConfig    = attribution.ComponentClusterConfig
	ComponentClusterPolicy    = attribution.ComponentClusterPolicy
	ComponentIdempotencyEvict = attribution.ComponentIdempotencyEvict
	ComponentBackup           = attribution.ComponentBackup
)

// SystemCallerSnapshot builds the CallerSnapshot stamped onto a
// system-initiated proposal.
func SystemCallerSnapshot(component attribution.SystemActor) *commonpb.CallerSnapshot {
	capability, err := attribution.NewSystem(component)
	if err != nil {
		panic(err)
	}

	return capability.Snapshot()
}
