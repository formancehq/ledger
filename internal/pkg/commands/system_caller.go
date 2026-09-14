package commands

import "github.com/formancehq/ledger/v3/internal/proto/commonpb"

// System component identifiers recorded on system/internal proposals, so their
// audit entries name the acting subsystem.
const (
	ComponentQueryCheckpoint  = "query-checkpoint-scheduler"
	ComponentMirror           = "mirror"
	ComponentEventsSink       = "events-sink"
	ComponentClusterConfig    = "cluster-config"
	ComponentClusterPolicy    = "cluster-policy"
	ComponentIdempotencyEvict = "idempotency-eviction"
	ComponentBackup           = "backup"
)

// SystemCallerSnapshot builds the CallerSnapshot stamped onto a
// system-initiated proposal.
func SystemCallerSnapshot(component string) *commonpb.CallerSnapshot {
	return &commonpb.CallerSnapshot{
		Principal: &commonpb.CallerSnapshot_System{
			System: &commonpb.SystemCaller{Component: component},
		},
	}
}
