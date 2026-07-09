package commands

import "github.com/formancehq/ledger/v3/internal/proto/commonpb"

// System component identifiers recorded as CallerIdentity.system_component on
// system/internal proposals, so their audit entries name the acting subsystem.
const (
	ComponentChapterArchiver  = "chapter-archiver"
	ComponentChapterSealer    = "chapter-sealer"
	ComponentChapterScheduler = "chapter-scheduler"
	ComponentQueryCheckpoint  = "query-checkpoint-scheduler"
	ComponentMirror           = "mirror"
	ComponentEventsSink       = "events-sink"
	ComponentClusterConfig    = "cluster-config"
	ComponentIdempotencyEvict = "idempotency-eviction"
	ComponentBackup           = "backup"
)

// SystemCallerSnapshot builds the CallerSnapshot stamped onto a
// system-initiated proposal. It carries no subject, scopes, or god — only a
// system_component source naming the subsystem — so the FSM records an
// unambiguous system actor in the audit entry.
func SystemCallerSnapshot(component string) *commonpb.CallerSnapshot {
	return &commonpb.CallerSnapshot{
		Identity: &commonpb.CallerIdentity{
			Source: &commonpb.CallerIdentity_SystemComponent{SystemComponent: component},
		},
	}
}
