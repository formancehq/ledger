package admission

import (
	"strings"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// validateOrderQueryCheckpointSchedule rejects a cron timezone prefix naming
// any zone other than UTC. The cron parser resolves that zone through
// node-local tzdata during FSM apply, so only UTC resolves identically on
// every node. Existing committed schedules are not revalidated.
func validateOrderQueryCheckpointSchedule(order *raftcmdpb.Order) domain.Describable {
	cron := order.GetSystemScoped().GetSetQueryCheckpointSchedule().GetCron()
	if !strings.HasPrefix(cron, "TZ=") && !strings.HasPrefix(cron, "CRON_TZ=") {
		return nil
	}

	// The parser takes the zone up to the first space and panics without one.
	_, rest, _ := strings.Cut(cron, "=")
	zone, _, hasSchedule := strings.Cut(rest, " ")
	if hasSchedule && zone == "UTC" {
		return nil
	}

	return &domain.ErrInvalidCronExpression{
		Expression: cron,
		Details:    "the only supported time zone prefix is TZ=UTC or CRON_TZ=UTC followed by a space",
	}
}
