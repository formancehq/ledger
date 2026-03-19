package subscription

import (
	"github.com/formancehq/ledger-v3-poc/pkg/scenario"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal/block"
)

func init() {
	block.Register(scenario.SubscriptionBlocks())
}
