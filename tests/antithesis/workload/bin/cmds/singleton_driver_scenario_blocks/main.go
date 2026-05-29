package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/formancehq/ledger/v3/pkg/scenario"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/block"

	// Register all scenario blocks via init().
	_ "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/block/gaming"
	_ "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/block/lending"
	_ "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/block/marketplace"
	_ "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/block/multicurrency"
	_ "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/block/payroll"
	_ "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/block/subscription"
)

func main() {
	log.Println("composer: scenario_blocks")

	ctx := context.Background()
	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("error creating client: %s", err)
		return
	}
	defer conn.Close()

	// SCENARIO env var selects which scenarios to run.
	// Empty or "all" runs everything. Comma-separated for multiple scenarios.
	// Example: SCENARIO=marketplace,gaming
	scenarioEnv := os.Getenv("SCENARIO")

	var groups []*scenario.BlockGroup
	switch {
	case scenarioEnv == "" || scenarioEnv == "all":
		groups = block.All()
		log.Printf("composer: scenario_blocks: running all groups from %v", block.Scenarios())
	default:
		for _, name := range strings.Split(scenarioEnv, ",") {
			name = strings.TrimSpace(name)
			selected := block.ForScenario(name)
			if len(selected) == 0 {
				log.Printf("composer: scenario_blocks: unknown scenario %q (available: %v)", name, block.Scenarios())
				continue
			}
			groups = append(groups, selected...)
		}
		log.Printf("composer: scenario_blocks: SCENARIO=%s → %d groups selected", scenarioEnv, len(groups))
	}

	block.RunLoop(ctx, client, groups)
}
