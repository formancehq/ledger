// Command eventually_pit_convergence proves that every voter and learner in a
// stable leader-reported Raft membership eventually serves the same exact PIT
// result after Antithesis stops faults and competing writers.
package main

import (
	"context"
	"log"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

const overallTimeout = 10 * time.Minute

func main() {
	log.Println("composer: eventually_pit_convergence")

	ctx, cancel := context.WithTimeout(context.Background(), overallTimeout)
	defer cancel()

	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("composer: could not create PIT convergence control connection: %v", err)

		return
	}
	defer conn.Close()

	converged, details := internal.WaitForQuiescentPITConvergence(
		ctx,
		client,
		clusterpb.NewClusterServiceClient(conn),
	)
	assert.Sometimes(
		converged,
		"pit: all quiescent raft members converge exactly",
		details,
	)
}
