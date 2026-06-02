package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	expectedVoters := 1
	if v := os.Getenv("EXPECTED_VOTERS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			log.Fatalf("invalid EXPECTED_VOTERS %q: %v", v, err)
		}
		expectedVoters = n
	}

	ctx := context.Background()

	// Wait for the ledger service to become available
	var (
		client        servicepb.BucketServiceClient
		clusterClient clusterpb.ClusterServiceClient
	)
	for {
		time.Sleep(time.Second)
		c, conn, err := internal.NewClient()
		if err != nil {
			fmt.Printf("Not ready (connect): %s\n", err)
			continue
		}
		// Try listing ledgers as a health check
		_, err = internal.ListLedgers(ctx, c)
		if err != nil {
			fmt.Printf("Not ready (list): %s\n", err)
			_ = conn.Close()
			continue
		}
		client = c
		clusterClient = clusterpb.NewClusterServiceClient(conn)
		break
	}

	_ = client
	log.Println("init: ledger service is reachable")

	// Wait for the full cluster to be ready (leader elected, all nodes are voters)
	for {
		time.Sleep(time.Second)
		state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		if err != nil {
			fmt.Printf("Not ready (cluster state): %s\n", err)
			continue
		}
		if state.GetLeader() == 0 {
			fmt.Println("Not ready: no leader elected")
			continue
		}
		voterCount := 0
		for _, n := range state.GetNodes() {
			if n.GetSuffrage() == "Voter" {
				voterCount++
			}
		}
		if voterCount < expectedVoters {
			fmt.Printf("Not ready: %d/%d voters\n", voterCount, expectedVoters)
			continue
		}
		log.Printf("init: cluster ready (leader=%d, voters=%d)", state.GetLeader(), voterCount)
		break
	}

	lifecycle.SetupComplete(map[string]any{})

	// In k8s mode, the init binary runs as an init container and must exit
	// after signaling setup_complete so the main workload container can start.
	if os.Getenv("EXIT_AFTER_SETUP") == "true" {
		log.Println("init: EXIT_AFTER_SETUP=true, exiting")
		return
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
}
