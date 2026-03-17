package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
)

func main() {
	ctx := context.Background()

	// Wait for the ledger service to become available
	var client servicepb.BucketServiceClient
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
		_ = conn // keep alive
		break
	}

	_ = client
	log.Println("init: ledger service is ready")

	lifecycle.SetupComplete(map[string]any{})

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
}
