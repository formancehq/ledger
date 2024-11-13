package main

import (
	"context"
	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"github.com/formancehq/ledger/test/antithesis/internal"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx := context.Background()
	client := internal.NewClient()

	for {
		time.Sleep(time.Second)

		_, err := client.Ledger.GetInfo(ctx)
		if err != nil {
			continue
		}
		break
	}

	lifecycle.SetupComplete(map[string]any{
		"Ledger": "Available",
	})

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs
}
