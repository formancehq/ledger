package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"github.com/formancehq/ledger/test/antithesis/internal"
)

func main() {
	ctx := context.Background()
	client := internal.NewClient()

	for {
		time.Sleep(time.Second)

		_, err := client.Ledger.GetInfo(ctx)
		if err != nil {
			fmt.Printf("Not ready: %s\n", err)
			continue
		}
		break
	}

	lifecycle.SetupComplete(map[string]any{})

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs
}
