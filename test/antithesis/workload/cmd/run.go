package cmd

import (
	"context"
	"fmt"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/spf13/cobra"
	"net/http"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/pkg/client"
)

type Details map[string]any

func run(cmd *cobra.Command, _ []string) {
	client := client.New(
		client.WithServerURL("http://gateway:8080"),
		client.WithClient(&http.Client{
			Timeout: 10 * time.Second,
		}),
	)

	waitServicesReady(cmd.Context(), client)
	<-time.After(10 * time.Second)
	runWorkload(cmd.Context(), client)
}

func createLedger(ctx context.Context, client *client.Formance) error {

	deadline := time.Now().Add(10 * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	fmt.Printf("Creating ledger with deadline %s...\r\n", deadline)
	_, err := client.Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
		Ledger: "default",
	})

	if assert.Always(err == nil, "ledger should have been created", Details{
		"error": fmt.Sprintf("%+v\n", err),
	}); err != nil {
		return err
	}

	fmt.Println("Ledger created!")

	return nil
}
