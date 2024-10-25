package cmd

import (
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	ledgerclient "github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/pkg/client/models/sdkerrors"
	"github.com/formancehq/ledger/pkg/generate"
	"os"
	"sync"

	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "generator <ledger-url> <script-location>",
		Short: "Generate data for a ledger. WARNING: This is an experimental tool.",
		RunE:  run,
		Args:  cobra.ExactArgs(2),
	}
	parallelFlag           = "parallel"
	ledgerFlag             = "ledger"
	untilTransactionIDFlag = "until-transaction-id"
)

func run(cmd *cobra.Command, args []string) error {
	ledgerUrl := args[0]
	scriptLocation := args[1]

	fileContent, err := os.ReadFile(scriptLocation)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	vus, err := cmd.Flags().GetInt(parallelFlag)
	if err != nil {
		return fmt.Errorf("failed to get vu: %w", err)
	}

	ledger, err := cmd.Flags().GetString(ledgerFlag)
	if err != nil {
		return fmt.Errorf("failed to get ledger: %w", err)
	}

	untilTransactionID, err := cmd.Flags().GetInt64(untilTransactionIDFlag)
	if err != nil {
		return fmt.Errorf("failed to get untilTransactionID: %w", err)
	}

	client := ledgerclient.New(ledgerclient.WithServerURL(ledgerUrl))

	_, err = client.Ledger.V2.CreateLedger(cmd.Context(), operations.V2CreateLedgerRequest{
		Ledger: ledger,
	})
	if err != nil {
		sdkError := &sdkerrors.V2ErrorResponse{}
		if !errors.As(err, &sdkError) || sdkError.ErrorCode != components.V2ErrorsEnumLedgerAlreadyExists {
			return fmt.Errorf("failed to create ledger: %w", err)
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(vus)

	for i := 0; i < vus; i++ {
		generator, err := generate.NewGenerator(string(fileContent))
		if err != nil {
			return fmt.Errorf("failed to create generator: %w", err)
		}
		go func() {
			defer wg.Done()

			for {
				next := generator.Next(i)
				tx, err := client.Ledger.V2.CreateTransaction(
					cmd.Context(),
					operations.V2CreateTransactionRequest{
						Ledger: ledger,
						V2PostTransaction: components.V2PostTransaction{
							Script: &components.V2PostTransactionScript{
								Plain: next.Script,
								Vars:  next.Variables,
							},
						},
					},
				)
				if err != nil {
					logging.FromContext(cmd.Context()).Errorf("Vu stopped with error: %s", err)
					return
				}
				if untilTransactionID != 0 && tx.V2CreateTransactionResponse.Data.ID.Int64() >= untilTransactionID {
					return
				}
			}
		}()
	}

	wg.Wait()

	return nil
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().IntP(parallelFlag, "p", 1, "Number of parallel users")
	rootCmd.Flags().StringP(ledgerFlag, "l", "default", "Ledger to feed")
	rootCmd.Flags().Int64P(untilTransactionIDFlag, "u", 0, "Stop after this transaction ID")
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
