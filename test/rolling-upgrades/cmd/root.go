package cmd

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/pkg/client/models/sdkerrors"
	"github.com/pterm/pterm"
	"github.com/pulumi/pulumi/sdk/v3/go/auto"
	"github.com/pulumi/pulumi/sdk/v3/go/auto/optup"
	"github.com/pulumi/pulumi/sdk/v3/go/common/resource/config"
	"github.com/pulumi/pulumi/sdk/v3/go/common/workspace"
	"github.com/spf13/cobra"
	"go.uber.org/atomic"
	"k8s.io/client-go/tools/clientcmd"
	"math/big"
	"os"
)

const (
	baseConfigFlag = "from-config"
	toConfigFlag   = "to-config"
	stackNameFlag  = "stack-name"
	debugFlag      = "debug"
	overlayFlag    = "overlay"
	noCleanupFlag  = "no-cleanup"
)

var rootCmd = &cobra.Command{
	Use:          "rolling-upgrades",
	RunE:         runE,
	SilenceUsage: true,
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().String(baseConfigFlag, "", "Initial configuration")
	rootCmd.Flags().String(stackNameFlag, "tests-rolling-upgrades", "Stack name")
	rootCmd.Flags().Bool(debugFlag, false, "Enable debug mode")
	rootCmd.Flags().StringArray(overlayFlag, nil, "Overlay configs")
	rootCmd.Flags().Bool(noCleanupFlag, false, "Do not cleanup the stack after the test")

	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func runE(cmd *cobra.Command, _ []string) error {
	baseConfig, err := loadConfigFromFlag(cmd, baseConfigFlag)
	if err != nil {
		return fmt.Errorf("failed to load from config: %w", err)
	}

	stackName, err := cmd.Flags().GetString(stackNameFlag)
	if err != nil {
		return fmt.Errorf("failed to load stack name: %w", err)
	}

	noCleanup, err := cmd.Flags().GetBool(noCleanupFlag)
	if err != nil {
		return fmt.Errorf("failed to load no-cleanup flag: %w", err)
	}

	debug, err := cmd.Flags().GetBool(debugFlag)
	if err != nil {
		return fmt.Errorf("failed to load debug flag: %w", err)
	}

	pterm.EnableStyling()
	if debug {
		pterm.EnableDebugMessages()
	}

	info := pterm.Info.WithWriter(cmd.OutOrStdout())

	options := []DeploymentOption{
		WithLogger(LoggerFn(func(fmt string, args ...interface{}) {
			info.Printf(fmt, args...)
		})),
	}
	if debug {
		options = append(options,
			WithProgressStream(cmd.OutOrStdout()),
			WithErrorProgressStream(cmd.ErrOrStderr()),
		)
	}

	stackManager := NewStackManager(options...)

	stack, err := auto.UpsertStackLocalSource(
		cmd.Context(),
		stackName,
		"../../deployments/pulumi",
		auto.Stacks(map[string]workspace.ProjectStack{
			stackName: {Config: config.Map{}},
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to get ledger stack: %w", err)
	}

	ledgerStackUpResult, err := stackManager.Deploy(cmd.Context(), stack, *baseConfig, optup.Refresh())
	if err != nil {
		return fmt.Errorf("failed to deploy stack: %w", err)
	}

	projectSettings, err := stack.Workspace().ProjectSettings(cmd.Context())
	if err != nil {
		return fmt.Errorf("failed to get project settings: %w", err)
	}

	startingConfig, err := clientcmd.NewDefaultPathOptions().GetStartingConfig()
	if err != nil {
		return fmt.Errorf("failed to get kubeconfig: %w", err)
	}

	accessTokenCreationStack, err := auto.UpsertStackInlineSource(
		cmd.Context(),
		stack.Name(),
		string(projectSettings.Name)+"-access-token",
		hackStack,
		auto.Stacks(map[string]workspace.ProjectStack{
			stack.Name(): {Config: config.Map{}},
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to get access token creation stack: %w", err)
	}
	defer func() {
		_ = stackManager.Destroy(cmd.Context(), accessTokenCreationStack)
	}()

	hashStackResult, err := stackManager.Deploy(cmd.Context(), accessTokenCreationStack, nil, optup.Refresh())
	if err != nil {
		return fmt.Errorf("failed to up access token creation stack: %w", err)
	}

	apiClient := createAPIClient(
		ledgerStackUpResult.Outputs["namespace"].Value.(string),
		ledgerStackUpResult.Outputs["api-service"].Value.(string),
		hashStackResult.Outputs["token"].Value.(string),
		startingConfig,
		func(_ context.Context) func(fmt string, args ...interface{}) {
			return func(fmt string, args ...interface{}) {
				pterm.Debug.
					WithWriter(cmd.OutOrStdout()).
					Printf(fmt, args...)
			}
		},
	)

	events := make(chan any, 1)
	errCh := make(chan error, 1)

	workflowContext, cancelWorkflow := context.WithCancel(cmd.Context())
	defer cancelWorkflow()

	go func() {
		info.Printf("Starting workflow...\r\n")
		errCh <- runWorkflow(workflowContext, apiClient, events)
		info.Printf("Workflow terminated.\r\n")
	}()

	counter := atomic.Int64{}
	deployErrCh := make(chan error, 1)
l:
	for {
		select {
		case <-events:
			if counter.Add(1) == 100 {
				info.Printf("100 transactions inserted, triggering rolling upgrade...\r\n")
				go func() {
					_, err := stackManager.Deploy(cmd.Context(), stack, *baseConfig,
						optup.Replace([]string{"urn:pulumi:tests-rolling-upgrades::ledger::Formance:Ledger$Formance:Ledger:API$kubernetes:apps/v1:Deployment::ledger-api"}),
					)
					deployErrCh <- err
				}()
			}
		case err := <-deployErrCh:
			if err != nil {
				return fmt.Errorf("failed to up stack: %w", err)
			}
			break l

		case err := <-errCh:
			return fmt.Errorf("failed to run workflow: %w", err)
		}
	}

	// todo: Wait the complete migration

	info.Printf("Waiting some transactions to be processed...\r\n")
	counter.Store(0)
l2:
	for {
		select {
		case <-events:
			if counter.Add(1) == 100 {
				info.Printf("100 transactions inserted, considering the upgrade ok.\r\n")
				break l2
			}
		case err := <-errCh:
			return fmt.Errorf("failed to run workflow: %w", err)
		}
	}

	info.Printf("Cancelling workflow...\r\n")
	cancelWorkflow()

	if !noCleanup {
		info.Printf("Cleaning up resources...\r\n")
		if err := stackManager.Destroy(cmd.Context(), stack); err != nil {
			return fmt.Errorf("failed to cleanup stack: %w", err)
		}
	}

	return nil
}

func runWorkflow(ctx context.Context, client *client.Formance, events chan any) error {

	const ledgerName = "testing"

	_, err := client.Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
		Ledger:                ledgerName,
		V2CreateLedgerRequest: components.V2CreateLedgerRequest{},
	})
	if err != nil {
		switch err := err.(type) {
		case *sdkerrors.V2ErrorResponse:
			if err.ErrorCode != components.V2ErrorsEnumLedgerAlreadyExists {
				return fmt.Errorf("failed to create ledger, got api error: %w", err)
			}
		default:
			return fmt.Errorf("failed to create ledger: %w", err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			_, err := client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
				Ledger: ledgerName,
				V2PostTransaction: components.V2PostTransaction{
					Postings: []components.V2Posting{
						{
							Source:      "world",
							Destination: "bank",
							Asset:       "USD/2",
							Amount:      big.NewInt(100),
						},
					},
				},
			})
			if err != nil {
				return err
			}

			events <- struct{}{}
		}
	}
}
