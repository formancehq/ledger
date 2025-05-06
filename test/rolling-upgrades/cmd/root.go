package cmd

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/blang/semver"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/pterm/pterm"
	"github.com/pulumi/pulumi/sdk/v3/go/auto"
	"github.com/pulumi/pulumi/sdk/v3/go/auto/optup"
	pconfig "github.com/pulumi/pulumi/sdk/v3/go/common/resource/config"
	"github.com/pulumi/pulumi/sdk/v3/go/common/workspace"
	"github.com/spf13/cobra"
	"go.uber.org/atomic"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"path/filepath"
)

const (
	baseConfigFlag   = "base-config"
	fromRevisionFlag = "from-revision"
	toRevisionFlag   = "to-revision"
	stackNameFlag    = "stack-name"
	debugFlag        = "debug"
	overlayFlag      = "overlay"
	noCleanupFlag    = "no-cleanup"
)

var rootCmd = &cobra.Command{
	Use:           "rolling-upgrades",
	RunE:          runE,
	SilenceUsage:  true,
	SilenceErrors: true,
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		pterm.Error.WithWriter(rootCmd.OutOrStderr()).Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().String(baseConfigFlag, "", "Initial configuration")
	rootCmd.Flags().String(stackNameFlag, "tests-rolling-upgrades", "Stack name")
	rootCmd.Flags().Bool(debugFlag, false, "Enable debug mode")
	rootCmd.Flags().StringArray(overlayFlag, nil, "Overlay configs")
	rootCmd.Flags().Bool(noCleanupFlag, false, "Do not cleanup the stack after the test")
	rootCmd.Flags().String(fromRevisionFlag, "", "From revision/hash")
	rootCmd.Flags().String(toRevisionFlag, "", "To revision/hash")

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

	fromRevision, err := cmd.Flags().GetString(fromRevisionFlag)
	if err != nil {
		return fmt.Errorf("failed to load from revision: %w", err)
	}

	toRevision, err := cmd.Flags().GetString(toRevisionFlag)
	if err != nil {
		return fmt.Errorf("failed to load to revision: %w", err)
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

	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	semver.Parse()

	info.Println("Cloning ledger into temporary directory...\r\n")
	repository, err := git.PlainClone(tmpDir, false, &git.CloneOptions{
		URL: "https://github.com/formancehq/ledger",
	})
	if err != nil {
		return fmt.Errorf("failed to clone repository: %w", err)
	}
	info.Println("Ledger cloned.")

	fromHash, err := repository.ResolveRevision(plumbing.Revision(fromRevision))
	if err != nil {
		return fmt.Errorf("failed to resolve revision %s: %w", fromRevision, err)
	}
	info.Printf("Resolved revision %s to %s...\r\n", fromRevision, fromHash)

	toHash, err := repository.ResolveRevision(plumbing.Revision(toRevision))
	if err != nil {
		return fmt.Errorf("failed to resolve revision %s: %w", toRevision, err)
	}
	info.Printf("Resolved revision %s to %s...\r\n", toRevision, toHash)

	worktree, err := repository.Worktree()
	if err != nil {
		return fmt.Errorf("failed to get worktree: %w", err)
	}

	if err = worktree.Checkout(&git.CheckoutOptions{
		Hash: *fromHash,
	}); err != nil {
		return err
	}

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
		filepath.Join(tmpDir, "deployments/pulumi"),
		auto.Stacks(map[string]workspace.ProjectStack{
			stackName: {Config: pconfig.Map{}},
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
			stack.Name(): {Config: pconfig.Map{}},
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
				if err := worktree.Checkout(&git.CheckoutOptions{
					Hash: *toHash,
				}); err != nil {
					return err
				}
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
