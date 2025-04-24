package cmd

import (
	"dario.cat/mergo"
	"encoding/json"
	"fmt"
	pconfig "github.com/formancehq/ledger/deployments/pulumi/pkg/config"
	"github.com/pulumi/pulumi/sdk/v3/go/auto"
	"github.com/pulumi/pulumi/sdk/v3/go/auto/optdestroy"
	"github.com/pulumi/pulumi/sdk/v3/go/auto/optup"
	"github.com/pulumi/pulumi/sdk/v3/go/common/resource/config"
	"github.com/pulumi/pulumi/sdk/v3/go/common/workspace"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "embed"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

//go:embed script.js
var script string

const (
	fromConfigFlag = "from-config"
	toConfigFlag   = "to-config"
	stackNameFlag  = "stack-name"
	debugFlag      = "debug"
	overlayFlag    = "overlay"
	noCleanupFlag  = "no-cleanup"
)

type PartialConfig struct {
	pconfig.Common `yaml:",inline"`

	// Storage is the storage configuration for the ledger
	Storage *pconfig.Storage `json:"storage,omitempty" yaml:"storage,omitempty"`

	// API is the API configuration for the ledger
	API *pconfig.API `json:"api,omitempty" yaml:"api,omitempty"`

	// Worker is the worker configuration for the ledger
	Worker *pconfig.Worker `json:"worker,omitempty" yaml:"worker,omitempty"`

	// InstallDevBox is whether to install the dev box
	InstallDevBox bool `json:"install-dev-box,omitempty" yaml:"install-dev-box,omitempty"`
}

var rootCmd = &cobra.Command{
	Use:  "rolling-upgrades",
	RunE: runE,
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().String(fromConfigFlag, "", "Initial configuration")
	rootCmd.Flags().String(toConfigFlag, "", "Target configuration")
	rootCmd.Flags().String(stackNameFlag, "tests-rolling-upgrades", "Stack name")
	rootCmd.Flags().Bool(debugFlag, false, "Enable debug mode")
	rootCmd.Flags().StringArray(overlayFlag, nil, "Overlay configs")
	rootCmd.Flags().Bool(noCleanupFlag, false, "Do not cleanup the stack after the test")

	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func runE(cmd *cobra.Command, _ []string) error {
	fromConfig, err := loadConfigFromFlag(cmd, fromConfigFlag)
	if err != nil {
		return fmt.Errorf("failed to load from config: %w", err)
	}

	toConfig, err := loadConfigFromFlag(cmd, toConfigFlag)
	if err != nil {
		return fmt.Errorf("failed to load to config: %w", err)
	}

	stackName, err := cmd.Flags().GetString(stackNameFlag)
	if err != nil {
		return fmt.Errorf("failed to load stack name: %w", err)
	}

	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		return err
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	tmpConfigFileName := filepath.Join(tmpDir, "config.yaml")
	tmpConfigFile, err := os.Create(tmpConfigFileName)
	if err != nil {
		return err
	}
	defer func() {
		_ = tmpConfigFile.Close()
	}()

	stack, err := auto.UpsertStackLocalSource(
		cmd.Context(),
		stackName,
		"../../deployments/pulumi",
		auto.Stacks(map[string]workspace.ProjectStack{
			stackName: {Config: config.Map{}},
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create stack: %w", err)
	}

	defer func() {
		_ = stack.Cancel(cmd.Context())
	}()

	noCleanup, err := cmd.Flags().GetBool(noCleanupFlag)
	if err != nil {
		return fmt.Errorf("failed to load no-cleanup flag: %w", err)
	}

	// Remove the existing generator if exists
	if err := destroyGeneratorIfExists(cmd, stack); err != nil {
		return fmt.Errorf("failed to destroy existing generator: %w", err)
	}

	if err := updateConfigFile(tmpConfigFileName, *fromConfig); err != nil {
		return fmt.Errorf("failed to update config file: %w", err)
	}

	if err := upStack(cmd, stack, tmpConfigFileName,
		optup.Refresh(),
	); err != nil {
		return fmt.Errorf("failed to up stack: %w", err)
	}

	// let a bit of time to the generator to start
	<-time.After(5 * time.Second)

	if err := updateConfigFile(tmpConfigFileName, *toConfig); err != nil {
		return err
	}

	if err := upStack(cmd, stack, tmpConfigFileName,
		optup.Replace([]string{"urn:pulumi:tests-rolling-upgrades::ledger::Formance:Ledger$Formance:Ledger:API$kubernetes:apps/v1:Deployment::ledger-api"}),
	); err != nil {
		return fmt.Errorf("failed to up stack: %w", err)
	}

	// todo: make checks

	if !noCleanup {
		if err := cleanup(cmd, stack); err != nil {
			return fmt.Errorf("failed to cleanup stack: %w", err)
		}
	}

	return nil
}

func cleanup(cmd *cobra.Command, stack auto.Stack) error {

	pterm.Info.Println("Cleaning up stack...")

	destroyOptions := []optdestroy.Option{}

	debug, err := cmd.Flags().GetBool(debugFlag)
	if err != nil {
		return fmt.Errorf("failed to load debug flag: %w", err)
	}

	if debug {
		destroyOptions = append(destroyOptions,
			optdestroy.ProgressStreams(os.Stdout),
			optdestroy.ErrorProgressStreams(os.Stderr),
		)
	}

	_, err = stack.Destroy(cmd.Context(), destroyOptions...)
	if err != nil {
		return fmt.Errorf("failed to destroy stack: %w", err)
	}

	pterm.Info.Println("Stack cleaned up.")

	return nil
}

func upStack(cmd *cobra.Command, stack auto.Stack, configFileLocation string, options ...optup.Option) error {

	pterm.Info.Println("Upgrading stack...")

	upOptions := append(options,
		optup.ConfigFile(configFileLocation),
	)

	debug, err := cmd.Flags().GetBool(debugFlag)
	if err != nil {
		return fmt.Errorf("failed to load debug flag: %w", err)
	}

	if debug {
		upOptions = append(upOptions,
			optup.ProgressStreams(os.Stdout),
			optup.ErrorProgressStreams(os.Stderr),
		)
	}

	_, err = stack.Up(cmd.Context(), upOptions...)
	if err != nil {
		return fmt.Errorf("failed to up stack: %w", err)
	}

	pterm.Info.Println("Stack upgraded.")

	return nil
}

func destroyGeneratorIfExists(cmd *cobra.Command, stack auto.Stack) error {

	pterm.Info.Println("Destroying generator if exists...")

	debug, err := cmd.Flags().GetBool(debugFlag)
	if err != nil {
		return fmt.Errorf("failed to load debug flag: %w", err)
	}

	destroyOptions := []optdestroy.Option{
		optdestroy.Target([]string{
			"urn:pulumi:ledger-tests-rolling-upgrades::ledger::Formance:Ledger$Formance:Ledger:Tools:Generator::generator",
		}),
		optdestroy.TargetDependents(),
	}
	if debug {
		destroyOptions = append(destroyOptions,
			optdestroy.ProgressStreams(os.Stdout),
			optdestroy.ErrorProgressStreams(os.Stderr),
		)
	}

	_, err = stack.Destroy(cmd.Context(), destroyOptions...)
	// Ugly check...
	if err != nil && !strings.Contains(err.Error(), "no resource named") {
		return err
	}
	if strings.Contains(err.Error(), "no resource named") {
		pterm.Info.Println("No generator to destroy.")
	} else {
		pterm.Info.Println("Generator destroyed.")
	}

	return nil
}

func loadConfigFromFlag(cmd *cobra.Command, flag string) (*pconfig.Config, error) {
	value, err := cmd.Flags().GetString(flag)
	if err != nil {
		return nil, err
	}
	cfg := &PartialConfig{}
	if value != "" {
		if err := json.Unmarshal([]byte(value), cfg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal config: %w", err)
		}
	}

	overlays, err := cmd.Flags().GetStringArray(overlayFlag)
	if err != nil {
		return nil, err
	}
	for _, overlay := range overlays {
		oCfg := &PartialConfig{}
		if err := json.Unmarshal([]byte(overlay), oCfg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal overlay (%s): %w", overlay, err)
		}

		err := mergo.Merge(cfg, oCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to merge overlay (%s): %w", overlay, err)
		}
	}

	return &pconfig.Config{
		Common:        cfg.Common,
		Storage:       cfg.Storage,
		API:           cfg.API,
		Worker:        cfg.Worker,
		InstallDevBox: cfg.InstallDevBox,
		Generator: &pconfig.Generator{
			Ledgers: map[string]pconfig.GeneratorLedgerConfiguration{
				"testing": {
					Script:            script,
					VUs:               1,
					HTTPClientTimeout: pconfig.Duration(2 * time.Second),
					SkipAwait:         true,
				},
			},
		},
	}, nil
}

func updateConfigFile(configFileName string, cfg pconfig.Config) error {
	tmpConfigFile, err := os.Create(configFileName)
	if err != nil {
		return err
	}
	defer func() {
		_ = tmpConfigFile.Close()
	}()

	return yaml.NewEncoder(tmpConfigFile).Encode(struct {
		Config pconfig.Config `yaml:"config"`
	}{
		Config: cfg,
	})
}
