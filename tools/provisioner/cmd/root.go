package cmd

import (
	"fmt"
	"github.com/formancehq/ledger/pkg/client"
	provisionner "github.com/formancehq/ledger/tools/provisioner/pkg"
	"gopkg.in/yaml.v2"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "provisioner",
	Short: "Provision a ledger instance",
	RunE:  run,
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().String("config", "", "No default")
	rootCmd.Flags().String("state-store", "", "Where to store the state")
	rootCmd.Flags().String("ledger-url", "", "URL of the ledger")

	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func run(cmd *cobra.Command, _ []string) error {
	configFilePath, err := cmd.Flags().GetString("config")
	if err != nil {
		return err
	}
	if configFilePath == "" {
		return fmt.Errorf("config is required")
	}

	serverUrl, err := cmd.Flags().GetString("ledger-url")
	if err != nil {
		return err
	}
	if serverUrl == "" {
		return fmt.Errorf("ledger-url is required")
	}

	stateStore, err := cmd.Flags().GetString("state-store")
	if err != nil {
		return err
	}

	var store provisionner.Store
	switch {
	case strings.HasPrefix(stateStore, "file://"):
		store = provisionner.NewFileStore(stateStore[7:])
	case strings.HasPrefix(stateStore, "k8s://"):
		parts := strings.SplitN(stateStore[6:], "/", 3)
		if len(parts) != 3 {
			return fmt.Errorf("invalid usage of k8s store")
		}
		fmt.Printf("Initialize k8s store %s/%s\r\n", parts[1], parts[2])

		store, err = provisionner.NewK8SConfigMapStore(parts[1], parts[2])
		if err != nil {
			return fmt.Errorf("initializing k8s store: %w", err)
		}
	case stateStore == "":
		return fmt.Errorf("state-store is required")
	default:
		return fmt.Errorf("unsupported state store: %s", stateStore)
	}

	file, err := os.Open(configFilePath)
	if err != nil {
		return err
	}

	cfg := &provisionner.Config{}
	if err := yaml.NewDecoder(file).Decode(cfg); err != nil {
		return err
	}

	ledgerClient := client.New(client.WithServerURL(serverUrl))

	return provisionner.NewReconciler(store, ledgerClient).Reconcile(cmd.Context(), *cfg)
}
