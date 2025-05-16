package cmd

import (
	"dario.cat/mergo"
	"encoding/json"
	"fmt"
	pconfig "github.com/formancehq/ledger/deployments/pulumi/pkg/config"
	"github.com/spf13/cobra"
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
	}, nil
}