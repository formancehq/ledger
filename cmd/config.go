package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func LoadConfig[V any](cmd *cobra.Command) (*V, error) {
	var cfg V
	if err := MapConfig(cmd, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func MapConfig(cmd *cobra.Command, cfg any) error {
	v := viper.New()
	if err := v.BindPFlags(cmd.Flags()); err != nil {
		return fmt.Errorf("binding flags: %w", err)
	}

	if err := v.Unmarshal(cfg); err != nil {
		return fmt.Errorf("unmarshalling config: %w", err)
	}

	return nil
}
