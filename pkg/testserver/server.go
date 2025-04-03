package testserver

import (
	"fmt"
	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/publish"
	"github.com/formancehq/go-libs/v2/testing/testservice"
	"github.com/formancehq/ledger/cmd"
)

type PostgresConfiguration bunconnect.ConnectionOptions

func (cfg PostgresConfiguration) GetArgs() []string {
	args := make([]string, 0)
	if cfg.DatabaseSourceName != "" {
		args = append(args, "--"+bunconnect.PostgresURIFlag, cfg.DatabaseSourceName)
	}
	if cfg.MaxIdleConns != 0 {
		args = append(args, "--"+bunconnect.PostgresMaxIdleConnsFlag, fmt.Sprint(cfg.MaxIdleConns))
	}
	if cfg.MaxOpenConns != 0 {
		args = append(args, "--"+bunconnect.PostgresMaxOpenConnsFlag, fmt.Sprint(cfg.MaxOpenConns))
	}
	if cfg.ConnMaxIdleTime != 0 {
		args = append(args, "--"+bunconnect.PostgresConnMaxIdleTimeFlag, fmt.Sprint(cfg.ConnMaxIdleTime))
	}
	return args
}

type ServeConfiguration struct {
	PostgresConfiguration        PostgresConfiguration
	NatsURL                      string
	ExperimentalFeatures         bool
	DisableAutoUpgrade           bool
	BulkMaxSize                  int
	ExperimentalNumscriptRewrite bool
	MaxPageSize                  uint64
	DefaultPageSize              uint64
	WorkerEnabled                bool
	WorkerConfiguration          *WorkerConfiguration
}

func (cfg ServeConfiguration) GetArgs(serverID string) []string {
	args := []string{
		"serve",
		"--" + cmd.BindFlag, ":0",
	}
	args = append(args, cfg.PostgresConfiguration.GetArgs()...)
	if !cfg.DisableAutoUpgrade {
		args = append(args, "--"+cmd.AutoUpgradeFlag)
	}
	if cfg.WorkerEnabled {
		args = append(args, "--"+cmd.WorkerEnabledFlag)
		if cfg.WorkerConfiguration != nil {
			args = append(args, cfg.WorkerConfiguration.GetArgs(serverID)...)
		}
	}
	if cfg.ExperimentalFeatures {
		args = append(
			args,
			"--"+cmd.ExperimentalFeaturesFlag,
		)
	}
	if cfg.BulkMaxSize != 0 {
		args = append(
			args,
			"--"+cmd.BulkMaxSizeFlag,
			fmt.Sprint(cfg.BulkMaxSize),
		)
	}
	if cfg.ExperimentalNumscriptRewrite {
		args = append(
			args,
			"--"+cmd.NumscriptInterpreterFlag,
		)
	}
	if cfg.NatsURL != "" {
		args = append(
			args,
			"--"+publish.PublisherNatsEnabledFlag,
			"--"+publish.PublisherNatsURLFlag, cfg.NatsURL,
			"--"+publish.PublisherTopicMappingFlag, fmt.Sprintf("*:%s", serverID),
		)
	}
	if cfg.MaxPageSize != 0 {
		args = append(args, "--"+cmd.MaxPageSizeFlag, fmt.Sprint(cfg.MaxPageSize))
	}
	if cfg.DefaultPageSize != 0 {
		args = append(args, "--"+cmd.DefaultPageSizeFlag, fmt.Sprint(cfg.DefaultPageSize))
	}

	return args
}

type Server = testservice.Service[ServeConfiguration]
