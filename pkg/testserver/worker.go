package testserver

import (
	"github.com/formancehq/go-libs/v2/testing/testservice"
	"github.com/formancehq/ledger/cmd"
	"strconv"
)

type WorkerConfiguration struct {
	PostgresConfiguration PostgresConfiguration
	LogsHashBlockMaxSize  int
	LogsHashBlockCRONSpec string
}

func (cfg WorkerConfiguration) GetArgs(_ string) []string {
	args := []string{"worker"}
	args = append(args, cfg.PostgresConfiguration.GetArgs()...)
	if cfg.LogsHashBlockMaxSize > 0 {
		args = append(args, "--"+cmd.WorkerAsyncBlockHasherMaxBlockSizeFlag, strconv.Itoa(cfg.LogsHashBlockMaxSize))
	}
	if cfg.LogsHashBlockCRONSpec != "" {
		args = append(args, "--"+cmd.WorkerAsyncBlockHasherScheduleFlag, cfg.LogsHashBlockCRONSpec)
	}

	return args
}

type Worker = testservice.Service[WorkerConfiguration]
