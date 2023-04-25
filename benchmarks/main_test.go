package benchmarks

import (
	"os"
	"testing"

	_ "github.com/formancehq/ledger/pkg/storage/sqlstorage/ledger/migrates/0-init-schema"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/ory/dockertest/v3/docker"
)

func TestMain(m *testing.M) {
	if err := pgtesting.CreatePostgresServer(pgtesting.WithDockerHostConfigOption(func(hostConfig *docker.HostConfig) {
		hostConfig.CPUCount = 2
	})); err != nil {
		logging.Error(err)
		os.Exit(1)
	}

	code := m.Run()
	if err := pgtesting.DestroyPostgresServer(); err != nil {
		logging.Error(err)
	}
	os.Exit(code)
}
