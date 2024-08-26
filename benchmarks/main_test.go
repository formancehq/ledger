package benchmarks

import (
	"github.com/formancehq/stack/libs/go-libs/testing/docker"
	"github.com/formancehq/stack/libs/go-libs/testing/utils"
	dockerlib "github.com/ory/dockertest/v3/docker"
	"testing"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/testing/platform/pgtesting"
)

func TestMain(m *testing.M) {
	utils.WithTestMain(func(t *utils.TestingTForMain) int {
		pgtesting.CreatePostgresServer(
			t,
			docker.NewPool(t, logging.Testing()),
			pgtesting.WithDockerHostConfigOption(func(hostConfig *dockerlib.HostConfig) {
				hostConfig.CPUCount = 2
			}),
		)

		return m.Run()
	})
}
