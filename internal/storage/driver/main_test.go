//go:build it

package driver

import (
	"testing"

	"github.com/formancehq/go-libs/testing/docker"
	"github.com/formancehq/go-libs/testing/utils"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/testing/platform/pgtesting"
)

func TestMain(m *testing.M) {
	utils.WithTestMain(func(t *utils.TestingTForMain) int {
		pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()))

		return m.Run()
	})
}
