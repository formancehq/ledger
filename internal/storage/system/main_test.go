//go:build it

package system

import (
	"testing"

	"github.com/formancehq/go-libs/v4/logging"
	"github.com/formancehq/go-libs/v4/testing/docker"
	"github.com/formancehq/go-libs/v4/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v4/testing/utils"
)

var srv *pgtesting.PostgresServer

func TestMain(m *testing.M) {
	utils.WithTestMain(func(t *utils.TestingTForMain) int {
		srv = pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()))

		return m.Run()
	})
}
