package systemstore

import (
	"testing"

	"github.com/formancehq/stack/libs/go-libs/testing/docker"
	"github.com/formancehq/stack/libs/go-libs/testing/utils"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/testing/platform/pgtesting"
)

var srv *pgtesting.PostgresServer

func TestMain(m *testing.M) {
	utils.WithTestMain(func(t *utils.TestingTForMain) int {
		srv = pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()))

		return m.Run()
	})
}
