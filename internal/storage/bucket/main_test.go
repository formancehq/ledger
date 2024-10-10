//go:build it

package bucket_test

import (
	"testing"

	"github.com/formancehq/go-libs/testing/docker"
	. "github.com/formancehq/go-libs/testing/utils"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/testing/platform/pgtesting"
)

var (
	srv *pgtesting.PostgresServer
)

func TestMain(m *testing.M) {
	WithTestMain(func(t *TestingTForMain) int {
		srv = pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()))

		return m.Run()
	})
}
