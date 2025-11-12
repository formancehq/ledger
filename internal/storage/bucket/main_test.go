//go:build it

package bucket_test

import (
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/testing/docker"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	. "github.com/formancehq/go-libs/v3/testing/utils"
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
