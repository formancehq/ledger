package ginkgo

import (
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/testing/docker"
	. "github.com/onsi/ginkgo/v2"
)

var pool = new(docker.Pool)

func ActualDockerPool() *docker.Pool {
	return pool
}

func WithNewDockerPool(logger logging.Logger, fn func()) bool {
	return Context("With docker pool", func() {
		BeforeEach(func() {
			*pool = *docker.NewPool(GinkgoT(), logger)
		})
		fn()
	})
}
