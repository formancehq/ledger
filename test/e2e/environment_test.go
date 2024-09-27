//go:build it

package test_suite

import (
	"encoding/json"
	"github.com/formancehq/go-libs/testing/platform/natstesting"
	"os"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/testing/docker"
	. "github.com/formancehq/go-libs/testing/platform/pgtesting"
	. "github.com/formancehq/go-libs/testing/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var (
	dockerPool = NewDeferred[*docker.Pool]()
	pgServer   = NewDeferred[*PostgresServer]()
	natsServer = NewDeferred[*natstesting.NatsServer]()
	debug      = os.Getenv("DEBUG") == "true"
	logger     = logging.NewDefaultLogger(GinkgoWriter, debug, false)
)

type ParallelExecutionContext struct {
	PostgresServer *PostgresServer
	NatsServer     *natstesting.NatsServer
}

var _ = SynchronizedBeforeSuite(func() []byte {
	By("Initializing docker pool")
	dockerPool.SetValue(docker.NewPool(GinkgoT(), logger))

	pgServer.LoadAsync(func() *PostgresServer {
		By("Initializing postgres server")
		ret := CreatePostgresServer(
			GinkgoT(),
			dockerPool.GetValue(),
			WithPGStatsExtension(),
			WithPGCrypto(),
		)
		By("Postgres address: " + ret.GetDSN())
		return ret
	})
	natsServer.LoadAsync(func() *natstesting.NatsServer {
		By("Initializing nats server")
		ret := natstesting.CreateServer(GinkgoT(), debug, logger)
		By("Nats address: " + ret.ClientURL())
		return ret
	})

	By("Waiting services alive")
	Wait(pgServer, natsServer)
	By("All services ready.")

	data, err := json.Marshal(ParallelExecutionContext{
		PostgresServer: pgServer.GetValue(),
		NatsServer:     natsServer.GetValue(),
	})
	Expect(err).To(BeNil())

	return data
}, func(data []byte) {
	select {
	case <-pgServer.Done():
		// Process #1, setup is terminated
		return
	default:
	}
	pec := ParallelExecutionContext{}
	err := json.Unmarshal(data, &pec)
	Expect(err).To(BeNil())

	pgServer.SetValue(pec.PostgresServer)
	natsServer.SetValue(pec.NatsServer)
})
