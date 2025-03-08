//go:build it

package test_suite

import (
	"context"
	"encoding/json"
	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/testing/platform/clickhousetesting"
	"github.com/formancehq/go-libs/v2/testing/platform/natstesting"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/system"
	"os"
	"slices"
	"testing"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/testing/docker"
	. "github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	. "github.com/formancehq/go-libs/v2/testing/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func Test(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Test Suite")
}

var (
	dockerPool       = NewDeferred[*docker.Pool]()
	pgServer         = NewDeferred[*PostgresServer]()
	natsServer       = NewDeferred[*natstesting.NatsServer]()
	clickhouseServer = NewDeferred[*clickhousetesting.Server]()
	debug            = os.Getenv("DEBUG") == "true"
	logger           = logging.NewDefaultLogger(GinkgoWriter, debug, false, false)

	DBTemplate = "dbtemplate"
)

type ParallelExecutionContext struct {
	PostgresServer   *PostgresServer
	NatsServer       *natstesting.NatsServer
	ClickhouseServer *clickhousetesting.Server
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

		templateDatabase := ret.NewDatabase(GinkgoT(), WithName(DBTemplate))

		bunDB, err := bunconnect.OpenSQLDB(context.Background(), templateDatabase.ConnectionOptions())
		Expect(err).To(BeNil())

		err = system.Migrate(context.Background(), bunDB)
		Expect(err).To(BeNil())

		// Initialize the _default bucket on the default database
		// This way, we will be able to clone this database to speed up the tests
		err = bucket.GetMigrator(bunDB, ledger.DefaultBucket).Up(context.Background())
		Expect(err).To(BeNil())

		Expect(bunDB.Close()).To(BeNil())

		return ret
	})

	natsServer.LoadAsync(func() *natstesting.NatsServer {
		By("Initializing nats server")
		ret := natstesting.CreateServer(GinkgoT(), debug, logger)
		By("Nats address: " + ret.ClientURL())
		return ret
	})

	if slices.Contains(enabledReplicationDrivers(), "clickhouse") {
		clickhouseServer.LoadAsync(func() *clickhousetesting.Server {
			By("Initializing clickhouse server")
			return clickhousetesting.CreateServer(dockerPool.GetValue())
		})
	} else {
		clickhouseServer.SetValue(&clickhousetesting.Server{})
	}

	By("Waiting services alive")
	Wait(pgServer, natsServer, clickhouseServer)
	By("All services ready.")

	data, err := json.Marshal(ParallelExecutionContext{
		PostgresServer:   pgServer.GetValue(),
		NatsServer:       natsServer.GetValue(),
		ClickhouseServer: clickhouseServer.GetValue(),
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
	clickhouseServer.SetValue(pec.ClickhouseServer)
})

func UseTemplatedDatabase() *Deferred[*Database] {
	return UsePostgresDatabase(pgServer, CreateWithTemplate(DBTemplate))
}
