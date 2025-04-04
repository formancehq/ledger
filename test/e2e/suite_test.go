//go:build it

package test_suite

import (
	"context"
	"encoding/json"
	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/testing/deferred"
	"github.com/formancehq/go-libs/v2/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v2/testing/testservice"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/system"
	"github.com/nats-io/nats.go"
	"github.com/uptrace/bun"
	"os"
	"testing"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/testing/docker"
	. "github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func Test(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Test Suite")
}

var (
	dockerPool = deferred.New[*docker.Pool]()
	pgServer   = deferred.New[*PostgresServer]()
	natsServer = deferred.New[*natstesting.NatsServer]()
	debug      = os.Getenv("DEBUG") == "true"
	logger     = logging.NewDefaultLogger(GinkgoWriter, debug, false, false)

	DBTemplate = "dbtemplate"
)

type ParallelExecutionContext struct {
	PostgresServer *PostgresServer
	NatsServer     *natstesting.NatsServer
}

var _ = SynchronizedBeforeSuite(func(specContext SpecContext) []byte {
	deferred.RegisterRecoverHandler(GinkgoRecover)

	By("Initializing docker pool")
	dockerPool.SetValue(docker.NewPool(GinkgoT(), logger))

	pgServer.LoadAsync(func() (*PostgresServer, error) {
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

		return ret, nil
	})

	natsServer.LoadAsync(func() (*natstesting.NatsServer, error) {
		By("Initializing nats server")
		ret := natstesting.CreateServer(GinkgoT(), debug, logger)
		By("Nats address: " + ret.ClientURL())
		return ret, nil
	})

	By("Waiting services alive")
	Expect(deferred.WaitContext(specContext, pgServer, natsServer)).To(BeNil())
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

func UseTemplatedDatabase() *deferred.Deferred[*Database] {
	return UsePostgresDatabase(pgServer, CreateWithTemplate(DBTemplate))
}

func ConnectToDatabase(ctx context.Context, dbOptions *deferred.Deferred[bunconnect.ConnectionOptions]) *bun.DB {
	db, err := bunconnect.OpenSQLDB(ctx, dbOptions.GetValue())
	Expect(err).To(BeNil())

	DeferCleanup(db.Close)

	return db
}

func Subscribe(ctx context.Context, d *deferred.Deferred[*testservice.Service], natsURL *deferred.Deferred[string]) (*nats.Subscription, chan *nats.Msg) {

	srv, err := d.Wait(ctx)
	Expect(err).To(BeNil())

	ret := make(chan *nats.Msg)
	conn, err := nats.Connect(natsURL.GetValue())
	Expect(err).To(BeNil())

	subscription, err := conn.Subscribe(srv.GetID(), func(msg *nats.Msg) {
		ret <- msg
	})
	Expect(err).To(BeNil())

	return subscription, ret
}
