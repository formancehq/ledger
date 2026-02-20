//go:build it

package test_suite

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"testing"

	"github.com/go-jose/go-jose/v4"
	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v4/bun/bunconnect"
	"github.com/formancehq/go-libs/v4/logging"
	"github.com/formancehq/go-libs/v4/testing/deferred"
	"github.com/formancehq/go-libs/v4/testing/docker"
	"github.com/formancehq/go-libs/v4/testing/platform/clickhousetesting"
	"github.com/formancehq/go-libs/v4/testing/platform/natstesting"
	. "github.com/formancehq/go-libs/v4/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v4/testing/testservice"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/system"
)

func Test(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Test Suite")
}

var (
	dockerPool       = deferred.New[*docker.Pool]()
	pgServer         = deferred.New[*PostgresServer]()
	natsServer       = deferred.New[*natstesting.NatsServer]()
	clickhouseServer = deferred.New[*clickhousetesting.Server]()
	testIssuerURL    = deferred.New[string]()
	testPrivateKey   *rsa.PrivateKey
	debug            = os.Getenv("DEBUG") == "true"
	logger           = logging.NewDefaultLogger(GinkgoWriter, debug, false, false)

	DBTemplate = "dbtemplate"
)

func init() {
	var err error
	// Use a static random source to have the same key every time (for parallel testing with ginkgo)
	testPrivateKey, err = rsa.GenerateKey(rand.New(rand.NewSource(1)), 2048)
	if err != nil {
		panic("failed to generate test RSA key: " + err.Error())
	}
}

type ParallelExecutionContext struct {
	PostgresServer    *PostgresServer
	NatsServer        *natstesting.NatsServer
	ClickhouseServer  *clickhousetesting.Server
	MockOIDCIssuerURL string
}

var _ = SynchronizedBeforeSuite(func(specContext SpecContext) []byte {
	deferred.RegisterRecoverHandler(GinkgoRecover)

	By("Initializing docker pool")
	dockerPool.SetValue(docker.NewPool(GinkgoT(), logger))

	// Initialize mock OIDC server
	testIssuerURL.LoadAsync(func() (string, error) {
		By("Initializing mock OIDC server")
		var issuerURL string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/.well-known/openid-configuration":
				// OIDC Discovery endpoint
				config := map[string]interface{}{
					"issuer":                 issuerURL,
					"jwks_uri":               issuerURL + "/.well-known/jwks.json",
					"token_endpoint":         issuerURL + "/token",
					"authorization_endpoint": issuerURL + "/authorize",
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(config)

			case "/.well-known/jwks.json":
				// JWKS endpoint - expose the public key
				jwk := jose.JSONWebKey{
					Key:       &testPrivateKey.PublicKey,
					KeyID:     "test-key-id",
					Algorithm: string(jose.RS256),
					Use:       "sig",
				}
				jwks := map[string]interface{}{
					"keys": []jose.JSONWebKey{jwk},
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(jwks)

			default:
				http.NotFound(w, r)
			}
		}))
		issuerURL = server.URL
		By("Mock OIDC server address: " + server.URL)
		return issuerURL, nil
	})

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

		By("Connecting to database...")
		bunDB, err := bunconnect.OpenSQLDB(context.Background(), templateDatabase.ConnectionOptions())
		Expect(err).To(BeNil())

		By("Creating system schema")
		err = system.Migrate(context.Background(), bunDB)
		Expect(err).To(BeNil())

		By("Creating default bucket")
		// Initialize the _default bucket on the default database
		// This way, we will be able to clone this database to speed up the tests
		err = bucket.GetMigrator(bunDB, ledger.DefaultBucket).Up(context.Background())
		Expect(err).To(BeNil())

		By("Closing connection")
		Expect(bunDB.Close()).To(BeNil())

		By("Loaded")
		return ret, nil
	})

	natsServer.LoadAsync(func() (*natstesting.NatsServer, error) {
		By("Initializing nats server")
		ret := natstesting.CreateServer(GinkgoT(), debug, logger)
		By("Nats address: " + ret.ClientURL())
		return ret, nil
	})

	if slices.Contains(enabledReplicationDrivers(), "clickhouse") {
		clickhouseServer.LoadAsync(func() (*clickhousetesting.Server, error) {
			By("Initializing clickhouse server")
			return clickhousetesting.CreateServer(dockerPool.GetValue()), nil
		})
	} else {
		clickhouseServer.SetValue(&clickhousetesting.Server{})
	}

	By("Waiting services alive")
	By("Waiting PG")
	_, err := pgServer.Wait(specContext)
	Expect(err).To(BeNil())
	By("Waiting nats")
	_, err = natsServer.Wait(specContext)
	Expect(err).To(BeNil())
	By("Waiting clickhouse")
	_, err = clickhouseServer.Wait(specContext)
	Expect(err).To(BeNil())
	By("Waiting mock OIDC server")
	_, err = testIssuerURL.Wait(specContext)
	Expect(err).To(BeNil())

	By("All services ready.")

	data, err := json.Marshal(ParallelExecutionContext{
		PostgresServer:    pgServer.GetValue(),
		NatsServer:        natsServer.GetValue(),
		ClickhouseServer:  clickhouseServer.GetValue(),
		MockOIDCIssuerURL: testIssuerURL.GetValue(),
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
	testIssuerURL.SetValue(pec.MockOIDCIssuerURL)
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

// GetTestIssuer returns the issuer URL from the mock OIDC server
// In the first process, it returns the actual server URL
// In parallel processes, it returns the URL from the first process (stored in testIssuerURL)
func GetTestIssuer() *deferred.Deferred[string] {
	return testIssuerURL
}

// GetTestPrivateKey returns the RSA private key used for signing test tokens
func GetTestPrivateKey() *rsa.PrivateKey {
	return testPrivateKey
}
