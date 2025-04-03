//go:build it

package test_suite

import (
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v2/testing/testservice"
	"github.com/formancehq/ledger/cmd"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sync"
)

var _ = Context("Ledger application multiple instance tests", func() {
	var (
		db  = pgtesting.UsePostgresDatabase(pgServer)
		ctx = logging.TestingContext()
	)

	const nbServer = 3

	When("starting multiple instances of the service", func() {
		var allServers []*Server
		BeforeEach(func() {
			servers := make(chan *Server, nbServer)
			wg := sync.WaitGroup{}
			wg.Add(nbServer)
			waitStart := make(chan struct{})
			for i := 0; i < nbServer; i++ {
				go func() {
					defer GinkgoRecover()
					defer wg.Done()

					// Best effort to start all servers at the same time and detect conflict errors
					<-waitStart

					testServer := testservice.New(
						cmd.NewRootCommand,
						testservice.Configuration[ServeConfiguration]{
							CommonConfiguration: testservice.CommonConfiguration{
								Debug:  debug,
								Output: GinkgoWriter,
							},
							Configuration: ServeConfiguration{
								PostgresConfiguration: PostgresConfiguration(db.GetValue().ConnectionOptions()),
								NatsURL:               natsServer.GetValue().ClientURL(),
								DisableAutoUpgrade:    true,
							},
						},
						testservice.WithInstruments(
							testservice.HTTPServerInstrumentation(),
						),
					)
					Expect(testServer.Start(ctx)).To(Succeed())

					servers <- testServer
				}()
			}

			close(waitStart)
			wg.Wait()
			close(servers)

			for server := range servers {
				allServers = append(allServers, server)
			}
		})

		It("each service should be up and running", func() {
			for _, server := range allServers {
				info, err := Client(server).Ledger.GetInfo(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(info.V2ConfigInfoResponse.Version).To(Equal("develop"))
			}
		})
	})
})
