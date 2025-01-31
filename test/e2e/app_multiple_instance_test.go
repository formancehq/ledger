//go:build it

package test_suite

import (
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
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
		var servers chan *Server
		BeforeEach(func() {
			servers = make(chan *Server, nbServer)
			wg := sync.WaitGroup{}
			wg.Add(nbServer)
			waitStart := make(chan struct{})
			for i := 0; i < nbServer; i++ {
				go func() {
					defer GinkgoRecover()
					defer wg.Done()

					// Best effort to start all servers at the same time
					<-waitStart

					servers <- New(GinkgoT(), Configuration{
						PostgresConfiguration: db.GetValue().ConnectionOptions(),
						Output:                GinkgoWriter,
						Debug:                 debug,
						NatsURL:               natsServer.GetValue().ClientURL(),
					})
				}()
			}

			close(waitStart)
			wg.Wait()
			close(servers)
		})

		It("each service should be up and running", func() {
			for server := range servers {
				info, err := server.Client().Ledger.V2.GetInfo(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(info.V2ConfigInfoResponse.Version).To(Equal("develop"))
			}
		})
	})
})
