//go:build it

package test_suite

import (
	"context"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sync"
	"time"
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

			for server := range servers {
				allServers = append(allServers, server)
			}
		})

		It("each service should be up and running", func() {

			for _, server := range allServers {
				info, err := server.Client().Ledger.GetInfo(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(info.V2ConfigInfoResponse.Version).To(Equal("develop"))
			}

			By("Only one should be a leader", func() {
				Eventually(func() bool {
					leaderFound := false
					for _, server := range allServers {
						if server.IsLeader() {
							if leaderFound {
								Fail("Multiple leaders found")
							}
							leaderFound = true
						}
					}
					return leaderFound
				}).WithTimeout(5 * time.Second).Should(BeTrue())
			})
		})
		Context("When a leader is elected", func() {
			var (
				selected int
			)
			BeforeEach(func() {
				Eventually(func() int {
					for index, server := range allServers {
						if server.IsLeader() {
							selected = index
							return index
						}
					}
					return -1
				}).Should(Not(Equal(-1)))
			})
			Context("and the leader dies", func() {
				BeforeEach(func() {
					Expect(allServers[selected].Stop(context.TODO())).To(BeNil())
					allServers = append(allServers[:selected], allServers[selected+1:]...)
				})
				It("should select another instance", func() {
					Eventually(func() bool {
						for _, server := range allServers {
							if server.IsLeader() {
								return true
							}
						}
						return false
					}).Should(BeTrue())
				})
			})
		})
	})
})
