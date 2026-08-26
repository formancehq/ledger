//go:build it

package test_suite

import (
	"fmt"
	"math/big"
	"slices"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/testing/deferred"
	"github.com/formancehq/go-libs/v5/pkg/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"

	"github.com/formancehq/ledger/cmd"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
)

var _ = Context("Ledger application multiple instance tests", func() {
	var (
		db  = pgtesting.UsePostgresDatabase(pgServer)
		ctx = logging.TestingContext()
	)

	const nbServer = 3

	When("starting multiple instances of the service", func() {
		var allServers []*testservice.Service
		BeforeEach(func() {
			servers := make(chan *testservice.Service, nbServer)
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
						GetTestServerOptions(deferred.Map(db, (*pgtesting.Database).ConnectionOptions)),
						testservice.WithInstruments(
							testservice.DebugInstrumentation(debug),
							testservice.OutputInstrumentation(GinkgoWriter),
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

		It("serializes post-import atomic bulks across instances", func() {
			const ledgerName = "multi-instance-import"

			_, err := Client(allServers[0]).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())

			importedLogs := `{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"world","destination":"payments:1234","amount":10000,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:07:41.522336Z","id":0,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:07:41.534898Z","idempotencyKey":"","id":0,"hash":"g489GFReBqquboEjkB95X3OU6mheMzgiu63PdSTfMuM="}
{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"payments:1234","destination":"platform","amount":1500,"asset":"EUR/2"},{"source":"payments:1234","destination":"merchants:777","amount":8500,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:07:55.145802Z","id":1,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:07:55.170731Z","idempotencyKey":"","id":1,"hash":"T+2SGiCeC8tagt1tf5E/L7r98wB8tm6EbNd+OJ7ZvCI="}`
			_, err = Client(allServers[0]).Ledger.V2.ImportLogs(ctx, operations.V2ImportLogsRequest{
				Ledger:              ledgerName,
				V2ImportLogsRequest: []byte(importedLogs),
			})
			Expect(err).To(Succeed())

			type result struct {
				id  uint64
				err error
			}
			results := make(chan result, len(allServers))
			start := make(chan struct{})
			for index, server := range allServers {
				go func() {
					result := result{}
					defer func() {
						if recovered := recover(); recovered != nil {
							result.err = fmt.Errorf("atomic bulk panicked: %v", recovered)
						}
						results <- result
					}()
					<-start

					response, err := Client(server).Ledger.V2.CreateBulk(ctx, operations.V2CreateBulkRequest{
						Atomic: pointer.For(true),
						Ledger: ledgerName,
						RequestBody: []components.V2BulkElement{
							components.CreateV2BulkElementCreateTransaction(components.V2BulkElementCreateTransaction{
								Data: &components.V2PostTransaction{
									Postings: []components.V2Posting{{
										Source:      "world",
										Destination: fmt.Sprintf("instance:%d", index),
										Asset:       "USD",
										Amount:      big.NewInt(1),
									}},
								},
							}),
						},
					})
					if err != nil {
						result.err = err
						return
					}
					if len(response.V2BulkResponse.Data) != 1 ||
						response.V2BulkResponse.Data[0].V2BulkElementResultCreateTransaction == nil ||
						response.V2BulkResponse.Data[0].V2BulkElementResultCreateTransaction.Data.ID == nil {
						result.err = fmt.Errorf("unexpected bulk response: %+v", response.V2BulkResponse.Data)
						return
					}
					result.id = response.V2BulkResponse.Data[0].V2BulkElementResultCreateTransaction.Data.ID.Uint64()
				}()
			}
			close(start)

			ids := make([]uint64, 0, len(allServers))
			for range allServers {
				result := <-results
				Expect(result.err).To(Succeed())
				ids = append(ids, result.id)
			}
			slices.Sort(ids)
			Expect(ids).To(Equal([]uint64{2, 3, 4}))
		})
	})
})
