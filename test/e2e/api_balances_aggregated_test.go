//go:build it

package test_suite

import (
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/pointer"
	. "github.com/formancehq/go-libs/v2/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v2/testing/testservice"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
	"math/big"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Ledger engine tests", func() {
	var (
		db  = UseTemplatedDatabase()
		ctx = logging.TestingContext()
	)

	testServer := ginkgo.DeferTestServer(
		DeferMap(db, (*pgtesting.Database).ConnectionOptions),
		testservice.WithInstruments(
			testservice.DebugInstrumentation(debug),
			testservice.OutputInstrumentation(GinkgoWriter),
		),
		testservice.WithLogger(GinkgoT()),
	)
	now := time.Now().UTC().Round(time.Second)
	When("creating two transactions on a ledger with custom metadata", func() {
		var firstTransactionsInsertedAt time.Time
		BeforeEach(func(specContext SpecContext) {
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
				Ledger: "default",
			})
			Expect(err).To(BeNil())

			ret, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateBulk(ctx, operations.V2CreateBulkRequest{
				RequestBody: []components.V2BulkElement{
					components.CreateV2BulkElementCreateTransaction(components.V2BulkElementCreateTransaction{
						Data: &components.V2PostTransaction{
							Metadata: map[string]string{},
							Postings: []components.V2Posting{{
								Amount:      big.NewInt(100),
								Asset:       "USD/2",
								Destination: "bank1",
								Source:      "world",
							}},
							Timestamp: pointer.For(now.Add(-time.Minute)),
						},
					}),
					components.CreateV2BulkElementCreateTransaction(components.V2BulkElementCreateTransaction{
						Data: &components.V2PostTransaction{
							Metadata: map[string]string{},
							Postings: []components.V2Posting{{
								Amount:      big.NewInt(100),
								Asset:       "USD/2",
								Destination: "bank2",
								Source:      "world",
							}},
							Timestamp: pointer.For(now.Add(-2 * time.Minute)),
						},
					}),
					components.CreateV2BulkElementCreateTransaction(components.V2BulkElementCreateTransaction{
						Data: &components.V2PostTransaction{
							Metadata: map[string]string{},
							Postings: []components.V2Posting{{
								Amount:      big.NewInt(100),
								Asset:       "USD/2",
								Destination: "bank1",
								Source:      "world",
							}},
							Timestamp: pointer.For(now),
						},
					}),
					components.CreateV2BulkElementAddMetadata(components.V2BulkElementAddMetadata{
						Data: &components.Data{
							Metadata: map[string]string{
								"category": "premium",
							},
							TargetID:   components.CreateV2TargetIDStr("bank2"),
							TargetType: components.V2TargetTypeAccount,
						},
					}),
					components.CreateV2BulkElementAddMetadata(components.V2BulkElementAddMetadata{
						Data: &components.Data{
							Metadata: map[string]string{
								"category": "premium",
							},
							TargetID:   components.CreateV2TargetIDStr("bank1"),
							TargetType: components.V2TargetTypeAccount,
						},
					}),
				},
				Ledger: "default",
			})
			Expect(err).To(Succeed())

			firstTransactionsInsertedAt = *ret.V2BulkResponse.Data[2].V2BulkElementResultCreateTransaction.Data.InsertedAt

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateBulk(ctx, operations.V2CreateBulkRequest{
				RequestBody: []components.V2BulkElement{
					components.CreateV2BulkElementCreateTransaction(components.V2BulkElementCreateTransaction{
						Data: &components.V2PostTransaction{
							Metadata: map[string]string{},
							Postings: []components.V2Posting{{
								Amount:      big.NewInt(100),
								Asset:       "USD/2",
								Destination: "bank1",
								Source:      "world",
							}},
							Timestamp: pointer.For(now),
						},
					}),
				},
				Ledger: "default",
			})
			Expect(err).To(Succeed())
		})
		It("should be ok when aggregating using the metadata", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetBalancesAggregated(
				ctx,
				operations.V2GetBalancesAggregatedRequest{
					RequestBody: map[string]any{
						"$match": map[string]any{
							"metadata[category]": "premium",
						},
					},
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(response.V2AggregateBalancesResponse.Data).To(HaveLen(1))
			Expect(response.V2AggregateBalancesResponse.Data["USD/2"]).To(Equal(big.NewInt(400)))
		})
		It("should be ok when aggregating using pit on effective date", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetBalancesAggregated(
				ctx,
				operations.V2GetBalancesAggregatedRequest{
					Ledger: "default",
					Pit:    pointer.For(now.Add(-time.Minute)),
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"address": "bank1",
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(response.V2AggregateBalancesResponse.Data).To(HaveLen(1))
			Expect(response.V2AggregateBalancesResponse.Data["USD/2"]).To(Equal(big.NewInt(100)))
		})
		It("should be ok when aggregating using pit on insertion date", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetBalancesAggregated(
				ctx,
				operations.V2GetBalancesAggregatedRequest{
					Ledger:           "default",
					Pit:              pointer.For(firstTransactionsInsertedAt),
					UseInsertionDate: pointer.For(true),
					RequestBody: map[string]interface{}{
						"$match": map[string]any{
							"address": "bank1",
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(response.V2AggregateBalancesResponse.Data).To(HaveLen(1))
			Expect(response.V2AggregateBalancesResponse.Data["USD/2"]).To(Equal(big.NewInt(200)))
		})
	})
})
