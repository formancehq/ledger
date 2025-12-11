//go:build it

package test_suite

import (
	"math/big"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"

	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
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
		It("should be ok when aggregating using $in operator on address", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetBalancesAggregated(
				ctx,
				operations.V2GetBalancesAggregatedRequest{
					RequestBody: map[string]any{
						"$in": map[string]any{
							"address": []any{"bank1", "bank2"},
						},
					},
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(response.V2AggregateBalancesResponse.Data).To(HaveLen(1))
			Expect(response.V2AggregateBalancesResponse.Data["USD/2"]).To(Equal(big.NewInt(400)))
		})
		It("should be ok when aggregating using $in operator on address with non-existing addresses", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetBalancesAggregated(
				ctx,
				operations.V2GetBalancesAggregatedRequest{
					RequestBody: map[string]any{
						"$in": map[string]any{
							"address": []any{"not_existing", "also_not_existing"},
						},
					},
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(response.V2AggregateBalancesResponse.Data).To(HaveLen(0))
		})
		// Test case to reproduce bug: column "accounts_address_array" does not exist
		// This happens when using $or with both exact addresses AND partial addresses with PIT
		It("should be ok when aggregating with PIT and $or filter mixing exact and partial addresses", func(specContext SpecContext) {
			// This test reproduces the same bug as in volumes:
			// Using $or with exact addresses and partial addresses combined with PIT
			// could cause "column accounts_address_array does not exist" error
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetBalancesAggregated(
				ctx,
				operations.V2GetBalancesAggregatedRequest{
					Ledger: "default",
					Pit:    pointer.For(now),
					RequestBody: map[string]interface{}{
						"$or": []any{
							map[string]any{
								"$match": map[string]any{
									"address": "bank:", // partial address - requires accounts_address_array
								},
							},
							map[string]any{
								"$match": map[string]any{
									"address": "world", // exact address - does NOT require accounts_address_array
								},
							},
						},
					},
				},
			)
			Expect(err).ToNot(HaveOccurred())
			// bank1 + bank2 + world all have USD/2, total should be aggregated
			Expect(response.V2AggregateBalancesResponse.Data).To(HaveLen(1))
		})
	})
})
