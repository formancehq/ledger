//go:build it

package test_suite

import (
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

var _ = Context("Ledger query API tests", func() {
	var (
		db  = UseTemplatedDatabase()
		ctx = logging.TestingContext()
	)

	testServer := ginkgo.DeferTestServer(
		DeferMap(db, (*pgtesting.Database).ConnectionOptions),
		testservice.WithInstruments(
			testservice.OutputInstrumentation(GinkgoWriter),
		),
		testservice.WithLogger(GinkgoT()),
	)

	When("creating a ledger", func() {
		BeforeEach(func(specContext SpecContext) {
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
				Ledger: "default",
			})
			Expect(err).To(BeNil())
		})

		schemaVersion := "v1.0.0"
		When("inserting schema and transactions", func() {
			BeforeEach(func(specContext SpecContext) {
				// Schema v1.0.0 - Basic validation
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.InsertSchema(ctx, operations.V2InsertSchemaRequest{
					Ledger:  "default",
					Version: schemaVersion,
					V2SchemaData: components.V2SchemaData{
						Chart: map[string]components.V2ChartSegment{
							"world": {
								DotSelf: &components.DotSelf{},
							},
							"bank": {
								AdditionalProperties: map[string]components.V2ChartSegment{
									"$bankID": {
										DotPattern: pointer.For("^[0-9]{3}$"),
									},
								},
							},
							"foo": {
								AdditionalProperties: map[string]components.V2ChartSegment{
									"$fooID": {
										DotSelf: &components.DotSelf{},
									},
								},
							},
							"bar": {
								AdditionalProperties: map[string]components.V2ChartSegment{
									"$barID": {
										DotSelf: &components.DotSelf{},
									},
								},
							},
						},
						Transactions: map[string]components.V2TransactionTemplate{
							"DEPOSIT": {
								Script: `
							vars {
								account $dest
								monetary $mon
							}
							send $mon (
								source = @world
								destination = $dest
							)`,
							},
						},
						Queries: map[string]components.V2QueryTemplate{
							"CUSTOMERS": {
								Name:     "Balance of customers with matching category and hat",
								Resource: components.V2QueryResourceAccounts.ToPointer(),
								Vars: map[string]components.V2QueryTemplateVar{
									"category": {
										Type:    "string",
										Default: "foo",
									},
									"hat_type": {
										Type: "string",
									},
								},
								Body: map[string]any{
									"$and": []any{
										map[string]any{
											"$match": map[string]any{
												"address": "<category>:",
											},
										},
										map[string]any{
											"$match": map[string]any{
												"metadata[hat_type]": "<hat_type>",
											},
										},
									},
								},
							},
						},
					},
				})
				Expect(err).To(BeNil())

				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(ctx, operations.V2AddMetadataToAccountRequest{
					Ledger:        "default",
					SchemaVersion: &schemaVersion,
					Address:       "foo:012",
					RequestBody: map[string]string{
						"hat_type": "cap",
					},
				})
				Expect(err).To(BeNil())

				for _, tx := range []components.V2PostTransactionScript{
					{
						Template: pointer.For("DEPOSIT"),
						Vars: map[string]string{
							"dest": "foo:000",
							"mon":  "COIN 42",
						},
					},
					{
						Template: pointer.For("DEPOSIT"),
						Vars: map[string]string{
							"dest": "foo:001",
							"mon":  "COIN 7",
						},
					},
					{
						Template: pointer.For("DEPOSIT"),
						Vars: map[string]string{
							"dest": "bar:000",
							"mon":  "COIN 52",
						},
					},
				} {
					_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
						Ledger:        "default",
						SchemaVersion: &schemaVersion,
						V2PostTransaction: components.V2PostTransaction{
							Force:  pointer.For(true),
							Script: &tx,
						},
					})
					Expect(err).To(BeNil())
				}
			})

			It("should return correct results", func(specContext SpecContext) {
				res, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.RunQuery(ctx, operations.V2RunQueryRequest{
					Ledger:        "default",
					ID:            "CUSTOMERS",
					SchemaVersion: schemaVersion,
					RequestBody: operations.V2RunQueryRequestBody{
						Vars: map[string]string{
							"category": "foo",
							"hat_type": "cap",
						},
					},
				})
				Expect(err).To(BeNil())
				Expect(len(res.V2AccountsCursorResponse.Cursor.Data)).To(Equal(1))
			})
		})
	})
})
