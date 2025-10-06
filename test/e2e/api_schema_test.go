//go:build it

package test_suite

import (
	"math/big"

	"github.com/formancehq/go-libs/v3/pointer"

	"github.com/formancehq/go-libs/v3/logging"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Ledger schema API tests", func() {
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
		It("should return empty list when no schemas exist", func(specContext SpecContext) {
			schemas, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListSchemas(ctx, operations.V2ListSchemasRequest{
				Ledger: "default",
			})
			Expect(err).To(BeNil())
			Expect(schemas.V2SchemasCursorResponse.Cursor.Data).To(HaveLen(0))
			Expect(schemas.V2SchemasCursorResponse.Cursor.HasMore).To(BeFalse())
			Expect(schemas.V2SchemasCursorResponse.Cursor.PageSize).To(Equal(int64(15)))
		})

		When("inserting schemas with different validation rules", func() {
			BeforeEach(func(specContext SpecContext) {
				// Schema v1.0.0 - Basic validation
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.InsertSchema(ctx, operations.V2InsertSchemaRequest{
					Ledger:       "default",
					Version:      "v1.0.0",
					V2SchemaData: components.V2SchemaData{},
				})
				Expect(err).To(BeNil())

				// Schema v2.0.0 - Stricter validation
				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.InsertSchema(ctx, operations.V2InsertSchemaRequest{
					Ledger:       "default",
					Version:      "v2.0.0",
					V2SchemaData: components.V2SchemaData{},
				})
				Expect(err).To(BeNil())

				// Schema v3.0.0 - Account validation
				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.InsertSchema(ctx, operations.V2InsertSchemaRequest{
					Ledger:       "default",
					Version:      "v3.0.0",
					V2SchemaData: components.V2SchemaData{},
				})
				Expect(err).To(BeNil())
			})

			It("should be able to read all schema versions", func(specContext SpecContext) {
				// Read v1.0.0
				schemaV1, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetSchema(ctx, operations.V2GetSchemaRequest{
					Ledger:  "default",
					Version: "v1.0.0",
				})
				Expect(err).To(BeNil())
				Expect(schemaV1.V2SchemaResponse.Data.Version).To(Equal("v1.0.0"))

				// Read v2.0.0
				schemaV2, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetSchema(ctx, operations.V2GetSchemaRequest{
					Ledger:  "default",
					Version: "v2.0.0",
				})
				Expect(err).To(BeNil())
				Expect(schemaV2.V2SchemaResponse.Data.Version).To(Equal("v2.0.0"))

				// Read v3.0.0
				schemaV3, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetSchema(ctx, operations.V2GetSchemaRequest{
					Ledger:  "default",
					Version: "v3.0.0",
				})
				Expect(err).To(BeNil())
				Expect(schemaV3.V2SchemaResponse.Data.Version).To(Equal("v3.0.0"))
			})

			It("should be able to list all schemas", func(specContext SpecContext) {
				schemas, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListSchemas(ctx, operations.V2ListSchemasRequest{
					Ledger: "default",
				})
				Expect(err).To(BeNil())
				Expect(schemas.V2SchemasCursorResponse.Cursor.Data).To(HaveLen(3))
				Expect(schemas.V2SchemasCursorResponse.Cursor.HasMore).To(BeFalse())
				Expect(schemas.V2SchemasCursorResponse.Cursor.PageSize).To(Equal(int64(15)))

				// Check that schemas are ordered by created_at DESC (newest first)
				Expect(schemas.V2SchemasCursorResponse.Cursor.Data[0].Version).To(Equal("v3.0.0"))
				Expect(schemas.V2SchemasCursorResponse.Cursor.Data[1].Version).To(Equal("v2.0.0"))
				Expect(schemas.V2SchemasCursorResponse.Cursor.Data[2].Version).To(Equal("v1.0.0"))
			})

			When("testing transaction creation with schema validation", func() {
				It("should create transaction with v1.0.0 schema", func(specContext SpecContext) {
					schemaVersion := "v1.0.0"
					_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
						Ledger:        "default",
						SchemaVersion: &schemaVersion,
						V2PostTransaction: components.V2PostTransaction{
							Force: pointer.For(true),
							Postings: []components.V2Posting{
								{
									Source:      "bank:001",
									Destination: "users:001",
									Amount:      big.NewInt(100),
									Asset:       "USD",
								},
								{
									Source:      "users:001",
									Destination: "bank:001",
									Amount:      big.NewInt(100),
									Asset:       "USD",
								},
							},
						},
					})
					Expect(err).To(BeNil())
				})

				It("should create transaction with v2.0.0 schema", func(specContext SpecContext) {
					schemaVersion := "v2.0.0"
					_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
						Ledger:        "default",
						SchemaVersion: &schemaVersion,
						V2PostTransaction: components.V2PostTransaction{
							Force: pointer.For(true),
							Postings: []components.V2Posting{
								{
									Source:      "bank:001",
									Destination: "users:001",
									Amount:      big.NewInt(100),
									Asset:       "USD",
								},
								{
									Source:      "users:001",
									Destination: "bank:001",
									Amount:      big.NewInt(100),
									Asset:       "USD",
								},
							},
							Metadata: map[string]string{
								"description": "Test transaction with metadata",
							},
						},
					})
					Expect(err).To(BeNil())
				})

				It("should create transaction with v3.0.0 schema", func(specContext SpecContext) {
					schemaVersion := "v3.0.0"
					_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
						Ledger:        "default",
						SchemaVersion: &schemaVersion,
						V2PostTransaction: components.V2PostTransaction{
							Force: pointer.For(true),
							Postings: []components.V2Posting{
								{
									Source:      "bank:001",
									Destination: "users:001",
									Amount:      big.NewInt(100),
									Asset:       "USD",
								},
								{
									Source:      "users:001",
									Destination: "bank:001",
									Amount:      big.NewInt(100),
									Asset:       "USD",
								},
							},
						},
					})
					Expect(err).To(BeNil())
				})

				It("should fail with non-existent schema version", func(specContext SpecContext) {
					schemaVersion := "non-existent"
					_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
						Ledger:        "default",
						SchemaVersion: &schemaVersion,
						V2PostTransaction: components.V2PostTransaction{
							Force: pointer.For(true),
							Postings: []components.V2Posting{
								{
									Source:      "bank:001",
									Destination: "users:001",
									Amount:      big.NewInt(100),
									Asset:       "USD",
								},
								{
									Source:      "users:001",
									Destination: "bank:001",
									Amount:      big.NewInt(100),
									Asset:       "USD",
								},
							},
						},
					})
					Expect(err).ToNot(BeNil())
				})
			})

			When("testing logs contain schema version", func() {
				It("should include schema version in transaction logs", func(specContext SpecContext) {
					// Create a transaction with schema version
					schemaVersion := "v1.0.0"
					_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
						Ledger:        "default",
						SchemaVersion: &schemaVersion,
						V2PostTransaction: components.V2PostTransaction{
							Force: pointer.For(true),
							Postings: []components.V2Posting{
								{
									Source:      "bank:001",
									Destination: "users:001",
									Amount:      big.NewInt(100),
									Asset:       "USD",
								},
								{
									Source:      "users:001",
									Destination: "bank:001",
									Amount:      big.NewInt(100),
									Asset:       "USD",
								},
							},
						},
					})
					Expect(err).To(BeNil())

					// Get logs and verify schema version is included
					logs, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLogs(ctx, operations.V2ListLogsRequest{
						Ledger: "default",
					})
					Expect(err).To(BeNil())
					Expect(len(logs.V2LogsCursorResponse.Cursor.Data)).To(BeNumerically(">", 0))

					// Find the transaction log
					var transactionLog *components.V2Log
					for _, log := range logs.V2LogsCursorResponse.Cursor.Data {
						if log.Type == "NEW_TRANSACTION" {
							transactionLog = &log
							break
						}
					}
					Expect(transactionLog).ToNot(BeNil())
					Expect(transactionLog.SchemaVersion).ToNot(BeNil())
					Expect(*transactionLog.SchemaVersion).To(Equal("v1.0.0"))
				})
			})

			It("should be able to list schemas", func(specContext SpecContext) {
				schemas, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListSchemas(ctx, operations.V2ListSchemasRequest{
					Ledger: "default",
				})
				Expect(err).To(BeNil())
				Expect(schemas.V2SchemasCursorResponse.Cursor.Data).To(HaveLen(3))
				Expect(schemas.V2SchemasCursorResponse.Cursor.Data[0].Version).To(Equal("v3.0.0"))
				Expect(schemas.V2SchemasCursorResponse.Cursor.Data[0].CreatedAt).ToNot(BeZero())
				Expect(schemas.V2SchemasCursorResponse.Cursor.HasMore).To(BeFalse())
				Expect(schemas.V2SchemasCursorResponse.Cursor.PageSize).To(Equal(int64(15)))
			})
		})
	})
})
