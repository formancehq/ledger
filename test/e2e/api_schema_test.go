//go:build it

package test_suite

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/go-libs/v3/logging"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"

	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

var _ = Context("Ledger schema API tests", func() {
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

		When("inserting a schema", func() {
			BeforeEach(func(specContext SpecContext) {
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.InsertSchema(ctx, operations.V2InsertSchemaRequest{
					Ledger:  "default",
					Version: "v1.0.0",
				})
				Expect(err).To(BeNil())
			})

			It("should be able to read the schema", func(specContext SpecContext) {
				schema, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetSchema(ctx, operations.V2GetSchemaRequest{
					Ledger:  "default",
					Version: "v1.0.0",
				})
				Expect(err).To(BeNil())
				Expect(schema.V2SchemaResponse.Data.Version).To(Equal("v1.0.0"))
				Expect(schema.V2SchemaResponse.Data.CreatedAt).ToNot(BeZero())
			})

			When("inserting another version of the schema", func() {
				BeforeEach(func(specContext SpecContext) {
					_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.InsertSchema(ctx, operations.V2InsertSchemaRequest{
						Ledger:  "default",
						Version: "v2.0.0",
					})
					Expect(err).To(BeNil())
				})

				It("should be able to read both schema versions", func(specContext SpecContext) {
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
				})

				It("should be able to list all schemas", func(specContext SpecContext) {
					schemas, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListSchemas(ctx, operations.V2ListSchemasRequest{
						Ledger: "default",
					})
					Expect(err).To(BeNil())
					Expect(schemas.V2SchemasCursorResponse.Cursor.Data).To(HaveLen(2))
					Expect(schemas.V2SchemasCursorResponse.Cursor.HasMore).To(BeFalse())
					Expect(schemas.V2SchemasCursorResponse.Cursor.PageSize).To(Equal(int64(15)))

					// Check that schemas are ordered by created_at DESC (newest first)
					Expect(schemas.V2SchemasCursorResponse.Cursor.Data[0].Version).To(Equal("v2.0.0"))
					Expect(schemas.V2SchemasCursorResponse.Cursor.Data[1].Version).To(Equal("v1.0.0"))
				})
			})

			When("trying to read a non-existent schema version", func() {
				It("should return 404", func(specContext SpecContext) {
					_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetSchema(ctx, operations.V2GetSchemaRequest{
						Ledger:  "default",
						Version: "non-existent",
					})
					Expect(err).ToNot(BeNil())
				})
			})

			When("trying to insert the same schema version again", func() {
				It("should fail", func(specContext SpecContext) {
					_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.InsertSchema(ctx, operations.V2InsertSchemaRequest{
						Ledger:  "default",
						Version: "v1.0.0",
					})
					Expect(err).NotTo(BeNil())
				})
			})

			It("should be able to list schemas", func(specContext SpecContext) {
				schemas, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListSchemas(ctx, operations.V2ListSchemasRequest{
					Ledger: "default",
				})
				Expect(err).To(BeNil())
				Expect(schemas.V2SchemasCursorResponse.Cursor.Data).To(HaveLen(1))
				Expect(schemas.V2SchemasCursorResponse.Cursor.Data[0].Version).To(Equal("v1.0.0"))
				Expect(schemas.V2SchemasCursorResponse.Cursor.Data[0].CreatedAt).ToNot(BeZero())
				Expect(schemas.V2SchemasCursorResponse.Cursor.HasMore).To(BeFalse())
				Expect(schemas.V2SchemasCursorResponse.Cursor.PageSize).To(Equal(int64(15)))
			})
		})
	})
})
