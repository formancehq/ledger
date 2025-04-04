//go:build it

package test_suite

import (
	"github.com/formancehq/go-libs/v2/logging"
	. "github.com/formancehq/go-libs/v2/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v2/testing/testservice"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
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

	When("creating a ledger", func() {
		BeforeEach(func(specContext SpecContext) {
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
				Ledger: "default",
			})
			Expect(err).To(BeNil())
		})
		When("updating metadata", func() {
			m := map[string]string{
				"foo": "bar",
			}
			BeforeEach(func(specContext SpecContext) {
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.UpdateLedgerMetadata(ctx, operations.V2UpdateLedgerMetadataRequest{
					Ledger:      "default",
					RequestBody: m,
				})
				Expect(err).To(BeNil())
			})
			It("should be ok", func(specContext SpecContext) {
				ledger, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetLedger(ctx, operations.V2GetLedgerRequest{
					Ledger: "default",
				})
				Expect(err).To(BeNil())
				Expect(ledger.V2GetLedgerResponse.Data.Metadata).To(Equal(m))
			})
			When("deleting metadata", func() {
				BeforeEach(func(specContext SpecContext) {
					_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteLedgerMetadata(ctx, operations.V2DeleteLedgerMetadataRequest{
						Ledger: "default",
						Key:    "foo",
					})
					Expect(err).To(BeNil())
				})
				It("should be ok", func(specContext SpecContext) {
					ledger, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetLedger(ctx, operations.V2GetLedgerRequest{
						Ledger: "default",
					})
					Expect(err).To(BeNil())
					Expect(ledger.V2GetLedgerResponse.Data.Metadata).To(BeEmpty())
				})
			})
		})
	})
})
