//go:build it

package test_suite

import (
	"fmt"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
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
			MaxPageSizeInstrumentation(15),
			DefaultPageSizeInstrumentation(15),
		),
		testservice.WithLogger(GinkgoT()),
	)

	When("creating 20 ledger", func() {
		BeforeEach(func(specContext SpecContext) {
			for i := range 20 {
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
					Ledger: fmt.Sprintf("ledger%d", i),
					V2CreateLedgerRequest: components.V2CreateLedgerRequest{
						Bucket: pointer.For(fmt.Sprintf("bucket%d", i%2)),
						Metadata: map[string]string{
							"foo": fmt.Sprintf("bar%d", i%3),
						},
					},
				})
				Expect(err).To(BeNil())
			}
		})
		It("should be listable without filter", func(specContext SpecContext) {
			ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
				PageSize: pointer.For(int64(100)),
			})
			Expect(err).To(BeNil())
			Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(15))
		})
		It("filtering on bucket should return 5 ledgers", func(specContext SpecContext) {
			ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
				PageSize: pointer.For(int64(100)),
				RequestBody: map[string]any{
					"$match": map[string]any{
						"bucket": "bucket0",
					},
				},
			})
			Expect(err).To(BeNil())
			Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(10))
		})
		It("filtering on metadata[foo] = 0 should return 7 ledgers", func(specContext SpecContext) {
			ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
				PageSize: pointer.For(int64(100)),
				RequestBody: map[string]any{
					"$match": map[string]any{
						"metadata[foo]": "bar0",
					},
				},
			})
			Expect(err).To(BeNil())
			Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(7))
		})
		It("filtering on name = ledger0 should return 1 ledger", func(specContext SpecContext) {
			ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
				PageSize: pointer.For(int64(100)),
				RequestBody: map[string]any{
					"$match": map[string]any{
						"name": "ledger0",
					},
				},
			})
			Expect(err).To(BeNil())
			Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(1))
		})
		It("filtering on name starting with ledger1 should return 11 ledger", func(specContext SpecContext) {
			ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
				PageSize: pointer.For(int64(100)),
				RequestBody: map[string]any{
					"$like": map[string]any{
						"name": "ledger1%",
					},
				},
			})
			Expect(err).To(BeNil())
			Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(11))
		})
	})
})
