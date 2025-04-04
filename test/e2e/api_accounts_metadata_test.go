//go:build it

package test_suite

import (
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/testing/deferred/ginkgo"
	. "github.com/formancehq/go-libs/v2/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v2/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v2/testing/testservice"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	ledgerevents "github.com/formancehq/ledger/pkg/events"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/formancehq/ledger/pkg/testserver/ginkgo"
	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Ledger accounts metadata API tests", func() {
	var (
		db      = UseTemplatedDatabase()
		ctx     = logging.TestingContext()
		natsURL = ginkgo.DeferMap(natsServer, (*natstesting.NatsServer).ClientURL)
	)

	testServer := DeferTestServer(
		ginkgo.DeferMap(db, (*pgtesting.Database).ConnectionOptions),
		testservice.WithInstruments(
			testservice.NatsInstrumentation(natsURL),
			testservice.DebugInstrumentation(debug),
			testservice.OutputInstrumentation(GinkgoWriter),
		),
		testservice.WithLogger(GinkgoT()),
	)
	var events chan *nats.Msg
	BeforeEach(func(specContext SpecContext) {
		_, events = Subscribe(specContext, testServer, natsURL)
	})

	BeforeEach(func(specContext SpecContext) {
		_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())
	})
	When("setting metadata on a unknown account", func() {
		var (
			metadata = map[string]string{
				"clientType": "gold",
			}
		)
		JustBeforeEach(func(specContext SpecContext) {
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(
				ctx,
				operations.V2AddMetadataToAccountRequest{
					RequestBody: metadata,
					Address:     "foo",
					Ledger:      "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
		})
		It("should be available on api", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetAccount(
				ctx,
				operations.V2GetAccountRequest{
					Address: "foo",
					Ledger:  "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(response.V2AccountResponse.Data).Should(Equal(components.V2Account{
				Address:  "foo",
				Metadata: metadata,
			}))
		})
		Context("Then updating the metadata", func() {
			var (
				newMetadata = map[string]string{
					"clientType": "silver",
				}
			)
			JustBeforeEach(func(specContext SpecContext) {
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(
					ctx,
					operations.V2AddMetadataToAccountRequest{
						RequestBody: newMetadata,
						Address:     "foo",
						Ledger:      "default",
					},
				)
				Expect(err).ToNot(HaveOccurred())
			})
			It("should update the metadata", func(specContext SpecContext) {
				response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetAccount(
					ctx,
					operations.V2GetAccountRequest{
						Address: "foo",
						Ledger:  "default",
					},
				)
				Expect(err).ToNot(HaveOccurred())

				Expect(response.V2AccountResponse.Data).Should(Equal(components.V2Account{
					Address:  "foo",
					Metadata: newMetadata,
				}))
			})
		})
		It("should trigger a new event", func() {
			Eventually(events).Should(Receive(Event(ledgerevents.EventTypeSavedMetadata)))
		})
		Context("with empty metadata", func() {
			BeforeEach(func() {
				metadata = nil
			})
			It("should be OK", func(specContext SpecContext) {
				response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetAccount(
					ctx,
					operations.V2GetAccountRequest{
						Address: "foo",
						Ledger:  "default",
					},
				)
				Expect(err).ToNot(HaveOccurred())

				Expect(response.V2AccountResponse.Data).Should(Equal(components.V2Account{
					Address:  "foo",
					Metadata: map[string]string{},
				}))
			})
			Context("then adding with empty metadata", func() {
				It("should be OK", func(specContext SpecContext) {

					// The first call created the row in the database,
					// the second call should not change the metadata, and checks than updates works.
					_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(
						ctx,
						operations.V2AddMetadataToAccountRequest{
							RequestBody: metadata,
							Address:     "foo",
							Ledger:      "default",
						},
					)
					Expect(err).ToNot(HaveOccurred())

					response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetAccount(
						ctx,
						operations.V2GetAccountRequest{
							Address: "foo",
							Ledger:  "default",
						},
					)
					Expect(err).ToNot(HaveOccurred())

					Expect(response.V2AccountResponse.Data).Should(Equal(components.V2Account{
						Address:  "foo",
						Metadata: map[string]string{},
					}))
				})
			})
		})
	})
})
