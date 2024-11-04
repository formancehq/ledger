//go:build it

package test_suite

import (
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	ledgerevents "github.com/formancehq/ledger/pkg/events"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Ledger accounts list API tests", func() {
	var (
		db  = UseTemplatedDatabase()
		ctx = logging.TestingContext()
	)

	testServer := NewTestServer(func() Configuration {
		return Configuration{
			PostgresConfiguration: db.GetValue().ConnectionOptions(),
			Output:                GinkgoWriter,
			Debug:                 debug,
			NatsURL:               natsServer.GetValue().ClientURL(),
		}
	})
	var events chan *nats.Msg
	BeforeEach(func() {
		events = Subscribe(GinkgoT(), testServer.GetValue())
	})

	BeforeEach(func() {
		err := CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
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
		JustBeforeEach(func() {
			err := AddMetadataToAccount(
				ctx,
				testServer.GetValue(),
				operations.V2AddMetadataToAccountRequest{
					RequestBody: metadata,
					Address:     "foo",
					Ledger:      "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
		})
		It("should be available on api", func() {
			response, err := GetAccount(
				ctx,
				testServer.GetValue(),
				operations.V2GetAccountRequest{
					Address: "foo",
					Ledger:  "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(*response).Should(Equal(components.V2Account{
				Address:  "foo",
				Metadata: metadata,
			}))
		})
		It("should trigger a new event", func() {
			Eventually(events).Should(Receive(Event(ledgerevents.EventTypeSavedMetadata)))
		})
		Context("with empty metadata", func() {
			BeforeEach(func() {
				metadata = nil
			})
			It("should be OK", func() {
				response, err := GetAccount(
					ctx,
					testServer.GetValue(),
					operations.V2GetAccountRequest{
						Address: "foo",
						Ledger:  "default",
					},
				)
				Expect(err).ToNot(HaveOccurred())

				Expect(*response).Should(Equal(components.V2Account{
					Address:  "foo",
					Metadata: map[string]string{},
				}))
			})
			Context("then adding with empty metadata", func() {
				It("should be OK", func() {

					// The first call created the row in the database,
					// the second call should not change the metadata, and checks than updates works.
					err := AddMetadataToAccount(
						ctx,
						testServer.GetValue(),
						operations.V2AddMetadataToAccountRequest{
							RequestBody: metadata,
							Address:     "foo",
							Ledger:      "default",
						},
					)
					Expect(err).ToNot(HaveOccurred())

					response, err := GetAccount(
						ctx,
						testServer.GetValue(),
						operations.V2GetAccountRequest{
							Address: "foo",
							Ledger:  "default",
						},
					)
					Expect(err).ToNot(HaveOccurred())

					Expect(*response).Should(Equal(components.V2Account{
						Address:  "foo",
						Metadata: map[string]string{},
					}))
				})
			})
		})
	})
})
