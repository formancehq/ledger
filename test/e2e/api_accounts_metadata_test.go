//go:build it

package test_suite

import (
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	. "github.com/formancehq/go-libs/v3/testing/api"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"
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
		natsURL = DeferMap(natsServer, (*natstesting.NatsServer).ClientURL)
	)

	testServer := DeferTestServer(
		DeferMap(db, (*pgtesting.Database).ConnectionOptions),
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

	When("using idempotency keys with account metadata", func() {
		var (
			metadata = map[string]string{
				"type": "checking",
				"tier": "premium",
			}
		)

		It("should succeed when using the same idempotency key with same data", func(specContext SpecContext) {
			// First call with idempotency key
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(
				ctx,
				operations.V2AddMetadataToAccountRequest{
					RequestBody:    metadata,
					Address:        "account-ik-test",
					Ledger:         "default",
					IdempotencyKey: pointer.For("account-meta-key-1"),
				},
			)
			Expect(err).ToNot(HaveOccurred())

			// Second call with same idempotency key and same data
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(
				ctx,
				operations.V2AddMetadataToAccountRequest{
					RequestBody:    metadata,
					Address:        "account-ik-test",
					Ledger:         "default",
					IdempotencyKey: pointer.For("account-meta-key-1"),
				},
			)
			Expect(err).ToNot(HaveOccurred())

			// Verify metadata was set correctly
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetAccount(
				ctx,
				operations.V2GetAccountRequest{
					Address: "account-ik-test",
					Ledger:  "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.V2AccountResponse.Data.Metadata).Should(Equal(metadata))
		})

		It("should fail when using the same idempotency key with different data", func(specContext SpecContext) {
			// First call with idempotency key
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(
				ctx,
				operations.V2AddMetadataToAccountRequest{
					RequestBody:    metadata,
					Address:        "account-ik-test-2",
					Ledger:         "default",
					IdempotencyKey: pointer.For("account-meta-key-2"),
				},
			)
			Expect(err).ToNot(HaveOccurred())

			// Second call with same idempotency key but different data
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(
				ctx,
				operations.V2AddMetadataToAccountRequest{
					RequestBody: map[string]string{
						"type": "savings",
						"tier": "basic",
					},
					Address:        "account-ik-test-2",
					Ledger:         "default",
					IdempotencyKey: pointer.For("account-meta-key-2"),
				},
			)
			Expect(err).To(HaveOccurred())
			Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
		})

		It("should succeed when deleting metadata with idempotency key", func(specContext SpecContext) {
			// First add metadata
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(
				ctx,
				operations.V2AddMetadataToAccountRequest{
					RequestBody: metadata,
					Address:     "account-ik-delete-test",
					Ledger:      "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			ikPtr := pointer.For("account-delete-key-1")

			// Delete metadata with idempotency key
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteAccountMetadata(
				ctx,
				operations.V2DeleteAccountMetadataRequest{
					Address:        "account-ik-delete-test",
					Ledger:         "default",
					Key:            "type",
					IdempotencyKey: ikPtr,
				},
			)
			Expect(err).ToNot(HaveOccurred())

			// Second delete with same idempotency key should succeed (idempotent)
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteAccountMetadata(
				ctx,
				operations.V2DeleteAccountMetadataRequest{
					Address:        "account-ik-delete-test",
					Ledger:         "default",
					Key:            "type",
					IdempotencyKey: ikPtr,
				},
			)
			Expect(err).ToNot(HaveOccurred())

			// Verify metadata was partially deleted
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetAccount(
				ctx,
				operations.V2GetAccountRequest{
					Address: "account-ik-delete-test",
					Ledger:  "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response.V2AccountResponse.Data.Metadata).Should(Equal(map[string]string{
				"tier": "premium",
			}))
		})

		It("should fail when deleting metadata with same idempotency key but different parameters", func(specContext SpecContext) {
			// First add metadata
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(
				ctx,
				operations.V2AddMetadataToAccountRequest{
					RequestBody: metadata,
					Address:     "account-ik-delete-test-2",
					Ledger:      "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			ikPtr := pointer.For("account-delete-key-2")

			// Delete "type" metadata with idempotency key
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteAccountMetadata(
				ctx,
				operations.V2DeleteAccountMetadataRequest{
					Address:        "account-ik-delete-test-2",
					Ledger:         "default",
					Key:            "type",
					IdempotencyKey: ikPtr,
				},
			)
			Expect(err).ToNot(HaveOccurred())

			// Try to delete "tier" with same idempotency key should fail
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteAccountMetadata(
				ctx,
				operations.V2DeleteAccountMetadataRequest{
					Address:        "account-ik-delete-test-2",
					Ledger:         "default",
					Key:            "tier", // Different key
					IdempotencyKey: ikPtr,
				},
			)
			Expect(err).To(HaveOccurred())
		})
	})
})
