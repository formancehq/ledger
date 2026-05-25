//go:build it

package test_suite

import (
	"fmt"
	"math/big"
	"strings"
	"sync"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	. "github.com/formancehq/go-libs/v5/pkg/testing/api"
	. "github.com/formancehq/go-libs/v5/pkg/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v5/pkg/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v5/pkg/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"

	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/pkg/features"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

var _ = Context("Ledger engine tests", func() {
	var (
		db      = UseTemplatedDatabase()
		ctx     = logging.TestingContext()
		natsURL = DeferMap(natsServer, (*natstesting.NatsServer).ClientURL)
	)

	Context("without experimental features", func() {
		testServer := ginkgo.DeferTestServer(
			DeferMap(db, (*pgtesting.Database).ConnectionOptions),
			testservice.WithInstruments(
				testservice.NatsInstrumentation(natsURL),
				testservice.DebugInstrumentation(debug),
				testservice.OutputInstrumentation(GinkgoWriter),
			),
			testservice.WithLogger(GinkgoT()),
		)

		When("creating a new ledger", func() {
			var (
				createLedgerRequest operations.V2CreateLedgerRequest
				err                 error
			)
			BeforeEach(func() {
				createLedgerRequest = operations.V2CreateLedgerRequest{
					Ledger:                "foo",
					V2CreateLedgerRequest: components.V2CreateLedgerRequest{},
				}
			})
			JustBeforeEach(func(specContext SpecContext) {
				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, createLedgerRequest)
			})
			It("should be ok", func() {
				Expect(err).To(BeNil())
			})
			Context("with specific features set", func() {
				BeforeEach(func() {
					createLedgerRequest.V2CreateLedgerRequest.Features = features.MinimalFeatureSet.
						With(features.FeatureMovesHistoryPostCommitEffectiveVolumes, "DISABLED")
				})
				It("should fail", func() {
					Expect(err).NotTo(BeNil())
					Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
				})
			})
		})
	})

	Context("with experimental features", func() {
		testServer := ginkgo.DeferTestServer(
			DeferMap(db, (*pgtesting.Database).ConnectionOptions),
			testservice.WithInstruments(
				testservice.NatsInstrumentation(natsURL),
				testservice.DebugInstrumentation(debug),
				testservice.OutputInstrumentation(GinkgoWriter),
				ExperimentalFeaturesInstrumentation(),
			),
			testservice.WithLogger(GinkgoT()),
		)

		When("creating a new ledger", func() {
			var (
				createLedgerRequest operations.V2CreateLedgerRequest
				err                 error
			)
			BeforeEach(func() {
				createLedgerRequest = operations.V2CreateLedgerRequest{
					Ledger:                "foo",
					V2CreateLedgerRequest: components.V2CreateLedgerRequest{},
				}
			})
			JustBeforeEach(func(specContext SpecContext) {
				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, createLedgerRequest)
			})
			It("should be ok", func() {
				Expect(err).To(BeNil())
			})
			Context("with specific features set", func() {
				BeforeEach(func() {
					createLedgerRequest.V2CreateLedgerRequest.Features = features.MinimalFeatureSet.
						With(features.FeatureMovesHistoryPostCommitEffectiveVolumes, "DISABLED")
				})
				It("should be ok", func() {
					Expect(err).To(BeNil())
				})
			})
			Context("with invalid feature configuration", func() {
				BeforeEach(func() {
					createLedgerRequest.V2CreateLedgerRequest.Features = features.MinimalFeatureSet.
						With(features.FeatureMovesHistoryPostCommitEffectiveVolumes, "XXX")
				})
				It("should fail", func() {
					Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
				})
			})
			Context("with invalid feature name", func() {
				BeforeEach(func() {
					createLedgerRequest.V2CreateLedgerRequest.Features = features.MinimalFeatureSet.
						With("foo", "XXX")
				})
				It("should fail", func() {
					Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
				})
			})
			Context("trying to create another ledger with the same name", func() {
				JustBeforeEach(func(specContext SpecContext) {
					_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
						Ledger: createLedgerRequest.Ledger,
					})
					Expect(err).NotTo(BeNil())
					Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumLedgerAlreadyExists)))
				})
				It("should fail", func() {})
			})
			Context("bucket naming convention depends on the database 63 bytes length (pg constraint)", func() {
				BeforeEach(func() {
					createLedgerRequest.V2CreateLedgerRequest.Bucket = pointer.For(strings.Repeat("a", 64))
				})
				It("should fail with > 63 characters in ledger or bucket name", func() {
					Expect(err).To(HaveOccurred())
				})
			})
			Context("With metadata", func() {
				BeforeEach(func() {
					createLedgerRequest.V2CreateLedgerRequest.Metadata = map[string]string{
						"foo": "bar",
					}
				})
				It("Should be ok", func(specContext SpecContext) {
					ledger, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetLedger(ctx, operations.V2GetLedgerRequest{
						Ledger: createLedgerRequest.Ledger,
					})
					Expect(err).To(BeNil())
					Expect(ledger.V2GetLedgerResponse.Data.Metadata).To(Equal(createLedgerRequest.V2CreateLedgerRequest.Metadata))
				})
			})
			Context("with invalid ledger name", func() {
				BeforeEach(func() {
					createLedgerRequest.Ledger = "invalid\\name\\contains\\some\\backslash"
				})
				It("should fail", func() {
					Expect(err).NotTo(BeNil())
					Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
				})
			})
			Context("with invalid bucket name", func() {
				BeforeEach(func() {
					createLedgerRequest.V2CreateLedgerRequest = components.V2CreateLedgerRequest{
						Bucket: pointer.For("invalid\\name\\contains\\some\\backslash"),
					}
				})
				It("should fail", func() {
					Expect(err).NotTo(BeNil())
					Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
				})
			})
			Context("on alternate bucket", func() {
				BeforeEach(func() {
					createLedgerRequest.V2CreateLedgerRequest = components.V2CreateLedgerRequest{
						Bucket: pointer.For("bucket0"),
					}
				})
				It("should be ok", func() {
					Expect(err).To(BeNil())
				})
			})

			// Regression test: concurrent CreateLedger calls into a brand-new
			// bucket used to leave losers of the migrator advisory-lock race
			// without their per-ledger sequences (transaction_id_<id> /
			// log_id_<id>), because migration 11's DO block only saw the
			// winner's uncommitted _system.ledgers row under READ COMMITTED.
			// The first write to a loser then 500'd with
			// `relation "<bucket>.transaction_id_N" does not exist`.
			Context("concurrent creation into a fresh bucket", func() {
				It("should set up per-ledger sequences for every ledger", func(specContext SpecContext) {
					const n = 10
					bucket := "race-" + uuid.NewString()[:8]

					names := make([]string, n)
					for i := range names {
						names[i] = fmt.Sprintf("ledger-%d", i)
					}

					start := make(chan struct{})
					wg := sync.WaitGroup{}
					errs := make([]error, n)
					for i := 0; i < n; i++ {
						wg.Add(1)
						go func(i int) {
							defer GinkgoRecover()
							defer wg.Done()
							<-start
							_, errs[i] = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx,
								operations.V2CreateLedgerRequest{
									Ledger: names[i],
									V2CreateLedgerRequest: components.V2CreateLedgerRequest{
										Bucket: pointer.For(bucket),
									},
								})
						}(i)
					}
					close(start)
					wg.Wait()

					for i, err := range errs {
						Expect(err).To(BeNil(), "CreateLedger failed for %s: %v", names[i], err)
					}

					// Force the initializing→in-use state transition that
					// runs setval on the per-ledger sequence.
					for _, name := range names {
						_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx,
							operations.V2CreateTransactionRequest{
								Ledger: name,
								V2PostTransaction: components.V2PostTransaction{
									Postings: []components.V2Posting{{
										Source:      "world",
										Destination: "alice",
										Amount:      big.NewInt(1),
										Asset:       "USD",
									}},
								},
							})
						Expect(err).To(BeNil(), "CreateTransaction failed for %s: %v", name, err)
					}
				})
			})
		})
	})
})
