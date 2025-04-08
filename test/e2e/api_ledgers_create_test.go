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
	"github.com/formancehq/ledger/pkg/features"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"strings"
)

var _ = Context("Ledger engine tests", func() {
	var (
		db      = UseTemplatedDatabase()
		ctx     = logging.TestingContext()
		natsURL = DeferMap(natsServer, (*natstesting.NatsServer).ClientURL)
	)

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
	})
})
