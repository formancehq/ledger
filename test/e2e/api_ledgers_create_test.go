//go:build it

package test_suite

import (
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/pointer"
	. "github.com/formancehq/go-libs/testing/api"
	"github.com/formancehq/go-libs/testing/platform/pgtesting"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/stack/ledger/client/models/components"
	"github.com/formancehq/stack/ledger/client/models/operations"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"strings"
)

var _ = Context("Ledger engine tests", func() {
	var (
		db  = pgtesting.UsePostgresDatabase(pgServer)
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
	When("creating a new ledger", func() {
		var (
			createLedgerRequest operations.V2CreateLedgerRequest
			err                 error
		)
		BeforeEach(func() {
			createLedgerRequest = operations.V2CreateLedgerRequest{
				Ledger:                "foo",
				V2CreateLedgerRequest: &components.V2CreateLedgerRequest{},
			}
		})
		JustBeforeEach(func() {
			err = CreateLedger(ctx, testServer.GetValue(), createLedgerRequest)
		})
		It("should be ok", func() {
			Expect(err).To(BeNil())
		})
		Context("trying to create another ledger with the same name", func() {
			JustBeforeEach(func() {
				err := CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
					Ledger: createLedgerRequest.Ledger,
				})
				Expect(err).NotTo(BeNil())
				Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
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
			It("Should be ok", func() {
				ledger, err := GetLedger(ctx, testServer.GetValue(), operations.V2GetLedgerRequest{
					Ledger: createLedgerRequest.Ledger,
				})
				Expect(err).To(BeNil())
				Expect(ledger.Metadata).To(Equal(createLedgerRequest.V2CreateLedgerRequest.Metadata))
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
				createLedgerRequest.V2CreateLedgerRequest = &components.V2CreateLedgerRequest{
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
				createLedgerRequest.V2CreateLedgerRequest = &components.V2CreateLedgerRequest{
					Bucket: pointer.For("bucket0"),
				}
			})
			It("should be ok", func() {
				Expect(err).To(BeNil())
			})
		})
	})
})
