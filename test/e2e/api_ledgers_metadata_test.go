//go:build it

package test_suite

import (
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Ledger engine tests", func() {
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
	When("creating a ledger", func() {
		BeforeEach(func() {
			err := CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
				Ledger: "default",
			})
			Expect(err).To(BeNil())
		})
		When("updating metadata", func() {
			m := map[string]string{
				"foo": "bar",
			}
			BeforeEach(func() {
				err := UpdateLedgerMetadata(ctx, testServer.GetValue(), operations.V2UpdateLedgerMetadataRequest{
					Ledger:      "default",
					RequestBody: m,
				})
				Expect(err).To(BeNil())
			})
			It("should be ok", func() {
				ledger, err := GetLedger(ctx, testServer.GetValue(), operations.V2GetLedgerRequest{
					Ledger: "default",
				})
				Expect(err).To(BeNil())
				Expect(ledger.Metadata).To(Equal(m))
			})
			When("deleting metadata", func() {
				BeforeEach(func() {
					err := DeleteLedgerMetadata(ctx, testServer.GetValue(), operations.V2DeleteLedgerMetadataRequest{
						Ledger: "default",
						Key:    "foo",
					})
					Expect(err).To(BeNil())
				})
				It("should be ok", func() {
					ledger, err := GetLedger(ctx, testServer.GetValue(), operations.V2GetLedgerRequest{
						Ledger: "default",
					})
					Expect(err).To(BeNil())
					Expect(ledger.Metadata).To(BeEmpty())
				})
			})
		})
	})
})
