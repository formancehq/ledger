//go:build it

package test_suite

import (
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/pkg/features"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"math/big"
	"time"
)

var _ = Context("Logs block async hashing", func() {
	var (
		db  = UseTemplatedDatabase()
		ctx = logging.TestingContext()
	)

	const (
		blockSize = 10
		nbBlock   = 10
		txCount   = blockSize * nbBlock
	)

	testServer := DeferTestServer(debug, GinkgoWriter, func() ServeConfiguration {
		return ServeConfiguration{
			PostgresConfiguration: PostgresConfiguration(db.GetValue().ConnectionOptions()),
			NatsURL:               natsServer.GetValue().ClientURL(),
			ExperimentalFeatures:  true,
		}
	})
	DeferTestWorker(debug, GinkgoWriter, func() WorkerConfiguration {
		return WorkerConfiguration{
			PostgresConfiguration: PostgresConfiguration(db.GetValue().ConnectionOptions()),
			LogsHashBlockMaxSize:  blockSize,
			LogsHashBlockCRONSpec: "* * * * * *", // every second
		}
	})
	JustBeforeEach(func() {
		err := CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
			Ledger: "default",
			V2CreateLedgerRequest: components.V2CreateLedgerRequest{
				Features: features.MinimalFeatureSet.With(features.FeatureHashLogs, "ASYNC"),
			},
		})
		Expect(err).To(BeNil())
	})

	When(fmt.Sprintf("creating %d transactions", txCount), func() {
		JustBeforeEach(func() {
			requestBody := make([]components.V2BulkElement, 0, txCount)
			for i := 0; i < txCount; i++ {
				requestBody = append(requestBody, components.CreateV2BulkElementCreateTransaction(
					components.V2BulkElementCreateTransaction{
						Data: &components.V2PostTransaction{
							Metadata: map[string]string{},
							Postings: []components.V2Posting{{
								Amount:      big.NewInt(100),
								Asset:       "USD",
								Source:      "world",
								Destination: "bank",
							}},
						},
					},
				))
			}

			_, err := CreateBulk(ctx, testServer.GetValue(), operations.V2CreateBulkRequest{
				Ledger:      "default",
				RequestBody: requestBody,
				Atomic:      pointer.For(true),
			})
			Expect(err).To(BeNil())
		})
		It(fmt.Sprintf("should generate %d blocks", nbBlock), func() {
			Eventually(func(g Gomega) bool {
				db := ConnectToDatabase(ctx, GinkgoT(), testServer.GetValue())

				ret := make([]map[string]any, 0)
				err := db.NewSelect().
					Model(&ret).
					ModelTableExpr("_default.logs_blocks").
					Scan(ctx)
				g.Expect(err).To(BeNil())
				g.Expect(ret).To(HaveLen(nbBlock))
				for _, block := range ret {
					g.Expect(block["hash"]).NotTo(BeEmpty())
				}

				return true
			}).
				WithTimeout(5 * time.Second).
				Should(BeTrue())
		})
	})
})
