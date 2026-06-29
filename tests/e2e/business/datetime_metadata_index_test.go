//go:build e2e

package business

import (
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Validates that a transaction metadata field declared as datetime is parsed
// to signed int64 microseconds and supports range queries via the existing
// ordered int64 encoding.
var _ = Describe("DatetimeMetadataIndex", Ordered, func() {
	const ledgerName = "idx-tx-meta-datetime"
	const key = "due_date"

	// microsOf parses an RFC3339 string into signed int64 epoch micros for
	// building range-query bounds (mirrors the server-side conversion).
	microsOf := func(s string) int64 {
		ts, err := time.Parse(time.RFC3339Nano, s)
		Expect(err).To(Succeed())
		return ts.UnixMicro()
	}

	BeforeAll(func() {
		// Declare the datetime field at creation (status COMPLETE), then index it.
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
			{
				TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
				Key:        key,
				Type:       commonpb.MetadataType_METADATA_TYPE_DATETIME,
			},
		})))
		Expect(err).To(Succeed())

		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionMetadataIndexAction(ledgerName, key)))
		Expect(err).To(Succeed())
		Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_TRANSACTION, key)).To(Succeed())
	})

	It("Should range-select transactions by datetime metadata", func() {
		type seed struct {
			actor   string
			dueDate string
		}
		seeds := []seed{
			{actor: "early", dueDate: "2023-01-01T00:00:00Z"},
			{actor: "mid", dueDate: "2024-01-15T10:00:00Z"},
			{actor: "late", dueDate: "2024-06-01T00:00:00Z"},
		}

		ids := map[string]uint64{}
		for _, s := range seeds {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", s.actor, big.NewInt(100), "USD"),
			}, map[string]string{key: s.dueDate})))
			Expect(err).To(Succeed())
			ids[s.actor] = resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id
		}

		// Range [2024-01-01, 2024-12-31] should match "mid" and "late", not "early".
		lo := microsOf("2024-01-01T00:00:00Z")
		hi := microsOf("2024-12-31T23:59:59Z")

		Eventually(func(g Gomega) {
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0,
				actions.Int64RangeMetadataFilter(key, &lo, &hi))
			g.Expect(err).To(Succeed())
			gotIDs := map[uint64]bool{}
			for _, tx := range txs {
				gotIDs[tx.Id] = true
			}
			g.Expect(gotIDs).To(HaveKey(ids["mid"]))
			g.Expect(gotIDs).To(HaveKey(ids["late"]))
			g.Expect(gotIDs).NotTo(HaveKey(ids["early"]), "out-of-range datetime must be excluded")
		}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})

	It("Should treat an unparseable datetime value as non-matching (NullValue)", func() {
		resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
			actions.NewPosting("world", "garbage", big.NewInt(100), "USD"),
		}, map[string]string{key: "not-a-date"})))
		Expect(err).To(Succeed())
		garbageID := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

		lo := int64(0)
		hi := microsOf("2100-01-01T00:00:00Z")

		// A NullValue entry is encoded as null, never inside an int range.
		Consistently(func(g Gomega) {
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0,
				actions.Int64RangeMetadataFilter(key, &lo, &hi))
			g.Expect(err).To(Succeed())
			for _, tx := range txs {
				g.Expect(tx.Id).NotTo(Equal(garbageID), "unparseable datetime must not appear in a range query")
			}
		}).Within(2 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})
})
