//go:build e2e

package business

import (
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// EN-1622: the transactions list route (writeOKChecked, sonic) and the
// transaction detail route (writeOK, sonic) must serialize the SAME
// transaction to the SAME JSON shape. Before this ticket the list route went
// through protojson instead, which ignores Transaction's custom MarshalJSON
// and rendered {"amount":{"v0":"12345"}} / snake_case / base64 instead of the
// hand-written camelCase shape the detail route already produced.
//
// A per-route unit test pins each shape independently and cannot catch this
// class of bug: a field added to one path leaves both unit tests green while
// the routes still disagree. Deep-equating the two live payloads is the only
// assertion that tracks the property the ticket is actually about.
//
// TestTransactionsListShape (see suite_test.go's TestBusiness, which runs this
// spec) is the greppable entry point for this guard.
var _ = Describe("TestTransactionsListShape: list/detail item parity (EN-1622)", Ordered, func() {
	const ledgerName = "tx-list-shape-ledger"

	restURL := func(path string) string {
		return fmt.Sprintf("http://localhost:%d/v3/%s%s", sharedHTTPPort, ledgerName, path)
	}

	var createdTxID uint64

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())

		logs, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.WithReference(
			actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "list-shape-dest", big.NewInt(12345), "USD"),
			}, map[string]string{"category": "list-shape-test"}, nil),
			"list-shape-ref-001",
		)))
		Expect(err).To(Succeed())
		Expect(logs.GetLogs()).NotTo(BeEmpty())

		createdTxID = logs.GetLogs()[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().GetId()

		// Revert the transaction so the reversion fields (reverted, revertedAt,
		// revertedByTransactionId, revertsTransactionId) are populated on the
		// original — those are exactly the fields the pre-fix protojson writer
		// rendered under snake_case/wrapped shapes on the list route only.
		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.RevertTransactionAction(ledgerName, createdTxID, false, false, nil)))
		Expect(err).To(Succeed())
	})

	It("renders the same JSON shape for a transaction on the list route and the detail route", func() {
		// Detail: GET /v3/{ledger}/transactions/{id} -> data.transaction.
		getReq, err := http.NewRequestWithContext(sharedCtx, http.MethodGet, restURL(fmt.Sprintf("/transactions/%d", createdTxID)), nil)
		Expect(err).To(Succeed())

		getResp, err := http.DefaultClient.Do(getReq)
		Expect(err).To(Succeed())
		defer func() { _ = getResp.Body.Close() }()

		getRaw, err := io.ReadAll(getResp.Body)
		Expect(err).To(Succeed())
		Expect(getResp.StatusCode).To(Equal(http.StatusOK), "unexpected status on detail route; body=%s", string(getRaw))

		var detailBody struct {
			Data struct {
				Transaction map[string]any `json:"transaction"`
			} `json:"data"`
		}
		Expect(json.Unmarshal(getRaw, &detailBody)).To(Succeed(), "detail body=%s", string(getRaw))
		Expect(detailBody.Data.Transaction).NotTo(BeEmpty())

		// List: GET /v3/{ledger}/transactions -> data[] — select the matching
		// item by id rather than assuming ordering, since the ticket's contract
		// is about the ITEM SHAPE, not the list ordering.
		listReq, err := http.NewRequestWithContext(sharedCtx, http.MethodGet, restURL("/transactions"), nil)
		Expect(err).To(Succeed())

		listResp, err := http.DefaultClient.Do(listReq)
		Expect(err).To(Succeed())
		defer func() { _ = listResp.Body.Close() }()

		listRaw, err := io.ReadAll(listResp.Body)
		Expect(err).To(Succeed())
		Expect(listResp.StatusCode).To(Equal(http.StatusOK), "unexpected status on list route; body=%s", string(listRaw))

		var listBody struct {
			Data []map[string]any `json:"data"`
		}
		Expect(json.Unmarshal(listRaw, &listBody)).To(Succeed(), "list body=%s", string(listRaw))
		Expect(listBody.Data).NotTo(BeEmpty())

		var (
			listItem map[string]any
			found    bool
		)

		for _, item := range listBody.Data {
			id, ok := item["id"].(float64)
			if ok && uint64(id) == createdTxID {
				listItem = item
				found = true

				break
			}
		}

		Expect(found).To(BeTrue(), "transaction %d not found in list route response; body=%s", createdTxID, string(listRaw))

		// The core assertion: the two routes must produce byte-for-byte
		// equivalent JSON structures for the same transaction. Do NOT weaken
		// this to a field-by-field spot check — a partial check is exactly the
		// kind of drift this test exists to prevent.
		Expect(listItem).To(Equal(detailBody.Data.Transaction), "list item and detail transaction diverge for id %d\nlist item:   %#v\ndetail item: %#v", createdTxID, listItem, detailBody.Data.Transaction)

		// Precise signature of the EN-1622 regression: before the fix, an
		// amount rendered as a protobuf object ({"v0":"12345"}) rather than a
		// JSON number. A map[string]any decodes a bare JSON number as float64,
		// so asserting that type is the exact fingerprint of the bug returning.
		postings, ok := listItem["postings"].([]any)
		Expect(ok).To(BeTrue(), "list item postings is not a JSON array: %#v", listItem["postings"])
		Expect(postings).NotTo(BeEmpty())

		firstPosting, ok := postings[0].(map[string]any)
		Expect(ok).To(BeTrue(), "list item posting is not a JSON object: %#v", postings[0])

		amount := firstPosting["amount"]
		_, isNumber := amount.(float64)
		Expect(isNumber).To(BeTrue(), "list item posting.amount must decode as a JSON number, not an object "+
			"(pre-fix signature was {\"v0\":\"...\"}); got %T: %#v", amount, amount)
	})
})
