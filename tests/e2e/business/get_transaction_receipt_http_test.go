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
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// EN-1510: the HTTP GetTransaction endpoint must return the verifiability
// receipt on the healthy local/leader path, at parity with gRPC. The business
// suite runs a single node that is its own leader and is configured with a
// receipt-signing key (see suite_test.go), so a GET served locally must produce
// a non-empty receipt — the receipt is signed in the shared controller layer,
// which HTTP reaches through RoutedController -> DefaultController.
var _ = Describe("GetTransaction HTTP receipt (EN-1510)", Ordered, func() {
	const ledgerName = "gettx-receipt-http"

	restURL := func(path string) string {
		return fmt.Sprintf("http://localhost:%d/v3/%s%s", testutil.TestSingleHTTPPort, ledgerName, path)
	}

	var createdTxID uint64

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())

		logs, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
			actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "receipt-http-dest", big.NewInt(100), "USD"),
			}, nil)))
		Expect(err).To(Succeed())
		Expect(logs.GetLogs()).NotTo(BeEmpty())

		createdTxID = logs.GetLogs()[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().GetId()
	})

	It("returns a non-empty, verifiable receipt on the local path", func() {
		getReq, err := http.NewRequestWithContext(sharedCtx, http.MethodGet, restURL(fmt.Sprintf("/transactions/%d", createdTxID)), nil)
		Expect(err).To(Succeed())

		resp, err := http.DefaultClient.Do(getReq)
		Expect(err).To(Succeed())
		defer func() { _ = resp.Body.Close() }()

		raw, err := io.ReadAll(resp.Body)
		Expect(err).To(Succeed())
		Expect(resp.StatusCode).To(Equal(http.StatusOK), "unexpected status; body=%s", string(raw))

		var body struct {
			Data struct {
				Transaction struct {
					ID uint64 `json:"id"`
				} `json:"transaction"`
				Receipt string `json:"receipt"`
			} `json:"data"`
		}
		Expect(json.Unmarshal(raw, &body)).To(Succeed(), "body=%s", string(raw))
		Expect(body.Data.Transaction.ID).To(Equal(createdTxID))
		Expect(body.Data.Receipt).NotTo(BeEmpty(), "HTTP GetTransaction must return a receipt on the local path; body=%s", string(raw))
	})
})
