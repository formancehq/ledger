//go:build e2e

package business

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This suite exercises the REST prepared-query surface against the canonical
// flat QueryFilter shape documented in openapi.yml (EN-1465). It guards that
// the wire body matches the spec — no protobuf-internal oneof/wrapper names
// (`and.filters[]`, `field.field.metadata`, `stringCond`, `hardcoded`, the
// QUERY_TARGET_* enum prefix) leak on either the request or the response side.
var _ = Describe("PreparedQuery REST shape (EN-1465)", Ordered, func() {
	const ledgerName = "pq-rest-shape"

	restURL := func(path string) string {
		return fmt.Sprintf("http://localhost:%d/v3/%s%s", testutil.TestSingleHTTPPort, ledgerName, path)
	}

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())
	})

	It("accepts the v2-aligned filter shape on POST and echoes it back on GET", func() {
		// v2-aligned query DSL: $-prefixed operators, single-key
		// operator->{field:value} bodies, $and/$or arrays, $not single.
		body := `{
			"name": "vip-and-ref",
			"target": "TRANSACTIONS",
			"filter": {
				"$and": [
					{"$match": {"reference": "order-1"}},
					{"$match": {"metadata[tier]": "gold"}},
					{"$not": {"$match": {"reverted": true}}}
				]
			}
		}`

		req, err := http.NewRequestWithContext(sharedCtx, http.MethodPost, restURL("/prepared-queries"), bytes.NewBufferString(body))
		Expect(err).To(Succeed())
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		Expect(err).To(Succeed())
		defer func() { _ = resp.Body.Close() }()

		raw, _ := io.ReadAll(resp.Body)
		Expect(resp.StatusCode).To(Equal(http.StatusNoContent), "unexpected status; body=%s", string(raw))

		// List over REST and assert the response body uses the flat shape.
		getReq, err := http.NewRequestWithContext(sharedCtx, http.MethodGet, restURL("/prepared-queries"), nil)
		Expect(err).To(Succeed())
		getResp, err := http.DefaultClient.Do(getReq)
		Expect(err).To(Succeed())
		defer func() { _ = getResp.Body.Close() }()
		Expect(getResp.StatusCode).To(Equal(http.StatusOK))

		listRaw, err := io.ReadAll(getResp.Body)
		Expect(err).To(Succeed())
		listStr := string(listRaw)

		// Positive: the v2 contract is present.
		Expect(listStr).To(ContainSubstring(`"target":"TRANSACTIONS"`))
		Expect(listStr).To(ContainSubstring(`"$match":{"reference":"order-1"}`))

		// Negative: no protobuf-internal names leak.
		for _, leak := range []string{
			`"filters"`, `"stringCond"`, `"intCond"`, `"boolCond"`, `"existsCond"`,
			`"cond"`, `"hardcoded"`, `QUERY_TARGET_`,
		} {
			Expect(listStr).NotTo(ContainSubstring(leak), "protojson-internal name leaked in list body: %s", leak)
		}

		// The listed filter must decode structurally.
		var listBody struct {
			Data []struct {
				Name   string          `json:"name"`
				Target string          `json:"target"`
				Filter json.RawMessage `json:"filter"`
			} `json:"data"`
		}
		Expect(json.Unmarshal(listRaw, &listBody)).To(Succeed())
		Expect(listBody.Data).NotTo(BeEmpty())

		var found bool
		for _, q := range listBody.Data {
			if q.Name == "vip-and-ref" {
				found = true
				var f struct {
					And []json.RawMessage `json:"$and"`
				}
				Expect(json.Unmarshal(q.Filter, &f)).To(Succeed())
				Expect(f.And).To(HaveLen(3), "$and combinator must round-trip as a direct array")
			}
		}
		Expect(found).To(BeTrue(), "created prepared query not found in list")
	})

	It("creates and executes a LOGS-target prepared query over REST (EN-1503)", func() {
		// Produce a couple of logs on this ledger.
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
			actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "logs-rest-a", big.NewInt(10), "USD"),
			}, nil),
			actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "logs-rest-b", big.NewInt(20), "USD"),
			}, nil)))
		Expect(err).To(Succeed())

		// Create a LOGS-target prepared query with a log-only (ledger) filter
		// over REST. Pre-EN-1503 this was rejected at decode ("use ACCOUNTS or
		// TRANSACTIONS").
		createBody := fmt.Sprintf(`{
			"name": "rest-logs",
			"target": "LOGS",
			"filter": {"$match": {"ledger": %q}}
		}`, ledgerName)

		createReq, err := http.NewRequestWithContext(sharedCtx, http.MethodPost, restURL("/prepared-queries"), bytes.NewBufferString(createBody))
		Expect(err).To(Succeed())
		createReq.Header.Set("Content-Type", "application/json")

		createResp, err := http.DefaultClient.Do(createReq)
		Expect(err).To(Succeed())
		defer func() { _ = createResp.Body.Close() }()

		createRaw, _ := io.ReadAll(createResp.Body)
		Expect(createResp.StatusCode).To(Equal(http.StatusNoContent), "unexpected create status; body=%s", string(createRaw))

		// Execute it over REST and assert logData is populated (not empty).
		Eventually(func(g Gomega) {
			execReq, execErr := http.NewRequestWithContext(sharedCtx, http.MethodPost, restURL("/prepared-queries/rest-logs/execute"), bytes.NewBufferString(`{"mode":"LIST"}`))
			g.Expect(execErr).To(Succeed())
			execReq.Header.Set("Content-Type", "application/json")

			execResp, execErr := http.DefaultClient.Do(execReq)
			g.Expect(execErr).To(Succeed())
			defer func() { _ = execResp.Body.Close() }()

			execRaw, _ := io.ReadAll(execResp.Body)
			g.Expect(execResp.StatusCode).To(Equal(http.StatusOK), "unexpected execute status; body=%s", string(execRaw))

			// The ExecutePreparedQueryResponse envelope still leaks the raw
			// protobuf oneof shape (Go-cased Result.Cursor) pending EN-1465's
			// envelope cleanup; the cursor payload itself is camelCase. Assert on
			// the actual wire path so the test tracks reality, not the intended
			// envelope.
			var execBody struct {
				Result struct {
					Cursor struct {
						LogData []json.RawMessage `json:"logData"`
					} `json:"Cursor"`
				} `json:"Result"`
			}
			g.Expect(json.Unmarshal(execRaw, &execBody)).To(Succeed())
			// The log payload field is logData (camelCase); it must carry the
			// ledger's logs rather than the pre-EN-1503 empty cursor.
			g.Expect(len(execBody.Result.Cursor.LogData)).To(BeNumerically(">=", 2), "logData empty; body=%s", string(execRaw))
		}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})

	It("rejects a legacy protojson-shaped filter (no silent acceptance)", func() {
		// The old protojson wire shape (`and.filters[]`, `field.field.metadata`)
		// must be rejected rather than silently parsed — it is not the documented
		// v2-aligned contract.
		body := `{
			"name": "legacy",
			"target": "TRANSACTIONS",
			"filter": {"and": {"filters": [{"field": {"field": {"metadata": "x"}, "existsCond": {}}}]}}
		}`

		req, err := http.NewRequestWithContext(sharedCtx, http.MethodPost, restURL("/prepared-queries"), bytes.NewBufferString(body))
		Expect(err).To(Succeed())
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		Expect(err).To(Succeed())
		defer func() { _ = resp.Body.Close() }()

		Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
	})
})
