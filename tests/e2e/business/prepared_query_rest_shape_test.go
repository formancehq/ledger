//go:build e2e

package business

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

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
