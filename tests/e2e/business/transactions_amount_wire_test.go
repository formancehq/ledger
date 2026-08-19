//go:build e2e

package business

import (
	"bytes"
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

// EN-1779: the Formance-Bigint-As-String request header is an opt-in. With a
// truthy value every Posting.amount is a quoted decimal string; without it the
// wire stays a bare JSON number, byte for byte as before the ticket.
//
// The unit tests pin each marshaller and each handler in isolation, and the
// schemathesis suite checks both branches against the schema. Neither proves
// exact value preservation: the string branch is constrained by
// `pattern: '^[0-9]+$'`, which happily accepts a truncated "9007199254740992",
// and the `oneOf` accepts either branch whatever the request asked for — so a
// server that ignored the header, or quoted amounts nobody asked for, still
// conforms. This spec is the end-to-end assertion that the exact 16-digit value
// survives on the real REST surface, in both modes, on two routes.
//
// TestTransactionAmountWire (see suite_test.go's TestBusiness, which runs this
// spec) is the greppable entry point for this guard.
var _ = Describe("TestTransactionAmountWire: amount parity above 2^53 (EN-1779)", Ordered, func() {
	const (
		ledgerName = "tx-amount-wire-ledger"
		headerName = "Formance-Bigint-As-String"

		// 2^53 + 1, the smallest positive integer a float64 cannot represent
		// exactly: any decoder that routes this amount through a float64 yields
		// 9007199254740992 instead. Asserting the full literal is what separates
		// "the digits look like an integer" from "the value is intact".
		hugeAmount = "9007199254740993"
	)

	restURL := func(path string) string {
		return fmt.Sprintf("http://localhost:%d/v3/%s%s", sharedHTTPPort, ledgerName, path)
	}

	// getRaw performs the GET and returns the response body unparsed. An empty
	// headerValue means the request carries no opt-in header at all, which is
	// the pre-EN-1779 client behaviour.
	getRaw := func(path, headerValue string) []byte {
		req, err := http.NewRequestWithContext(sharedCtx, http.MethodGet, restURL(path), nil)
		Expect(err).To(Succeed())

		if headerValue != "" {
			req.Header.Set(headerName, headerValue)
		}

		resp, err := http.DefaultClient.Do(req)
		Expect(err).To(Succeed())

		defer func() { _ = resp.Body.Close() }()

		raw, err := io.ReadAll(resp.Body)
		Expect(err).To(Succeed())
		Expect(resp.StatusCode).To(Equal(http.StatusOK), "unexpected status for %s with header %q; body=%s", path, headerValue, string(raw))

		return raw
	}

	// decodeUseNumber is the non-negotiable part of this spec. Decoding into a
	// plain map[string]any or interface{} sends a bare JSON number through
	// float64 and silently returns 9007199254740992, which would make this test
	// pass on a truncated value and defeat its whole purpose. json.Decoder with
	// UseNumber keeps the literal digits in a json.Number, and a quoted amount
	// still lands in a Go string, so the two wire forms stay distinguishable by
	// type. The repository's sonic wrapper (internal/adapter/json) exposes no
	// UseNumber option, hence the deliberate use of stdlib encoding/json here.
	decodeUseNumber := func(raw []byte, target any) error {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()

		if err := decoder.Decode(target); err != nil {
			return fmt.Errorf("decode response body: %w", err)
		}

		return nil
	}

	var createdTxID uint64

	// detailAmount extracts data.transaction.postings[0].amount from the
	// GET /transactions/{id} payload, still in its wire form.
	detailAmount := func(raw []byte) any {
		var body struct {
			Data struct {
				Transaction struct {
					ID       uint64 `json:"id"`
					Postings []struct {
						Amount any `json:"amount"`
					} `json:"postings"`
				} `json:"transaction"`
			} `json:"data"`
		}

		Expect(decodeUseNumber(raw, &body)).To(Succeed(), "detail body=%s", string(raw))
		Expect(body.Data.Transaction.ID).To(Equal(createdTxID), "detail body=%s", string(raw))
		Expect(body.Data.Transaction.Postings).NotTo(BeEmpty(), "detail body=%s", string(raw))

		return body.Data.Transaction.Postings[0].Amount
	}

	// listAmount extracts postings[0].amount for the created transaction from
	// the GET /transactions payload. The item is selected by id rather than by
	// position, since the property under test is the amount wire, not ordering.
	listAmount := func(raw []byte) any {
		var body struct {
			Data []struct {
				ID       uint64 `json:"id"`
				Postings []struct {
					Amount any `json:"amount"`
				} `json:"postings"`
			} `json:"data"`
		}

		Expect(decodeUseNumber(raw, &body)).To(Succeed(), "list body=%s", string(raw))
		Expect(body.Data).NotTo(BeEmpty(), "list body=%s", string(raw))

		for _, item := range body.Data {
			if item.ID != createdTxID {
				continue
			}

			Expect(item.Postings).NotTo(BeEmpty(), "list body=%s", string(raw))

			return item.Postings[0].Amount
		}

		Fail(fmt.Sprintf("transaction %d not found in list route response; body=%s", createdTxID, string(raw)))

		return nil
	}

	// expectNumericWire asserts the default wire: a bare JSON number whose full
	// 16 digits are present. json.Number is only produced for an unquoted
	// number, so the type assertion is itself the "not quoted" assertion.
	expectNumericWire := func(amount any, raw []byte, what string) string {
		number, isNumber := amount.(json.Number)
		Expect(isNumber).To(BeTrue(), "%s: posting.amount must be a bare JSON number on the default wire; got %T (%#v); body=%s", what, amount, amount, string(raw))
		Expect(number.String()).To(Equal(hugeAmount), "%s: posting.amount lost precision on the default wire; body=%s", what, string(raw))
		Expect(string(raw)).To(ContainSubstring(`"amount":`+hugeAmount), "%s: the raw bytes must carry the unquoted amount; body=%s", what, string(raw))

		return number.String()
	}

	// expectStringWire asserts the opt-in wire: exactly the quoted decimal.
	expectStringWire := func(amount any, raw []byte, what string) string {
		text, isString := amount.(string)
		Expect(isString).To(BeTrue(), "%s: posting.amount must be a quoted decimal string when the client opts in; got %T (%#v); body=%s", what, amount, amount, string(raw))
		Expect(text).To(Equal(hugeAmount), "%s: posting.amount is not the exact opted-in value; body=%s", what, string(raw))
		Expect(string(raw)).To(ContainSubstring(fmt.Sprintf(`"amount":%q`, hugeAmount)), "%s: the raw bytes must carry the quoted amount; body=%s", what, string(raw))

		return text
	}

	var (
		defaultDetailRaw   []byte
		defaultListRaw     []byte
		optInDetailRaw     []byte
		optInListRaw       []byte
		nonTruthyDetailRaw []byte
		nonTruthyListRaw   []byte
		paddedDetailRaw    []byte
	)

	BeforeAll(func() {
		amount, ok := new(big.Int).SetString(hugeAmount, 10)
		Expect(ok).To(BeTrue(), "%q is not a valid decimal integer", hugeAmount)

		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())

		logs, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
			actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "amount-wire-dest", amount, "USD"),
			}, nil)))
		Expect(err).To(Succeed())
		Expect(logs.GetLogs()).NotTo(BeEmpty())

		createdTxID = logs.GetLogs()[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().GetId()

		detailPath := fmt.Sprintf("/transactions/%d", createdTxID)

		defaultDetailRaw = getRaw(detailPath, "")
		defaultListRaw = getRaw("/transactions", "")
		optInDetailRaw = getRaw(detailPath, "true")
		optInListRaw = getRaw("/transactions", "true")
		nonTruthyDetailRaw = getRaw(detailPath, "false")
		nonTruthyListRaw = getRaw("/transactions", "false")
		paddedDetailRaw = getRaw(detailPath, "  YES  ")
	})

	It("keeps the full 16-digit amount as a bare JSON number when the client does not opt in", func() {
		expectNumericWire(detailAmount(defaultDetailRaw), defaultDetailRaw, "detail route, no header")
		expectNumericWire(listAmount(defaultListRaw), defaultListRaw, "list route, no header")
	})

	It("renders the exact amount as a quoted decimal string when the client opts in", func() {
		expectStringWire(detailAmount(optInDetailRaw), optInDetailRaw, "detail route, opt-in")
		expectStringWire(listAmount(optInListRaw), optInListRaw, "list route, opt-in")
	})

	It("agrees between the detail route and the list route within each mode", func() {
		// Compare the amount values, never whole bodies: the detail route writes
		// through the ConfigStd encoder (HTML escaping, trailing newline) while
		// the list route writes through ConfigDefault (neither). Diffing bodies
		// across the two routes, or trimming them first, would mask exactly the
		// encoder swap that internal/adapter/http/encoder_contract_test.go
		// exists to catch.
		defaultDetail := expectNumericWire(detailAmount(defaultDetailRaw), defaultDetailRaw, "detail route, no header")
		defaultList := expectNumericWire(listAmount(defaultListRaw), defaultListRaw, "list route, no header")
		Expect(defaultList).To(Equal(defaultDetail), "the two routes disagree on the default amount wire")

		optInDetail := expectStringWire(detailAmount(optInDetailRaw), optInDetailRaw, "detail route, opt-in")
		optInList := expectStringWire(listAmount(optInListRaw), optInListRaw, "list route, opt-in")
		Expect(optInList).To(Equal(optInDetail), "the two routes disagree on the opted-in amount wire")
	})

	It("carries the same value in both modes, differing only in the quoting", func() {
		Expect(expectStringWire(detailAmount(optInDetailRaw), optInDetailRaw, "detail route, opt-in")).
			To(Equal(expectNumericWire(detailAmount(defaultDetailRaw), defaultDetailRaw, "detail route, no header")),
				"the opt-in header must change the quoting of the amount, not its value")

		Expect(expectStringWire(listAmount(optInListRaw), optInListRaw, "list route, opt-in")).
			To(Equal(expectNumericWire(listAmount(defaultListRaw), defaultListRaw, "list route, no header")),
				"the opt-in header must change the quoting of the amount, not its value")
	})

	It("keeps the default numeric wire for a non-truthy header value", func() {
		// This is the assertion an existing client depends on, and the one that
		// catches a boundary helper reading "header present" as "opted in".
		nonTruthyDetail := expectNumericWire(detailAmount(nonTruthyDetailRaw), nonTruthyDetailRaw, "detail route, header=false")
		nonTruthyList := expectNumericWire(listAmount(nonTruthyListRaw), nonTruthyListRaw, "list route, header=false")

		Expect(nonTruthyDetail).To(Equal(expectNumericWire(detailAmount(defaultDetailRaw), defaultDetailRaw, "detail route, no header")),
			"a non-truthy header must serve the same wire as no header at all")
		Expect(nonTruthyList).To(Equal(expectNumericWire(listAmount(defaultListRaw), defaultListRaw, "list route, no header")),
			"a non-truthy header must serve the same wire as no header at all")
	})

	It("accepts a truthy header value that needs trimming and case folding", func() {
		expectStringWire(detailAmount(paddedDetailRaw), paddedDetailRaw, `detail route, header="  YES  "`)
	})
})
