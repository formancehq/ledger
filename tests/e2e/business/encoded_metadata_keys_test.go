//go:build e2e

package business

import (
	"bytes"
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

// EN-2015: exercise the real HTTP server and admission, including keys created
// in JSON before they are addressed as escaped URL segments. Percent-bearing
// keys are invalid; there is no valid percent-encoded sibling to mutate.
var _ = Describe("Encoded metadata keys (EN-2015)", func() {
	const key = "formance.com/reviewed"

	request := func(method, ledgerName, path, body string, wantStatus int) {
		GinkgoHelper()
		req, err := http.NewRequestWithContext(sharedCtx, method,
			fmt.Sprintf("http://localhost:%d/v3/%s%s", sharedHTTPPort, ledgerName, path), bytes.NewBufferString(body))
		Expect(err).To(Succeed())
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		Expect(err).To(Succeed())
		raw, err := io.ReadAll(resp.Body)
		Expect(err).To(Succeed())
		Expect(resp.Body.Close()).To(Succeed())
		Expect(resp.StatusCode).To(Equal(wantStatus), "%s %s: %s", method, path, raw)
	}

	DescribeTable("deletes a JSON-created namespaced metadata key", func(target string) {
		ledgerName := "encoded-meta-" + target
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())
		logs, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName,
			[]*commonpb.Posting{actions.NewPosting("world", "alice", big.NewInt(1), "USD")}, nil, nil)))
		Expect(err).To(Succeed())
		txID := logs.GetLogs()[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().GetId()

		path := "/metadata"
		switch target {
		case "account":
			path = "/accounts/alice/metadata"
		case "transaction":
			path = fmt.Sprintf("/transactions/%d/metadata", txID)
		}
		readMetadata := func(g Gomega) map[string]string {
			switch target {
			case "account":
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{Ledger: ledgerName, Address: "alice"})
				g.Expect(err).To(Succeed())
				return commonpb.MetadataToGoMap(account.GetMetadata())
			case "transaction":
				tx, err := sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{Ledger: ledgerName, TransactionId: txID})
				g.Expect(err).To(Succeed())
				return commonpb.MetadataToGoMap(tx.GetTransaction().GetMetadata())
			default:
				ledger, err := actions.GetLedger(sharedCtx, sharedClient, ledgerName)
				g.Expect(err).To(Succeed())
				return commonpb.MetadataToGoMap(ledger.GetMetadata())
			}
		}
		request(http.MethodPost, ledgerName, path, `{"formance.com/reviewed":"yes","keep":"untouched"}`, http.StatusNoContent)
		Eventually(func(g Gomega) {
			g.Expect(readMetadata(g)).To(HaveKeyWithValue(key, "yes"))
		}).Should(Succeed())

		// Both a literal percent and a double-encoded slash must remain invalid.
		// The latter is canonical URL escaping, so net/url leaves RawPath empty.
		for _, invalid := range []string{"formance.com%25reviewed", "formance.com%25ZZreviewed", "formance.com%252Freviewed"} {
			request(http.MethodDelete, ledgerName, path+"/"+invalid, "", http.StatusBadRequest)
			Eventually(func(g Gomega) {
				g.Expect(readMetadata(g)).To(HaveKeyWithValue(key, "yes"))
			}).Should(Succeed())
		}

		request(http.MethodDelete, ledgerName, path+"/formance.com%2Freviewed", "", http.StatusNoContent)
		Eventually(func(g Gomega) {
			metadata := readMetadata(g)
			g.Expect(metadata).NotTo(HaveKey(key))
			g.Expect(metadata).To(HaveKeyWithValue("keep", "untouched"))
		}).Should(Succeed())
	}, Entry("ledger", "ledger"), Entry("account", "account"), Entry("transaction", "transaction"))

	DescribeTable("addresses the decoded schema key on PUT and DELETE", func(target string) {
		ledgerName := "encoded-schema-" + target
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())
		readFields := func(g Gomega) map[string]*servicepb.MetadataFieldStatus {
			schema, err := sharedClient.GetMetadataSchemaStatus(sharedCtx, &servicepb.GetMetadataSchemaStatusRequest{Ledger: ledgerName})
			g.Expect(err).To(Succeed())
			switch target {
			case "account":
				return schema.GetAccountFields()
			case "transaction":
				return schema.GetTransactionFields()
			default:
				return schema.GetLedgerFields()
			}
		}
		path := "/metadata-schema/" + target + "/"
		request(http.MethodPut, ledgerName, path+"keep", `{"type":"string"}`, http.StatusNoContent)
		request(http.MethodPut, ledgerName, path+"formance.com%2Freviewed", `{"type":"string"}`, http.StatusNoContent)
		Eventually(func(g Gomega) {
			g.Expect(readFields(g)).To(HaveKey(key))
		}).Should(Succeed())

		for _, invalid := range []string{"formance.com%25reviewed", "formance.com%25ZZreviewed", "formance.com%252Freviewed"} {
			request(http.MethodPut, ledgerName, path+invalid, `{"type":"bool"}`, http.StatusBadRequest)
			request(http.MethodDelete, ledgerName, path+invalid, "", http.StatusBadRequest)
			Eventually(func(g Gomega) {
				fields := readFields(g)
				g.Expect(fields).To(HaveLen(2))
				g.Expect(fields).To(HaveKey(key))
				g.Expect(fields[key].GetDeclaredType()).To(Equal(commonpb.MetadataType_METADATA_TYPE_STRING))
			}).Should(Succeed())
		}

		request(http.MethodPut, ledgerName, path+"formance.com%2freviewed", `{"type":"bool"}`, http.StatusNoContent)
		Eventually(func(g Gomega) {
			fields := readFields(g)
			g.Expect(fields).To(HaveKey(key))
			g.Expect(fields[key].GetDeclaredType()).To(Equal(commonpb.MetadataType_METADATA_TYPE_BOOL))
		}).Should(Succeed())
		request(http.MethodDelete, ledgerName, path+"formance.com%2Freviewed", "", http.StatusNoContent)
		Eventually(func(g Gomega) {
			fields := readFields(g)
			g.Expect(fields).NotTo(HaveKey(key))
			g.Expect(fields).To(HaveKey("keep"))
		}).Should(Succeed())
	}, Entry("ledger", "ledger"), Entry("account", "account"), Entry("transaction", "transaction"))
})
