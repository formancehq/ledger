//go:build e2e

package business

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Ledger KV", Ordered, func() {
	const ledger = "kv-e2e"
	const other = "kv-e2e-other"
	const key = "binary"
	value := []byte{0, 1, 2, 255, 0}

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil), actions.CreateLedgerAction(other, nil)))
		Expect(err).To(Succeed())
	})

	request := func(method string, body []byte, expected int) []byte {
		GinkgoHelper()
		req, err := http.NewRequestWithContext(sharedCtx, method,
			fmt.Sprintf("http://localhost:%d/v3/%s/kv/%s", sharedHTTPPort, ledger, key), bytes.NewReader(body))
		Expect(err).To(Succeed())
		if method == http.MethodPut {
			req.Header.Set("Content-Type", "application/octet-stream")
		}
		resp, err := http.DefaultClient.Do(req)
		Expect(err).To(Succeed())
		defer func() { Expect(resp.Body.Close()).To(Succeed()) }()
		payload, err := io.ReadAll(resp.Body)
		Expect(err).To(Succeed())
		Expect(resp.StatusCode).To(Equal(expected), string(payload))
		return payload
	}

	It("puts, overwrites, isolates, deletes and audits opaque values", func() {
		request(http.MethodPut, value, http.StatusNoContent)
		got, err := sharedClient.GetKV(sharedCtx, &servicepb.GetKVRequest{Ledger: ledger, Key: key})
		Expect(err).To(Succeed())
		Expect(got.GetValue()).To(Equal(value))
		Expect(request(http.MethodGet, nil, http.StatusOK)).To(Equal(value))
		_, err = sharedClient.GetKV(sharedCtx, &servicepb.GetKVRequest{Ledger: other, Key: key})
		Expect(err).To(HaveOccurred())

		replacement := []byte("replacement")
		request(http.MethodPut, replacement, http.StatusNoContent)
		Expect(request(http.MethodGet, nil, http.StatusOK)).To(Equal(replacement))
		request(http.MethodDelete, nil, http.StatusNoContent)
		request(http.MethodDelete, nil, http.StatusNoContent)
		request(http.MethodGet, nil, http.StatusNotFound)
		expectStoreValid(sharedCtx, sharedClient)
	})

	It("replays one idempotent outcome and rejects conflicting content", func() {
		request := &servicepb.Request{Type: &servicepb.Request_PutKv{PutKv: &servicepb.PutKVRequest{
			Ledger: ledger, Key: "idempotent", Value: []byte("first"),
		}}}
		first, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("kv-e2e-idempotent", request))
		Expect(err).To(Succeed())
		replayed, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("kv-e2e-idempotent", request))
		Expect(err).To(Succeed())
		Expect(replayed.GetLogs()).To(HaveLen(1))
		Expect(replayed.GetLogs()[0].GetSequence()).To(Equal(first.GetLogs()[0].GetSequence()))
		conflicting := &servicepb.Request{Type: &servicepb.Request_PutKv{PutKv: &servicepb.PutKVRequest{
			Ledger: ledger, Key: "idempotent", Value: []byte("second"),
		}}}
		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("kv-e2e-idempotent", conflicting))
		Expect(err).To(HaveOccurred())
		value, err := sharedClient.GetKV(sharedCtx, &servicepb.GetKVRequest{Ledger: ledger, Key: "idempotent"})
		Expect(err).To(Succeed())
		Expect(value.GetValue()).To(Equal([]byte("first")))
		expectStoreValid(sharedCtx, sharedClient)
	})
})
