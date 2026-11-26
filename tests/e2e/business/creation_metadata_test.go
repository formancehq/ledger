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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("Atomic creation metadata (EN-2686)", func() {
	create := func(name, body string) (int, string) {
		req, err := http.NewRequestWithContext(sharedCtx, http.MethodPost,
			fmt.Sprintf("http://localhost:%d/v3/%s", sharedHTTPPort, name), bytes.NewBufferString(body))
		Expect(err).To(Succeed())
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		Expect(err).To(Succeed())
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		Expect(err).To(Succeed())
		return resp.StatusCode, string(raw)
	}

	It("persists typed metadata through the real HTTP and admission path", func() {
		code, body := create("creation-metadata-typed", `{"metadata":{"owner":"team","count":9007199254740993,"enabled":true,"omitted":null},"initialSchema":[{"targetType":"ledger","key":"owner","type":"int64"}]}`)
		Expect(code).To(Equal(http.StatusCreated), body)
		Expect(body).To(ContainSubstring(`"count":9007199254740993`))
		Expect(body).To(ContainSubstring(`"owner":"team"`))
		ledger, err := sharedClient.GetLedger(sharedCtx, &servicepb.GetLedgerRequest{Ledger: "creation-metadata-typed"})
		Expect(err).To(Succeed())
		Expect(ledger.GetMetadata()).To(HaveLen(3))
		Expect(ledger.GetMetadata()["owner"].GetStringValue()).To(Equal("team"))
		Expect(ledger.GetMetadata()["count"].GetUintValue()).To(Equal(uint64(9007199254740993)))
		Expect(ledger.GetMetadata()["enabled"].GetBoolValue()).To(BeTrue())
	})

	It("preserves the Go creation helper metadata in one creation log", func() {
		result, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
			actions.CreateLedgerAction("creation-metadata-go", map[string]string{"owner": "go-client"})))
		Expect(err).To(Succeed())
		Expect(result.GetLogs()).To(HaveLen(1))
		Expect(result.GetLogs()[0].GetPayload().GetCreateLedger().GetMetadata()["owner"].GetStringValue()).To(Equal("go-client"))
		ledger, err := sharedClient.GetLedger(sharedCtx, &servicepb.GetLedgerRequest{Ledger: "creation-metadata-go"})
		Expect(err).To(Succeed())
		Expect(ledger.GetMetadata()["owner"].GetStringValue()).To(Equal("go-client"))
	})

	DescribeTable("rejects invalid metadata without creating the ledger", func(suffix, body string) {
		name := "creation-metadata-invalid-" + suffix
		code, raw := create(name, body)
		Expect(code).To(Equal(http.StatusBadRequest), raw)
		_, err := sharedClient.GetLedger(sharedCtx, &servicepb.GetLedgerRequest{Ledger: name})
		Expect(status.Code(err)).To(Equal(codes.NotFound))
	},
		Entry("object", "object", `{"metadata":{"key":{"nested":"value"}}}`),
		Entry("array", "array", `{"metadata":{"key":[1]}}`),
		Entry("fraction", "fraction", `{"metadata":{"key":1.5}}`),
		Entry("key", "key", `{"metadata":{"bad%key":"value"}}`),
		Entry("NUL", "nul", `{"metadata":{"key":"bad\u0000value"}}`),
	)
})
