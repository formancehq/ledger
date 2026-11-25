//go:build e2e

package business

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("EN-3286 HTTP account-type retries", func() {
	It("replays add and remove without reapplying, and rejects changed payload reuse", func() {
		ctx, node := testutil.SetupSingleNode()
		ledger := "en-3286-account-types"
		_, err := node.Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil)))
		Expect(err).To(Succeed())
		client := &http.Client{Timeout: 10 * time.Second}
		request := func(method, body, key string) (int, []byte) {
			GinkgoHelper()
			path := fmt.Sprintf("http://localhost:%d/v3/%s/account-types", node.HTTPPort, ledger)
			if method == http.MethodDelete {
				path += "/users"
			}
			req, err := http.NewRequestWithContext(ctx, method, path, bytes.NewBufferString(body))
			Expect(err).To(Succeed())
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", key)
			resp, err := client.Do(req)
			Expect(err).To(Succeed())
			defer func() { Expect(resp.Body.Close()).To(Succeed()) }()
			raw, err := io.ReadAll(resp.Body)
			Expect(err).To(Succeed())
			return resp.StatusCode, raw
		}
		add := `{"name":"users","pattern":"users:{id}"}`
		code, raw := request(http.MethodPost, add, "en-3286-add")
		Expect(code).To(Equal(http.StatusCreated), string(raw))
		code, raw = request(http.MethodPost, add, "en-3286-add")
		Expect(code).To(Equal(http.StatusCreated), string(raw))
		code, raw = request(http.MethodPost, `{"name":"users","pattern":"users:{id}:other"}`, "en-3286-add")
		Expect(code).To(Equal(http.StatusConflict), string(raw))
		var failure struct {
			ErrorCode string `json:"errorCode"`
		}
		Expect(json.Unmarshal(raw, &failure)).To(Succeed())
		Expect(failure.ErrorCode).To(Equal("IDEMPOTENCY_KEY_CONFLICT"))

		code, raw = request(http.MethodDelete, "", "en-3286-remove")
		Expect(code).To(Equal(http.StatusNoContent), string(raw))
		code, raw = request(http.MethodPost, add, "en-3286-add")
		Expect(code).To(Equal(http.StatusCreated), string(raw))
		// The keyed add replay did not resurrect the removed account type.
		code, raw = request(http.MethodPost, add, "en-3286-recreate")
		Expect(code).To(Equal(http.StatusCreated), string(raw))
		code, raw = request(http.MethodDelete, "", "en-3286-remove")
		Expect(code).To(Equal(http.StatusNoContent), string(raw))
		// The keyed remove replay did not remove the replacement.
		code, raw = request(http.MethodDelete, "", "en-3286-final-remove")
		Expect(code).To(Equal(http.StatusNoContent), string(raw))
	})
})
