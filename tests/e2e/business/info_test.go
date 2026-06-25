//go:build e2e

package business

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Server info", func() {
	// The gRPC client (sharedClient) and HTTP server are brought up without
	// auth credentials by the suite's SetupSingleNode, so both Discovery and
	// /_info are exercised exactly as an unauthenticated caller would hit them.
	//
	// Test-built servers carry no ldflags, so version reports "dev" and build
	// date "unknown". Assertions therefore only check presence/non-emptiness
	// and key presence, never a specific version string.

	It("Should expose server info via the unauthenticated Discovery RPC", func() {
		resp, err := sharedClient.Discovery(sharedCtx, &servicepb.DiscoveryRequest{})
		Expect(err).To(Succeed())
		Expect(resp).NotTo(BeNil())

		info := resp.GetServerInfo()
		Expect(info).NotTo(BeNil(), "expected Discovery to populate server_info")
		Expect(info.GetVersion()).NotTo(BeEmpty(), "expected a non-empty version")
		Expect(info.GetGoVersion()).NotTo(BeEmpty(), "expected a non-empty Go version")
	})

	It("Should expose server info via the unauthenticated HTTP GET /_info", func() {
		url := fmt.Sprintf("http://localhost:%d/_info", testutil.TestSingleHTTPPort)
		req, err := http.NewRequestWithContext(sharedCtx, http.MethodGet, url, nil)
		Expect(err).To(Succeed())

		// No Authorization header is set: /_info must be reachable without auth.
		resp, err := http.DefaultClient.Do(req)
		Expect(err).To(Succeed())
		defer func() { _ = resp.Body.Close() }()

		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		Expect(resp.Header.Get("Content-Type")).To(ContainSubstring("application/json"))

		// The payload is flat camelCase JSON, NOT wrapped in a data envelope.
		var body map[string]any
		Expect(json.NewDecoder(resp.Body).Decode(&body)).To(Succeed())

		Expect(body).To(HaveKey("version"))
		Expect(body).To(HaveKey("commit"))
		Expect(body).To(HaveKey("buildDate"))
		Expect(body).To(HaveKey("goVersion"))

		Expect(body["version"]).To(BeAssignableToTypeOf(""))
		Expect(body["version"].(string)).NotTo(BeEmpty(), "expected a non-empty version")
		Expect(body["goVersion"]).To(BeAssignableToTypeOf(""))
		Expect(body["goVersion"].(string)).NotTo(BeEmpty(), "expected a non-empty Go version")
	})
})
