//go:build it

package test_suite

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"

	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/oidc"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	. "github.com/formancehq/ledger/pkg/testserver/ginkgo"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Regression test for TS-456.
//
// `auth.Module` (when --auth-enabled) and `publish.httpModule` (when
// --publisher-http-enabled) both call fx.Supply(http.DefaultClient) at the
// parent fx scope. uber/fx rejects the duplicate provider and `ledger serve`
// fails to start with:
//
//	cannot provide *http.Client from [0]: already provided by "reflect".makeFuncStub
var _ = Context("Ledger startup with --auth-enabled and --publisher-http-enabled (TS-456)", func() {
	oidcServer := newFakeOIDCServer()

	db := UseTemplatedDatabase()
	connectionOptions := DeferMap(db, func(from *pgtesting.Database) bunconnect.ConnectionOptions {
		return bunconnect.ConnectionOptions{
			DatabaseSourceName: from.ConnectionOptions().DatabaseSourceName,
			MaxOpenConns:       100,
		}
	})

	testServer := DeferTestServer(
		connectionOptions,
		testservice.WithInstruments(
			testservice.AppendArgsInstrumentation(
				"--auth-enabled",
				"--auth-issuers", oidcServer.URL,
				"--publisher-http-enabled",
				"--publisher-topic-mapping", "*:"+oidcServer.URL,
			),
			testservice.DebugInstrumentation(debug),
			testservice.OutputInstrumentation(GinkgoWriter),
		),
		testservice.WithLogger(GinkgoT()),
	)

	It("should boot without a duplicate *http.Client fx provider", func(specContext SpecContext) {
		_, err := testServer.Wait(specContext)
		Expect(err).To(BeNil())
	})
})

// newFakeOIDCServer stands up a tiny httptest server that answers the OIDC
// discovery + JWKS calls auth.Module makes at startup. It is started here at
// package load time so the URL is available when DeferTestServer captures its
// CLI args; tests that don't reference this Context simply ignore it.
func newFakeOIDCServer() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(oidc.DiscoveryConfiguration{
			Issuer:  scheme + "://" + r.Host,
			JwksURI: scheme + "://" + r.Host + "/.well-known/jwks.json",
		})
	})
	mux.HandleFunc("/.well-known/jwks.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"keys":[]}`))
	})
	return httptest.NewServer(mux)
}
