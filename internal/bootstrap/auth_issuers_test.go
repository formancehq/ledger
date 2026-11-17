package bootstrap

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestBuildAuthConfig_DiscoversEveryTrustedIssuer(t *testing.T) {
	t.Parallel()

	discoveryServer := func(hits *atomic.Int32) *httptest.Server {
		mux := http.NewServeMux()
		srv := httptest.NewServer(mux)
		mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]string{"issuer": srv.URL, "jwks_uri": srv.URL + "/jwks"})
		})
		t.Cleanup(srv.Close)

		return srv
	}

	var hitsA, hitsB atomic.Int32
	issuerA := discoveryServer(&hitsA)
	issuerB := discoveryServer(&hitsB)

	authCfg, err := buildAuthConfig(Config{
		AuthConfig: AuthFlagConfig{
			Enabled:              true,
			Issuer:               issuerA.URL,
			Issuers:              []string{issuerB.URL, issuerA.URL},
			OIDCDiscoveryTimeout: time.Second,
		},
	}, logging.Testing(), nil)
	require.NoError(t, err)

	require.Equal(t, []string{issuerA.URL, issuerB.URL}, authCfg.Issuers)
	require.NotNil(t, authCfg.KeySet)
	require.Equal(t, int32(1), hitsA.Load())
	require.Equal(t, int32(1), hitsB.Load())
}
