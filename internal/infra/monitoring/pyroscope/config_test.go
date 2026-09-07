//go:build pyroscope

package pyroscope

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/grafana/pyroscope-go"
	"github.com/grafana/pyroscope-go/upstream"
	"github.com/grafana/pyroscope-go/upstream/remote"
	"github.com/stretchr/testify/require"
)

func TestPyroscopeConfigAuthentication(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name          string
		token         string
		user          string
		password      string
		authorization string
	}{
		{name: "anonymous"},
		{name: "bearer", token: "test-token", authorization: "Bearer test-token"},
		{name: "basic", user: "user", password: "pass", authorization: "Basic dXNlcjpwYXNz"},
		{name: "basic takes precedence", token: "test-token", user: "user", password: "pass", authorization: "Basic dXNlcjpwYXNz"},
		{name: "user without password", token: "test-token", user: "user", authorization: "Bearer test-token"},
		{name: "password without user", token: "test-token", password: "pass", authorization: "Bearer test-token"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			headers := make(chan http.Header, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				headers <- r.Header.Clone()
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(server.Close)
			client := server.Client()
			client.Timeout = 5 * time.Second

			config := DefaultConfig()
			config.ServerAddress = server.URL
			config.AuthToken = test.token
			config.BasicAuthUser = test.user
			config.BasicAuthPassword = test.password
			config.TenantID = "test-tenant"
			sdkConfig := config.PyroscopeConfig()
			require.Equal(t, config.BasicAuthUser, sdkConfig.BasicAuthUser)
			require.Equal(t, config.BasicAuthPassword, sdkConfig.BasicAuthPassword)
			require.Equal(t, config.ServerAddress, sdkConfig.ServerAddress)
			// Use the SDK's uploader so the assertion observes the actual HTTP
			// headers after its Basic Auth and custom-header processing.
			uploader, err := remote.NewRemote(remote.Config{
				Address:           sdkConfig.ServerAddress,
				BasicAuthUser:     sdkConfig.BasicAuthUser,
				BasicAuthPassword: sdkConfig.BasicAuthPassword,
				TenantID:          sdkConfig.TenantID,
				HTTPHeaders:       sdkConfig.HTTPHeaders,
				HTTPClient:        client,
				Threads:           1,
				Logger:            pyroscope.StandardLogger,
			})
			require.NoError(t, err)
			uploader.Start()
			t.Cleanup(uploader.Stop)
			uploader.Upload(&upstream.UploadJob{
				Name:       "ledger",
				StartTime:  time.Unix(1, 0),
				EndTime:    time.Unix(2, 0),
				SpyName:    "gospy",
				SampleRate: 100,
				Format:     upstream.FormatPprof,
			})
			uploader.Flush()

			select {
			case requestHeaders := <-headers:
				require.Equal(t, test.authorization, requestHeaders.Get("Authorization"))
				require.Equal(t, "test-tenant", requestHeaders.Get("X-Scope-Orgid"))
			default:
				t.Fatal("profile upload completed without reaching the HTTP server")
			}
		})
	}
}
