package test_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/formancehq/webhooks/cmd/flag"
	webhooks "github.com/formancehq/webhooks/pkg"
	"github.com/formancehq/webhooks/pkg/server"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func TestServer(t *testing.T) {
	// New test server with success handler
	httpServerSuccess := httptest.NewServer(http.HandlerFunc(webhooksSuccessHandler))
	defer func() {
		httpServerSuccess.CloseClientConnections()
		httpServerSuccess.Close()
	}()

	serverApp := fxtest.New(t,
		fx.Supply(httpServerSuccess.Client()),
		server.StartModule(
			viper.GetString(flag.HttpBindAddressServer)))

	t.Run("start", func(t *testing.T) {
		serverApp.RequireStart()
	})

	t.Run("health check", func(t *testing.T) {
		requestServer(t, http.MethodGet, server.PathHealthCheck, http.StatusOK)
	})

	t.Run("clean existing configs", func(t *testing.T) {
		resBody := requestServer(t, http.MethodGet, server.PathConfigs, http.StatusOK)
		cur := decodeCursorResponse[webhooks.Config](t, resBody)
		for _, cfg := range cur.Data {
			requestServer(t, http.MethodDelete, server.PathConfigs+"/"+cfg.ID, http.StatusOK)
		}
		require.NoError(t, resBody.Close())

		resBody = requestServer(t, http.MethodGet, server.PathConfigs, http.StatusOK)
		cur = decodeCursorResponse[webhooks.Config](t, resBody)
		assert.Equal(t, 0, len(cur.Data))
		require.NoError(t, resBody.Close())
	})

	validConfigs := []webhooks.ConfigUser{
		{
			Endpoint:   "https://www.site1.com",
			EventTypes: []string{"TYPE1", "TYPE2"},
		},
		{
			Endpoint:   "https://www.site2.com",
			EventTypes: []string{"TYPE3"},
		},
		{
			Endpoint:   "https://www.site3.com",
			Secret:     webhooks.NewSecret(),
			EventTypes: []string{"TYPE1"},
		},
	}

	insertedIds := make([]string, len(validConfigs))

	t.Run("POST "+server.PathConfigs, func(t *testing.T) {
		t.Run("valid", func(t *testing.T) {
			for i, cfg := range validConfigs {
				resBody := requestServer(t, http.MethodPost, server.PathConfigs, http.StatusOK, cfg)
				c, ok := decodeSingleResponse[webhooks.Config](t, resBody)
				assert.Equal(t, true, ok)
				insertedIds[i] = c.ID
				require.NoError(t, resBody.Close())
			}
		})

		t.Run("invalid Content-Type", func(t *testing.T) {
			req, err := http.NewRequestWithContext(context.Background(),
				http.MethodPost, serverBaseURL+server.PathConfigs,
				buffer(t, validConfigs[0]))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "invalid")
			resp, err := httpClient.Do(req)
			require.NoError(t, err)
			assert.Equal(t, http.StatusUnsupportedMediaType, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
		})

		t.Run("invalid nil body", func(t *testing.T) {
			requestServer(t, http.MethodPost, server.PathConfigs, http.StatusBadRequest)
		})

		t.Run("invalid body not json", func(t *testing.T) {
			requestServer(t, http.MethodPost, server.PathConfigs, http.StatusBadRequest, []byte("{"))
		})

		t.Run("invalid body unknown field", func(t *testing.T) {
			requestServer(t, http.MethodPost, server.PathConfigs, http.StatusBadRequest, []byte("{\"field\":false}"))
		})

		t.Run("invalid body json syntax", func(t *testing.T) {
			requestServer(t, http.MethodPost, server.PathConfigs, http.StatusBadRequest, []byte("{\"endpoint\":\"example.com\",}"))
		})
	})

	t.Run("GET "+server.PathConfigs, func(t *testing.T) {
		resBody := requestServer(t, http.MethodGet, server.PathConfigs, http.StatusOK)
		cur := decodeCursorResponse[webhooks.Config](t, resBody)
		assert.Equal(t, len(validConfigs), len(cur.Data))
		for i, cfg := range validConfigs {
			assert.Equal(t, cfg.Endpoint, cur.Data[len(validConfigs)-i-1].Endpoint)
			assert.Equal(t, len(cfg.EventTypes), len(cur.Data[len(validConfigs)-i-1].EventTypes))
			for j, typ := range cfg.EventTypes {
				assert.Equal(t,
					strings.ToLower(typ),
					strings.ToLower(cur.Data[len(validConfigs)-i-1].EventTypes[j]))
			}
		}
		require.NoError(t, resBody.Close())

		cfg := validConfigs[0]
		ep := url.QueryEscape(cfg.Endpoint)
		resBody = requestServer(t, http.MethodGet, server.PathConfigs+"?endpoint="+ep, http.StatusOK)
		cur = decodeCursorResponse[webhooks.Config](t, resBody)
		assert.Equal(t, 1, len(cur.Data))
		assert.Equal(t, cfg.Endpoint, cur.Data[0].Endpoint)
		require.NoError(t, resBody.Close())

		resBody = requestServer(t, http.MethodGet, server.PathConfigs+"?id="+insertedIds[0], http.StatusOK)
		cur = decodeCursorResponse[webhooks.Config](t, resBody)
		assert.Equal(t, 1, len(cur.Data))
		assert.Equal(t, cfg.Endpoint, cur.Data[0].Endpoint)
		require.NoError(t, resBody.Close())
	})

	t.Run("PUT "+server.PathConfigs, func(t *testing.T) {
		t.Run(server.PathDeactivate, func(t *testing.T) {
			resBody := requestServer(t, http.MethodPut, server.PathConfigs+"/"+insertedIds[0]+server.PathDeactivate, http.StatusOK)
			c, ok := decodeSingleResponse[webhooks.Config](t, resBody)
			assert.Equal(t, true, ok)
			assert.Equal(t, false, c.Active)
			require.NoError(t, resBody.Close())

			resBody = requestServer(t, http.MethodGet, server.PathConfigs, http.StatusOK)
			cur := decodeCursorResponse[webhooks.Config](t, resBody)
			assert.Equal(t, len(validConfigs), len(cur.Data))
			assert.Equal(t, false, cur.Data[0].Active)
			require.NoError(t, resBody.Close())

			requestServer(t, http.MethodPut, server.PathConfigs+"/"+insertedIds[0]+server.PathDeactivate, http.StatusNotModified)
		})

		t.Run(server.PathActivate, func(t *testing.T) {
			resBody := requestServer(t, http.MethodPut, server.PathConfigs+"/"+insertedIds[0]+server.PathActivate, http.StatusOK)
			c, ok := decodeSingleResponse[webhooks.Config](t, resBody)
			assert.Equal(t, true, ok)
			assert.Equal(t, true, c.Active)
			require.NoError(t, resBody.Close())

			resBody = requestServer(t, http.MethodGet, server.PathConfigs, http.StatusOK)
			cur := decodeCursorResponse[webhooks.Config](t, resBody)
			assert.Equal(t, len(validConfigs), len(cur.Data))
			assert.Equal(t, true, cur.Data[len(cur.Data)-1].Active)
			require.NoError(t, resBody.Close())

			requestServer(t, http.MethodPut, server.PathConfigs+"/"+insertedIds[0]+server.PathActivate, http.StatusNotModified)
		})

		t.Run(server.PathChangeSecret, func(t *testing.T) {
			resBody := requestServer(t, http.MethodPut, server.PathConfigs+"/"+insertedIds[0]+server.PathChangeSecret, http.StatusOK)
			c, ok := decodeSingleResponse[webhooks.Config](t, resBody)
			assert.Equal(t, true, ok)
			assert.NotEqual(t, "", c.Secret)
			require.NoError(t, resBody.Close())

			validSecret := webhooks.Secret{Secret: webhooks.NewSecret()}
			resBody = requestServer(t, http.MethodPut, server.PathConfigs+"/"+insertedIds[0]+server.PathChangeSecret, http.StatusOK, validSecret)
			c, ok = decodeSingleResponse[webhooks.Config](t, resBody)
			assert.Equal(t, true, ok)
			assert.Equal(t, validSecret.Secret, c.Secret)
			require.NoError(t, resBody.Close())

			invalidSecret := webhooks.Secret{Secret: "invalid"}
			requestServer(t, http.MethodPut, server.PathConfigs+"/"+insertedIds[0]+server.PathChangeSecret, http.StatusBadRequest, invalidSecret)

			invalidSecret2 := validConfigs[0]
			requestServer(t, http.MethodPut, server.PathConfigs+"/"+insertedIds[0]+server.PathChangeSecret, http.StatusBadRequest, invalidSecret2)
		})
	})

	t.Run("GET "+server.PathConfigs+"/{id}"+server.PathTest, func(t *testing.T) {
		resBody := requestServer(t, http.MethodPost, server.PathConfigs, http.StatusOK, webhooks.ConfigUser{
			Endpoint:   httpServerSuccess.URL,
			Secret:     secret,
			EventTypes: []string{"TYPE1"},
		})
		c, ok := decodeSingleResponse[webhooks.Config](t, resBody)
		assert.Equal(t, true, ok)
		require.NoError(t, resBody.Close())

		resBody = requestServer(t, http.MethodGet, server.PathConfigs+"/"+c.ID+server.PathTest, http.StatusOK)
		attempt, ok := decodeSingleResponse[webhooks.Attempt](t, resBody)
		assert.Equal(t, true, ok)
		assert.Equal(t, webhooks.StatusAttemptSuccess, attempt.Status)
		assert.Equal(t, `{"data":"test"}`, attempt.Payload)

		requestServer(t, http.MethodDelete, server.PathConfigs+"/"+c.ID, http.StatusOK)
	})

	t.Run("DELETE "+server.PathConfigs, func(t *testing.T) {
		for _, id := range insertedIds {
			requestServer(t, http.MethodDelete, server.PathConfigs+"/"+id, http.StatusOK)
			requestServer(t, http.MethodDelete, server.PathConfigs+"/"+id, http.StatusNotFound)
		}
	})

	t.Run("GET "+server.PathConfigs+" after delete", func(t *testing.T) {
		resBody := requestServer(t, http.MethodGet, server.PathConfigs, http.StatusOK)
		cur := decodeCursorResponse[webhooks.Config](t, resBody)
		assert.Equal(t, 0, len(cur.Data))
		require.NoError(t, resBody.Close())
	})

	t.Run("stop", func(t *testing.T) {
		serverApp.RequireStop()
	})
}
