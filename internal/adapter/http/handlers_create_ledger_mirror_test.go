package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

func TestHandleCreateLedger_MirrorModeHTTP(t *testing.T) {
	t.Parallel()

	var capturedReq *servicepb.Request

	backend := &mockBackend{
		applyFn: func(_ context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
			capturedReq = requests[0]

			return []*commonpb.Log{{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_CreateLedger{
						CreateLedger: &commonpb.CreateLedgerLog{
							Name: "mirror-ledger",
							Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
						},
					},
				},
			}}, nil
		},
	}
	srv := newTestServer(t, backend)

	body := `{"mode":"MIRROR","mirrorSource":{"ledgerName":"default","type":"http","baseUrl":"http://v2:3068","oauth2ClientId":"my-id","oauth2ClientSecret":"my-secret","oauth2TokenEndpoint":"https://auth.example.com/token","oauth2Scopes":["ledger:read"]}}`
	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/mirror-ledger", strings.NewReader(body), map[string]string{
		"ledgerName": "mirror-ledger",
	})
	r.Header.Set("Content-Type", "application/json")

	srv.handleCreateLedger(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
	require.NotNil(t, capturedReq)

	createReq := capturedReq.GetCreateLedger()
	require.Equal(t, commonpb.LedgerMode_LEDGER_MODE_MIRROR, createReq.GetMode())
	require.NotNil(t, createReq.GetMirrorSource())
	require.Equal(t, "default", createReq.GetMirrorSource().GetLedgerName())

	httpCfg := createReq.GetMirrorSource().GetHttp()
	require.NotNil(t, httpCfg)
	require.Equal(t, "http://v2:3068", httpCfg.GetBaseUrl())
	require.NotNil(t, httpCfg.GetOauth2ClientCredentials())
	require.Equal(t, "my-id", httpCfg.GetOauth2ClientCredentials().GetClientId())
	require.Equal(t, "my-secret", httpCfg.GetOauth2ClientCredentials().GetClientSecret())
	require.Equal(t, "https://auth.example.com/token", httpCfg.GetOauth2ClientCredentials().GetTokenEndpoint())
	require.Equal(t, []string{"ledger:read"}, httpCfg.GetOauth2ClientCredentials().GetScopes())
}

func TestHandleCreateLedger_MirrorModePostgres(t *testing.T) {
	t.Parallel()

	var capturedReq *servicepb.Request

	backend := &mockBackend{
		applyFn: func(_ context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
			capturedReq = requests[0]

			return []*commonpb.Log{{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_CreateLedger{
						CreateLedger: &commonpb.CreateLedgerLog{
							Name: "mirror-pg",
							Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
						},
					},
				},
			}}, nil
		},
	}
	srv := newTestServer(t, backend)

	body := `{"mode":"MIRROR","mirrorSource":{"ledgerName":"default","type":"postgres","dsn":"postgres://user:pass@host:5432/ledger"}}`
	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/mirror-pg", strings.NewReader(body), map[string]string{
		"ledgerName": "mirror-pg",
	})
	r.Header.Set("Content-Type", "application/json")

	srv.handleCreateLedger(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
	require.NotNil(t, capturedReq)

	createReq := capturedReq.GetCreateLedger()
	require.Equal(t, commonpb.LedgerMode_LEDGER_MODE_MIRROR, createReq.GetMode())

	pgCfg := createReq.GetMirrorSource().GetPostgres()
	require.NotNil(t, pgCfg)
	require.Equal(t, "postgres://user:pass@host:5432/ledger", pgCfg.GetDsn())
}

func TestHandleCreateLedger_MirrorModeDefaultType(t *testing.T) {
	t.Parallel()

	var capturedReq *servicepb.Request

	backend := &mockBackend{
		applyFn: func(_ context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
			capturedReq = requests[0]

			return []*commonpb.Log{{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_CreateLedger{
						CreateLedger: &commonpb.CreateLedgerLog{
							Name: "mirror-default",
						},
					},
				},
			}}, nil
		},
	}
	srv := newTestServer(t, backend)

	// No "type" field → should default to HTTP
	body := `{"mode":"MIRROR","mirrorSource":{"ledgerName":"default","baseUrl":"http://v2:3068"}}`
	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/mirror-default", strings.NewReader(body), map[string]string{
		"ledgerName": "mirror-default",
	})
	r.Header.Set("Content-Type", "application/json")

	srv.handleCreateLedger(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
	require.NotNil(t, capturedReq.GetCreateLedger().GetMirrorSource().GetHttp())
}

func TestHandleCreateLedger_MirrorModeUnsupportedType(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	body := `{"mode":"MIRROR","mirrorSource":{"ledgerName":"default","type":"s3"}}`
	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/mirror-bad", strings.NewReader(body), map[string]string{
		"ledgerName": "mirror-bad",
	})
	r.Header.Set("Content-Type", "application/json")

	srv.handleCreateLedger(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestMirrorSourceToProto_HTTP(t *testing.T) {
	t.Parallel()

	body := &mirrorSourceBody{
		LedgerName:          "src-ledger",
		Type:                "http",
		BaseURL:             "http://localhost:3068",
		OAuth2ClientID:      "my-client-id",
		OAuth2ClientSecret:  "my-client-secret",
		OAuth2TokenEndpoint: "https://auth.example.com/token",
		OAuth2Scopes:        []string{"ledger:read"},
	}

	cfg, err := mirrorSourceToProto(body)
	require.NoError(t, err)
	require.Equal(t, "src-ledger", cfg.GetLedgerName())

	httpCfg := cfg.GetHttp()
	require.NotNil(t, httpCfg)
	require.Equal(t, "http://localhost:3068", httpCfg.GetBaseUrl())
	require.NotNil(t, httpCfg.GetOauth2ClientCredentials())
	require.Equal(t, "my-client-id", httpCfg.GetOauth2ClientCredentials().GetClientId())
	require.Equal(t, "my-client-secret", httpCfg.GetOauth2ClientCredentials().GetClientSecret())
	require.Equal(t, "https://auth.example.com/token", httpCfg.GetOauth2ClientCredentials().GetTokenEndpoint())
	require.Equal(t, []string{"ledger:read"}, httpCfg.GetOauth2ClientCredentials().GetScopes())
}

func TestMirrorSourceToProto_Postgres(t *testing.T) {
	t.Parallel()

	body := &mirrorSourceBody{
		LedgerName: "src-ledger",
		Type:       "postgres",
		DSN:        "postgres://user:pass@host/db",
	}

	cfg, err := mirrorSourceToProto(body)
	require.NoError(t, err)
	require.Equal(t, "src-ledger", cfg.GetLedgerName())

	pgCfg := cfg.GetPostgres()
	require.NotNil(t, pgCfg)
	require.Equal(t, "postgres://user:pass@host/db", pgCfg.GetDsn())
}

func TestMirrorSourceToProto_EmptyType(t *testing.T) {
	t.Parallel()

	body := &mirrorSourceBody{
		LedgerName: "src-ledger",
		Type:       "", // defaults to "http"
		BaseURL:    "http://localhost:3068",
	}

	cfg, err := mirrorSourceToProto(body)
	require.NoError(t, err)

	httpCfg := cfg.GetHttp()
	require.NotNil(t, httpCfg)
	require.Nil(t, httpCfg.GetOauth2ClientCredentials())
}

func TestMirrorSourceToProto_Unsupported(t *testing.T) {
	t.Parallel()

	body := &mirrorSourceBody{
		LedgerName: "src-ledger",
		Type:       "kafka",
	}

	_, err := mirrorSourceToProto(body)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported")
}
