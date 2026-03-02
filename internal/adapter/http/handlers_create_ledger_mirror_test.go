package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/stretchr/testify/require"
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
							Info: &commonpb.LedgerInfo{
								Name: "mirror-ledger",
								Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
							},
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
	require.Equal(t, commonpb.LedgerMode_LEDGER_MODE_MIRROR, createReq.Mode)
	require.NotNil(t, createReq.MirrorSource)
	require.Equal(t, "default", createReq.MirrorSource.LedgerName)

	httpCfg := createReq.MirrorSource.GetHttp()
	require.NotNil(t, httpCfg)
	require.Equal(t, "http://v2:3068", httpCfg.BaseUrl)
	require.NotNil(t, httpCfg.Oauth2ClientCredentials)
	require.Equal(t, "my-id", httpCfg.Oauth2ClientCredentials.ClientId)
	require.Equal(t, "my-secret", httpCfg.Oauth2ClientCredentials.ClientSecret)
	require.Equal(t, "https://auth.example.com/token", httpCfg.Oauth2ClientCredentials.TokenEndpoint)
	require.Equal(t, []string{"ledger:read"}, httpCfg.Oauth2ClientCredentials.Scopes)
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
							Info: &commonpb.LedgerInfo{
								Name: "mirror-pg",
								Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
							},
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
	require.Equal(t, commonpb.LedgerMode_LEDGER_MODE_MIRROR, createReq.Mode)

	pgCfg := createReq.MirrorSource.GetPostgres()
	require.NotNil(t, pgCfg)
	require.Equal(t, "postgres://user:pass@host:5432/ledger", pgCfg.Dsn)
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
							Info: &commonpb.LedgerInfo{Name: "mirror-default"},
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
	require.NotNil(t, capturedReq.GetCreateLedger().MirrorSource.GetHttp())
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
	require.Equal(t, "src-ledger", cfg.LedgerName)

	httpCfg := cfg.GetHttp()
	require.NotNil(t, httpCfg)
	require.Equal(t, "http://localhost:3068", httpCfg.BaseUrl)
	require.NotNil(t, httpCfg.Oauth2ClientCredentials)
	require.Equal(t, "my-client-id", httpCfg.Oauth2ClientCredentials.ClientId)
	require.Equal(t, "my-client-secret", httpCfg.Oauth2ClientCredentials.ClientSecret)
	require.Equal(t, "https://auth.example.com/token", httpCfg.Oauth2ClientCredentials.TokenEndpoint)
	require.Equal(t, []string{"ledger:read"}, httpCfg.Oauth2ClientCredentials.Scopes)
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
	require.Equal(t, "src-ledger", cfg.LedgerName)

	pgCfg := cfg.GetPostgres()
	require.NotNil(t, pgCfg)
	require.Equal(t, "postgres://user:pass@host/db", pgCfg.Dsn)
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
	require.Nil(t, httpCfg.Oauth2ClientCredentials)
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
