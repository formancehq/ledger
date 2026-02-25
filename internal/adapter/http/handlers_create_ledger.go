package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/go-chi/chi/v5"
)

// createLedgerBody holds optional fields for ledger creation.
type createLedgerBody struct {
	Mode            string           `json:"mode,omitempty"`
	MirrorSource    *mirrorSourceBody `json:"mirrorSource,omitempty"`
	ChartOfAccounts *chartJSON       `json:"chartOfAccounts,omitempty"`
	EnforcementMode string           `json:"enforcementMode,omitempty"`
}

// mirrorSourceBody holds the mirror source configuration.
type mirrorSourceBody struct {
	LedgerName          string   `json:"ledgerName"`
	Type                string   `json:"type"`                           // "http" (default) or "postgres"
	BaseURL             string   `json:"baseUrl,omitempty"`              // HTTP source
	OAuth2ClientID      string   `json:"oauth2ClientId,omitempty"`       // HTTP source OAuth2
	OAuth2ClientSecret  string   `json:"oauth2ClientSecret,omitempty"`   // HTTP source OAuth2
	OAuth2TokenEndpoint string   `json:"oauth2TokenEndpoint,omitempty"`  // HTTP source OAuth2
	OAuth2Scopes        []string `json:"oauth2Scopes,omitempty"`         // HTTP source OAuth2
	DSN                 string   `json:"dsn,omitempty"`                  // Postgres source
	BatchSize           uint32   `json:"batchSize,omitempty"`            // Max logs per batch (0 = default 100)
}

// handleCreateLedger handles POST /{ledgerName} to create a new ledger
func (s *Server) handleCreateLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	createReq := &servicepb.CreateLedgerRequest{
		Name: ledgerName,
	}

	// Parse optional body for mirror mode and chart of accounts fields
	if r.ContentLength > 0 {
		var body createLedgerBody
		if err := json.UnmarshalRead(r.Body, &body); err != nil {
			writeBadRequest(w, "INVALID_REQUEST", err)
			return
		}
		if body.Mode == "MIRROR" {
			createReq.Mode = commonpb.LedgerMode_LEDGER_MODE_MIRROR
			if body.MirrorSource != nil {
				cfg, err := mirrorSourceToProto(body.MirrorSource)
				if err != nil {
					writeBadRequest(w, "INVALID_REQUEST", err)
					return
				}
				createReq.MirrorSource = cfg
			}
		}
		if body.ChartOfAccounts != nil {
			createReq.ChartOfAccounts = fromChartJSON(body.ChartOfAccounts)
		}
		if body.EnforcementMode != "" {
			mode, err := parseEnforcementMode(body.EnforcementMode)
			if err != nil {
				writeBadRequest(w, "INVALID_REQUEST", err)
				return
			}
			createReq.EnforcementMode = mode
		}
	}


	logs, err := s.backend.Apply(r.Context(), &servicepb.Request{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: createReq,
		},
	})
	if err != nil {
		handleError(w, r, err)
		return
	}

	writeCreated(w, logs[0].Payload.GetCreateLedger().GetInfo())
}

// mirrorSourceToProto converts the HTTP body to the proto MirrorSourceConfig.
func mirrorSourceToProto(body *mirrorSourceBody) (*commonpb.MirrorSourceConfig, error) {
	cfg := &commonpb.MirrorSourceConfig{
		LedgerName: body.LedgerName,
		BatchSize:  body.BatchSize,
	}
	switch body.Type {
	case "http", "":
		httpCfg := &commonpb.HttpMirrorSourceConfig{
			BaseUrl: body.BaseURL,
		}
		if body.OAuth2ClientID != "" || body.OAuth2TokenEndpoint != "" {
			httpCfg.Oauth2ClientCredentials = &commonpb.OAuth2ClientCredentials{
				ClientId:      body.OAuth2ClientID,
				ClientSecret:  body.OAuth2ClientSecret,
				TokenEndpoint: body.OAuth2TokenEndpoint,
				Scopes:        body.OAuth2Scopes,
			}
		}
		cfg.Type = &commonpb.MirrorSourceConfig_Http{
			Http: httpCfg,
		}
	case "postgres":
		cfg.Type = &commonpb.MirrorSourceConfig_Postgres{
			Postgres: &commonpb.PostgresMirrorSourceConfig{
				Dsn: body.DSN,
			},
		}
	default:
		return nil, fmt.Errorf("unsupported mirror source type: %q", body.Type)
	}
	return cfg, nil
}
