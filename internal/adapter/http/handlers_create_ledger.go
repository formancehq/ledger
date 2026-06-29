package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// createLedgerBody holds optional fields for ledger creation.
type createLedgerBody struct {
	Mode                   string            `json:"mode,omitempty"`
	MirrorSource           *mirrorSourceBody `json:"mirrorSource,omitempty"`
	DefaultEnforcementMode string            `json:"defaultEnforcementMode,omitempty"`
}

// mirrorSourceBody holds the mirror source configuration.
type mirrorSourceBody struct {
	LedgerName          string   `json:"ledgerName"`
	Type                string   `json:"type"`                          // "http" (default) or "postgres"
	BaseURL             string   `json:"baseUrl,omitempty"`             // HTTP source
	OAuth2ClientID      string   `json:"oauth2ClientId,omitempty"`      // HTTP source OAuth2
	OAuth2ClientSecret  string   `json:"oauth2ClientSecret,omitempty"`  // HTTP source OAuth2
	OAuth2TokenEndpoint string   `json:"oauth2TokenEndpoint,omitempty"` // HTTP source OAuth2
	OAuth2Scopes        []string `json:"oauth2Scopes,omitempty"`        // HTTP source OAuth2
	DSN                 string   `json:"dsn,omitempty"`                 // Postgres source
	BatchSize           uint32   `json:"batchSize,omitempty"`           // Max logs per batch (0 = default 100)
}

// handleCreateLedger handles POST /{ledgerName} to create a new ledger.
func (s *Server) handleCreateLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	createReq := &servicepb.CreateLedgerRequest{
		Name: ledgerName,
	}

	// Parse optional body for mirror mode.
	// Use ContentLength != 0 to also handle chunked requests (ContentLength == -1).
	if r.ContentLength != 0 {
		var body createLedgerBody

		err := json.UnmarshalRead(r.Body, &body)
		if err != nil {
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

		if body.DefaultEnforcementMode != "" {
			mode, err := parseEnforcementMode(body.DefaultEnforcementMode)
			if err != nil {
				writeBadRequest(w, "INVALID_REQUEST", err)

				return
			}

			createReq.DefaultEnforcementMode = mode
		}
	}

	logs, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &servicepb.Request{
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: createReq,
		},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	if len(logs) == 0 {
		unreachable("create-ledger apply returned no log", map[string]any{"ledger": ledgerName})
	}

	createLedgerLog := logs[0].GetPayload().GetCreateLedger()
	if createLedgerLog == nil {
		writeInternalServerError(w, r, errors.New("unexpected log payload type"))

		return
	}

	writeCreated(w, createLedgerLog.ToLedgerInfo())
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
