package http

import (
	stdjson "encoding/json"
	"errors"
	"fmt"
	"net/http"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// createLedgerBody holds optional fields for ledger creation.
type createLedgerBody struct {
	Mode                   string            `json:"mode,omitempty"`
	MirrorSource           *mirrorSourceBody `json:"mirrorSource,omitempty"`
	DefaultEnforcementMode string            `json:"defaultEnforcementMode,omitempty"`
	// InitialSchema declares metadata field types to seed at creation time.
	InitialSchema []metadataFieldTypeBody `json:"initialSchema,omitempty"`
	// AccountTypes declares the initial account types (name -> full model).
	AccountTypes map[string]accountTypeBody `json:"accountTypes,omitempty"`
}

// metadataFieldTypeBody is the camelCase JSON representation of a
// SetMetadataFieldTypeCommand used to seed a ledger's initial metadata schema.
type metadataFieldTypeBody struct {
	TargetType string `json:"targetType"` // account | transaction | ledger
	Key        string `json:"key"`
	Type       string `json:"type"` // string | int64 | bool | uint64 | ... | datetime
}

// toProto converts the metadata field type body to its proto command, reusing
// the shared commonpb enum parsers.
func (b metadataFieldTypeBody) toProto() (*commonpb.SetMetadataFieldTypeCommand, error) {
	targetType, err := commonpb.ParseTargetType(b.TargetType)
	if err != nil {
		return nil, err
	}

	metadataType, err := commonpb.ParseMetadataType(b.Type)
	if err != nil {
		return nil, err
	}

	return &commonpb.SetMetadataFieldTypeCommand{
		TargetType: targetType,
		Key:        b.Key,
		Type:       metadataType,
	}, nil
}

// mirrorSourceBody holds the mirror source configuration.
type mirrorSourceBody struct {
	LedgerName          string   `json:"ledgerName"`
	Type                string   `json:"type"`                          // "ledgerV2Http" (default) or "ledgerV2Database"
	BaseURL             string   `json:"baseUrl,omitempty"`             // HTTP source
	OAuth2ClientID      string   `json:"oauth2ClientId,omitempty"`      // HTTP source OAuth2
	OAuth2ClientSecret  string   `json:"oauth2ClientSecret,omitempty"`  // HTTP source OAuth2
	OAuth2TokenEndpoint string   `json:"oauth2TokenEndpoint,omitempty"` // HTTP source OAuth2
	OAuth2Scopes        []string `json:"oauth2Scopes,omitempty"`        // HTTP source OAuth2
	DSN                 string   `json:"dsn,omitempty"`                 // Postgres source
	BatchSize           uint32   `json:"batchSize,omitempty"`           // Max logs per batch (0 = default 100)
	// RewriteRules are mirror rewrite rules applied, in order, to every mirror
	// log entry during translation (per-variant match + declarative actions).
	// Each element must be a JSON object that matches the MirrorRewriteRule
	// proto — the default JSON decoder can't dispatch its `scope` oneof, so we
	// route each rule through protojson.
	RewriteRules []stdjson.RawMessage `json:"rewriteRules,omitempty"`
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

		for i, cmd := range body.InitialSchema {
			converted, err := cmd.toProto()
			if err != nil {
				writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("initialSchema[%d]: %w", i, err))

				return
			}

			createReq.InitialSchema = append(createReq.InitialSchema, converted)
		}

		if len(body.AccountTypes) > 0 {
			createReq.AccountTypes = make(map[string]*commonpb.AccountType, len(body.AccountTypes))
			for name, at := range body.AccountTypes {
				converted, err := at.toProto()
				if err != nil {
					writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("accountTypes[%q]: %w", name, err))

					return
				}

				createReq.AccountTypes[name] = converted
			}
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

	for i, raw := range body.RewriteRules {
		if len(raw) == 0 || string(raw) == "null" {
			return nil, fmt.Errorf("rewriteRules[%d]: rule must not be empty", i)
		}

		rule := &commonpb.MirrorRewriteRule{}
		if err := protojson.Unmarshal(raw, rule); err != nil {
			return nil, fmt.Errorf("rewriteRules[%d]: %w", i, err)
		}

		cfg.RewriteRules = append(cfg.RewriteRules, rule)
	}

	switch body.Type {
	case "ledgerV2Http", "":
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

		cfg.Type = &commonpb.MirrorSourceConfig_LedgerV2Http{
			LedgerV2Http: httpCfg,
		}
	case "ledgerV2Database":
		cfg.Type = &commonpb.MirrorSourceConfig_LedgerV2Database{
			LedgerV2Database: &commonpb.PostgresMirrorSourceConfig{
				Dsn: body.DSN,
			},
		}
	default:
		return nil, fmt.Errorf("unsupported mirror source type: %q", body.Type)
	}

	return cfg, nil
}
