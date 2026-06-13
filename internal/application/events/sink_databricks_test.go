//go:build databricks

package events

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestDatabricksProtoFieldNumbers guards the protobuf wire layout of
// DatabricksSinkConfig and the nested DatabricksOAuthM2M message. Renumbering
// any of these fields silently corrupts already persisted/replicated sink
// configs after upgrade, so the numbers must stay stable (token deliberately
// keeps field number 3 inside the auth oneof).
func TestDatabricksProtoFieldNumbers(t *testing.T) {
	t.Parallel()

	assertFieldNumbers := func(t *testing.T, msg protoreflect.ProtoMessage, want map[string]protoreflect.FieldNumber) {
		t.Helper()

		fields := msg.ProtoReflect().Descriptor().Fields()
		for name, number := range want {
			f := fields.ByName(protoreflect.Name(name))
			require.NotNil(t, f, "field %q must exist", name)
			require.Equal(t, number, f.Number(), "field %q must keep wire number %d", name, number)
		}
	}

	assertFieldNumbers(t, &commonpb.DatabricksSinkConfig{}, map[string]protoreflect.FieldNumber{
		"server_hostname": 1,
		"http_path":       2,
		"token":           3,
		"catalog":         4,
		"schema":          5,
		"table":           6,
		"port":            7,
		"oauth_m2m":       8,
	})
	assertFieldNumbers(t, &commonpb.DatabricksOAuthM2M{}, map[string]protoreflect.FieldNumber{
		"client_id":     1,
		"client_secret": 2,
	})
}

func TestDatabricksConfigFromProto(t *testing.T) {
	t.Parallel()

	newBase := func() *commonpb.DatabricksSinkConfig {
		return &commonpb.DatabricksSinkConfig{
			ServerHostname: "adb-123.azuredatabricks.net",
			HttpPath:       "/sql/1.0/warehouses/abc",
			Catalog:        "main",
			Schema:         "default",
			Table:          "events",
			Port:           8443,
		}
	}

	t.Run("PAT token variant", func(t *testing.T) {
		t.Parallel()

		pb := newBase()
		pb.Auth = &commonpb.DatabricksSinkConfig_Token{Token: "dapi123"}

		cfg, err := databricksConfigFromProto(pb)

		require.NoError(t, err)
		require.Equal(t, "dapi123", cfg.Token)
		require.Empty(t, cfg.OAuthClientID)
		require.Empty(t, cfg.OAuthClientSecret)
		require.Equal(t, 8443, cfg.Port)
		require.Equal(t, "main", cfg.Catalog)
	})

	t.Run("OAuth M2M variant", func(t *testing.T) {
		t.Parallel()

		pb := newBase()
		pb.Auth = &commonpb.DatabricksSinkConfig_OauthM2M{
			OauthM2M: &commonpb.DatabricksOAuthM2M{
				ClientId:     "client-id",
				ClientSecret: "client-secret",
			},
		}

		cfg, err := databricksConfigFromProto(pb)

		require.NoError(t, err)
		require.Empty(t, cfg.Token)
		require.Equal(t, "client-id", cfg.OAuthClientID)
		require.Equal(t, "client-secret", cfg.OAuthClientSecret)
	})

	t.Run("port=0 is passed through (default applied later)", func(t *testing.T) {
		t.Parallel()

		pb := newBase()
		pb.Port = 0
		pb.Auth = &commonpb.DatabricksSinkConfig_Token{Token: "dapi123"}

		cfg, err := databricksConfigFromProto(pb)

		require.NoError(t, err)
		// The default (443) is applied inside newDatabricksConnector to keep
		// a single source of truth.
		require.Equal(t, 0, cfg.Port)
	})

	t.Run("empty oauth_m2m message is rejected", func(t *testing.T) {
		t.Parallel()

		pb := newBase()
		pb.Auth = &commonpb.DatabricksSinkConfig_OauthM2M{
			OauthM2M: &commonpb.DatabricksOAuthM2M{},
		}

		_, err := databricksConfigFromProto(pb)

		require.Error(t, err)
		require.Contains(t, err.Error(), "oauth_m2m")
	})

	t.Run("nil oauth_m2m pointer in oneof is rejected", func(t *testing.T) {
		t.Parallel()

		pb := newBase()
		pb.Auth = &commonpb.DatabricksSinkConfig_OauthM2M{OauthM2M: nil}

		_, err := databricksConfigFromProto(pb)

		require.Error(t, err)
	})
}

func TestNewDatabricksConnector_Validation(t *testing.T) {
	t.Parallel()

	base := DatabricksSinkConfig{
		ServerHostname: "adb-123.azuredatabricks.net",
		HTTPPath:       "/sql/1.0/warehouses/abc",
		Catalog:        "main",
		Schema:         "default",
		Port:           443,
	}

	tests := []struct {
		name        string
		patch       func(*DatabricksSinkConfig)
		wantErr     bool
		errContains string
	}{
		{
			name:    "PAT only — valid",
			patch:   func(c *DatabricksSinkConfig) { c.Token = "dapi123" },
			wantErr: false,
		},
		{
			name: "OAuth M2M — valid",
			patch: func(c *DatabricksSinkConfig) {
				c.OAuthClientID = "client-id"
				c.OAuthClientSecret = "client-secret"
			},
			wantErr: false,
		},
		{
			name: "both PAT and OAuth — mutually exclusive",
			patch: func(c *DatabricksSinkConfig) {
				c.Token = "dapi123"
				c.OAuthClientID = "client-id"
				c.OAuthClientSecret = "client-secret"
			},
			wantErr:     true,
			errContains: "mutually exclusive",
		},
		{
			name:        "neither PAT nor OAuth — no auth",
			patch:       func(_ *DatabricksSinkConfig) {},
			wantErr:     true,
			errContains: "no authentication configured",
		},
		{
			name: "OAuth client_id only — missing secret",
			patch: func(c *DatabricksSinkConfig) {
				c.OAuthClientID = "client-id"
			},
			wantErr:     true,
			errContains: "both oauth_client_id and oauth_client_secret",
		},
		{
			name: "OAuth client_secret only — missing client_id",
			patch: func(c *DatabricksSinkConfig) {
				c.OAuthClientSecret = "client-secret"
			},
			wantErr:     true,
			errContains: "both oauth_client_id and oauth_client_secret",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := base
			tt.patch(&cfg)

			_, err := newDatabricksConnector(cfg)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
