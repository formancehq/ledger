package ledgerv3

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	testOAuthSecret = "oauth-secret-9f28d1"
	testPostgresDSN = "postgres://ledger:dsn-secret-84ea@db.example/ledger"
)

func TestLedgerInfoCredentialsNeverReachResultEvents(t *testing.T) {
	t.Parallel()

	httpLedger := ledgerWithHTTPSecret(testOAuthSecret)
	postgresLedger := ledgerWithPostgresDSN(testPostgresDSN)
	tests := []struct {
		name      string
		commandID string
		run       func(*testing.T, sdk.Host) error
	}{
		{
			name: "create", commandID: "ledger.v3.ledgers.create",
			run: func(t *testing.T, host sdk.Host) error {
				command, decoded, request := decodedLedgerCommand(t, "ledger.v3.ledgers.create", []string{"mirror"}, nil)
				_, err := executeV3Ledgers(context.Background(), request, decoded, command, host)
				return err
			},
		},
		{
			name: "get", commandID: "ledger.v3.ledgers.get",
			run: func(t *testing.T, host sdk.Host) error {
				command, decoded, request := decodedLedgerCommand(t, "ledger.v3.ledgers.get", []string{"mirror"}, nil)
				_, err := executeV3Reads(context.Background(), request, decoded, command, host)
				return err
			},
		},
		{
			name: "list", commandID: "ledger.v3.ledgers.list",
			run: func(t *testing.T, host sdk.Host) error {
				command, decoded, request := decodedLedgerCommand(t, "ledger.v3.ledgers.list", nil, nil)
				_, err := executeV3Reads(context.Background(), request, decoded, command, host)
				return err
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			host := sdk.NewMemoryHost(func(_ context.Context, request sdk.Request) (sdk.Responses, error) {
				switch request.Operation {
				case opApplyCreateLedger.id:
					created := &commonpb.CreatedLedgerLog{
						Name: httpLedger.Name, Mode: httpLedger.Mode, MirrorSource: httpLedger.MirrorSource,
					}
					return sdk.NewResponseStream(protoResponse(t, ledgerApplyResponse(&commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: created}}))), nil
				case opGetLedger.id:
					return sdk.NewResponseStream(protoResponse(t, postgresLedger)), nil
				case opListLedgers.id:
					return sdk.NewResponseStream(protoResponse(t, httpLedger), protoResponse(t, postgresLedger)), nil
				default:
					t.Fatalf("unexpected operation %q", request.Operation)
					return nil, nil
				}
			})
			if err := test.run(t, host); err != nil {
				t.Fatal(err)
			}
			events := host.Events()
			if len(events) != 1 || events[0].Result == nil {
				t.Fatalf("events = %#v", events)
			}
			assertTokensAbsentRecursively(t, events[0].Result.Data, testOAuthSecret, testPostgresDSN)
			validateLedgerPublicOutput(t, test.commandID, events[0].Result.Data)
		})
	}

	if httpLedger.GetMirrorSource().GetHttp().GetOauth2ClientCredentials().GetClientSecret() != testOAuthSecret {
		t.Fatal("HTTP adapter response was mutated instead of cloned")
	}
	if postgresLedger.GetMirrorSource().GetPostgres().GetDsn() != testPostgresDSN {
		t.Fatal("Postgres adapter response was mutated instead of cloned")
	}
}

func TestLedgerInfoPublicSchemasRejectCredentialReintroduction(t *testing.T) {
	t.Parallel()

	for _, commandID := range []string{"ledger.v3.ledgers.create", "ledger.v3.ledgers.get", "ledger.v3.ledgers.list"} {
		commandID := commandID
		t.Run(commandID, func(t *testing.T) {
			t.Parallel()
			command, ok := commandByID(commandID)
			if !ok {
				t.Fatalf("missing command %q", commandID)
			}
			if !bytes.Equal(command.RawOutputSchema, command.PublicOutputSchema) {
				t.Fatal("already-sanitized plugin output must use the same raw and public schema")
			}
			compiler := jsonschema.NewCompiler()
			document, err := jsonschema.UnmarshalJSON(bytes.NewReader(command.PublicOutputSchema))
			if err != nil {
				t.Fatal(err)
			}
			if err := compiler.AddResource("schema.json", document); err != nil {
				t.Fatal(err)
			}
			schema, err := compiler.Compile("schema.json")
			if err != nil {
				t.Fatal(err)
			}

			item := map[string]any{"name": "mirror", "mirrorSource": map[string]any{
				"ledgerName": "source", "http": map[string]any{"baseUrl": "https://ledger.example", "oauth2ClientCredentials": map[string]any{
					"clientId": "client", "tokenEndpoint": "https://issuer.example/token", "scopes": []any{"ledger:read"},
				}},
			}}
			valid := any(item)
			if commandID == "ledger.v3.ledgers.list" {
				valid = []any{item}
			}
			if err := schema.Validate(valid); err != nil {
				t.Fatalf("safe LedgerInfo rejected: %v", err)
			}

			credentials := item["mirrorSource"].(map[string]any)["http"].(map[string]any)["oauth2ClientCredentials"].(map[string]any)
			credentials["clientSecret"] = testOAuthSecret
			if err := schema.Validate(valid); err == nil {
				t.Fatal("public schema accepts reintroduced OAuth2 clientSecret")
			}
			delete(credentials, "clientSecret")
			item["mirrorSource"].(map[string]any)["postgres"] = map[string]any{"dsn": testPostgresDSN}
			if err := schema.Validate(valid); err == nil {
				t.Fatal("public schema accepts reintroduced Postgres DSN")
			}
		})
	}
}

func ledgerWithHTTPSecret(secret string) *commonpb.LedgerInfo {
	return &commonpb.LedgerInfo{Name: "http-mirror", Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR, MirrorSource: &commonpb.MirrorSourceConfig{
		LedgerName: "source", Type: &commonpb.MirrorSourceConfig_Http{Http: &commonpb.HttpMirrorSourceConfig{
			BaseUrl: "https://ledger.example", Oauth2ClientCredentials: &commonpb.OAuth2ClientCredentials{
				ClientId: "client", ClientSecret: secret, TokenEndpoint: "https://issuer.example/token", Scopes: []string{"ledger:read"},
			},
		}},
	}}
}

func ledgerWithPostgresDSN(dsn string) *commonpb.LedgerInfo {
	return &commonpb.LedgerInfo{Name: "postgres-mirror", Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR, MirrorSource: &commonpb.MirrorSourceConfig{
		LedgerName: "source", Type: &commonpb.MirrorSourceConfig_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfig{
			Dsn: dsn, AwsIamAuth: &commonpb.PostgresAwsIamAuth{Region: "eu-west-1"},
		}},
	}}
}

func assertTokensAbsentRecursively(t *testing.T, encoded []byte, tokens ...string) {
	t.Helper()
	var document any
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatal(err)
	}
	var visit func(any)
	visit = func(value any) {
		switch value := value.(type) {
		case map[string]any:
			for _, child := range value {
				visit(child)
			}
		case []any:
			for _, child := range value {
				visit(child)
			}
		case string:
			for _, token := range tokens {
				if strings.Contains(value, token) {
					t.Fatalf("result contains secret token %q", token)
				}
			}
		}
	}
	visit(document)
}

func validateLedgerPublicOutput(t *testing.T, commandID string, encoded []byte) {
	t.Helper()
	command, ok := commandByID(commandID)
	if !ok {
		t.Fatalf("missing command %q", commandID)
	}
	compiler := jsonschema.NewCompiler()
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader(command.PublicOutputSchema))
	if err != nil {
		t.Fatal(err)
	}
	if err := compiler.AddResource("schema.json", document); err != nil {
		t.Fatal(err)
	}
	schema, err := compiler.Compile("schema.json")
	if err != nil {
		t.Fatal(err)
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.Validate(value); err != nil {
		t.Fatalf("emitted result violates public schema: %v; result=%s", err, encoded)
	}
}
