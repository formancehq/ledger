package ledger

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/formancehq/fctl/pkg/pluginsdk"
)

func TestPayloadValidationBeforeExecutors(t *testing.T) {
	t.Parallel()
	cases := []struct{ name, command, body, want string }{
		{"ledger array", "create books", `[]`, "body must be a JSON object"},
		{"ledger mode", "create books", `{"mode":1}`, "body.mode must be a string"},
		{"schema shape", "create books", `{"initialSchema":{}}`, "body.initialSchema must be a JSON array"},
		{"schema entry", "create books", `{"initialSchema":[false]}`, "body.initialSchema[0] must be a JSON object"},
		{"schema type", "create books", `{"initialSchema":[{"targetType":1}]}`, "body.initialSchema[0].targetType must be a string"},
		{"account type", "create books", `{"accountTypes":{"user":{"pattern":1}}}`, `body.accountTypes["user"].pattern must be a string`},
		{"segment type", "create books", `{"accountTypes":{"user":{"segmentTypes":{"id":[]}}}}`, `body.accountTypes["user"].segmentTypes["id"] must be a JSON object`},
		{"mirror shape", "create books", `{"mirrorSource":[]}`, "body.mirrorSource must be a JSON object"},
		{"mirror batch", "create books", `{"mirrorSource":{"batchSize":4294967296}}`, "body.mirrorSource.batchSize must be an unsigned 32-bit integer"},
		{"mirror scopes", "create books", `{"mirrorSource":{"oauth2Scopes":[false]}}`, "body.mirrorSource.oauth2Scopes[0] must be a string"},
		{"transaction null", "transactions create", `null`, "body must be a JSON object"},
		{"postings shape", "transactions create", `{"postings":{}}`, "body.postings must be a JSON array"},
		{"posting null", "transactions create", `{"postings":[null]}`, "body.postings[0] must be a JSON object"},
		{"posting address", "transactions create", `{"postings":[{"source":12}]}`, "body.postings[0].source must be a string"},
		{"amount float", "transactions create", `{"postings":[{"amount":1.5}]}`, "body.postings[0].amount must be a canonical unsigned decimal integer"},
		{"amount exponent", "transactions create", `{"postings":[{"amount":1e2}]}`, "body.postings[0].amount must be a canonical unsigned decimal integer"},
		{"amount negative", "transactions create", `{"postings":[{"amount":-1}]}`, "body.postings[0].amount must be a canonical unsigned decimal integer"},
		{"amount noncanonical", "transactions create", `{"postings":[{"amount":"01"}]}`, "body.postings[0].amount must be a canonical unsigned decimal integer"},
		{"amount overflow", "transactions create", `{"postings":[{"amount":115792089237316195423570985008687907853269984665640564039457584007913129639936}]}`, "body.postings[0].amount exceeds uint256"},
		{"script shape", "transactions create", `{"script":"send"}`, "body.script must be a JSON object"},
		{"script variable", "transactions create", `{"script":{"vars":{"amount":9007199254740993}}}`, `body.script.vars["amount"] must be a string`},
		{"script reference", "transactions create", `{"scriptReference":{"version":1}}`, "body.scriptReference.version must be a string"},
		{"timestamp", "transactions create", `{"timestamp":123}`, "body.timestamp must be a string"},
		{"force", "transactions create", `{"force":"true"}`, "body.force must be a boolean"},
		{"metadata nested", "metadata set", `{"nested":{}}`, `body["nested"] must be a string, boolean, exact integer or null`},
		{"metadata array", "accounts metadata set users:42", `[]`, "body must be a JSON object"},
		{"metadata fractional", "transactions metadata set 42", `{"score":0.5}`, `body["score"] must be an exact integer`},
		{"metadata positive overflow", "metadata set", `{"score":18446744073709551616}`, `body["score"] must be an exact integer`},
		{"metadata negative overflow", "metadata set", `{"score":-9223372036854775809}`, `body["score"] must be an exact integer`},
		{"metadata exponent overflow", "metadata set", `{"score":1e999999999999999999999999}`, `body["score"] must be an exact integer`},
		{"account metadata", "transactions create", `{"accountMetadata":{"users:42":[]}}`, `body.accountMetadata["users:42"] must be a JSON object`},
		{"revert option", "transactions revert 42", `{"atEffectiveDate":[]}`, "body.atEffectiveDate must be a boolean"},
		{"index id", "indexes create", `{"id":{}}`, "body.id must be a string"},
		{"bulk shape", "bulk", `{}`, "body must be a JSON array"},
		{"bulk entry", "bulk", `[null]`, "body[0] must be a JSON object"},
		{"bulk action", "bulk", `[{"action":false,"data":{}}]`, "body[0].action must be a string"},
		{"bulk missing action", "bulk", `[{"data":{}}]`, "body[0].action must be a string"},
		{"bulk empty action", "bulk", `[{"action":"","data":{}}]`, "body[0].action must not be empty"},
		{"bulk missing data", "bulk", `[{"action":"CREATE_TRANSACTION"}]`, "body[0].data must be a JSON object"},
		{"bulk identity", "bulk", `[{"action":"CREATE_TRANSACTION","data":{},"ik":"bad\nkey"}]`, "body[0].ik must be at most 256 bytes without newlines"},
		{"bulk data", "bulk", `[{"action":"CREATE_TRANSACTION","data":[]}]`, "body[0].data must be a JSON object"},
		{"bulk transaction", "bulk", `[{"action":"CREATE_TRANSACTION","data":{"force":1}}]`, "body[0].data.force must be a boolean"},
		{"bulk revert id", "bulk", `[{"action":"REVERT_TRANSACTION","data":{"id":18446744073709551616}}]`, "body[0].data.id must be an unsigned 64-bit integer"},
		{"bulk account target", "bulk", `[{"action":"ADD_METADATA","data":{"targetType":"ACCOUNT","targetId":1}}]`, "body[0].data.targetId must be a string"},
		{"bulk transaction target", "bulk", `[{"action":"DELETE_METADATA","data":{"targetType":"TRANSACTION","targetId":-1}}]`, "body[0].data.targetId must be an unsigned 64-bit integer"},
		{"bulk lowercase target", "bulk", `[{"action":"DELETE_METADATA","data":{"targetType":"transaction","targetId":"42"}}]`, "body[0].data.targetId must be an unsigned 64-bit integer"},
		{"bulk reasons", "bulk", `[{"action":"CREATE_TRANSACTION","data":{},"skippableReasons":[1]}]`, "body[0].skippableReasons[0] must be a string"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tokens := append([]string{"--ledger", "books"}, strings.Fields(tc.command)...)
			out, requests, err := execute(t, tokens, tc.body, envelope, 200)
			if err == nil || !strings.Contains(err.Error(), tc.want) || len(requests) != 0 || out != "" {
				t.Fatalf("HTTP: want %q before dispatch; requests=%d out=%s err=%v", tc.want, len(requests), out, err)
			}
			calls := 0
			p := NewWithExecutor("", func(context.Context, pluginsdk.ExecuteRequest) (pluginsdk.ExecuteResponse, error) {
				calls++

				return pluginsdk.ExecuteResponse{}, nil
			})
			manifest, err := p.GetManifest(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			_, err = p.Execute(t.Context(), testRequest(manifest, tokens, tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.want) || calls != 0 {
				t.Fatalf("alternate executor: want %q before dispatch; calls=%d err=%v", tc.want, calls, err)
			}
		})
	}
}

func TestPayloadPreservesCanonicalJSONAndServerValidation(t *testing.T) {
	t.Parallel()
	cases := []struct{ command, body string }{
		{"create books", `{}`}, {"transactions create", `{}`},
		{"transactions create", `{"postings":[],"metadata":null,"script":null}`},
		{"transactions create", `{"postings":[{"amount":null}],"force":null}`},
		{"transactions revert 42", `{}`}, {"indexes create", `{}`},
		{"metadata set", `{}`}, {"bulk", `[]`},
		{"create books", `{"metadata":{"big":18446744073709551615,"negative":-9223372036854775808},"initialSchema":[{"targetType":"account","key":"id","type":"uint64"}],"accountTypes":{"user":{"name":"user","pattern":"users:{id}","persistence":"EPHEMERAL","segmentTypes":{"id":{"type":"uint64"}}}},"future":{"nested":[1.5]}}`},
		{"create books", `{"mode":"MIRROR","mirrorSource":{"type":"http","baseUrl":"https://example.com","oauth2Scopes":["ledger:read"],"batchSize":4294967295,"rewriteRules":[{"anyVariant":{"actions":[]}}]}}`},
		{"transactions create", ` {"postings":[{"source":"world","destination":"users:42","asset":"USD/2","amount":115792089237316195423570985008687907853269984665640564039457584007913129639935,"color":"RED","future":[]},{"amount":"9007199254740993"}],"metadata":{"exact":9007199254740993},"accountMetadata":{"users:42":{"active":true}},"future":[9007199254740993]} `},
		{"transactions create", `{"script":{"plain":"send [USD/2 $amount]","vars":{"amount":"9007199254740993"}},"timestamp":"2026-10-09T12:00:00Z","reference":"payment","force":false}`},
		{"transactions create", `{"scriptReference":{"name":"pay","version":"latest","vars":{"amount":"9007199254740993"}}}`},
		{"metadata set", `{"integral":9007199254740993.0,"exponent":90071992547409930e-1,"max":1.8446744073709551615e19,"zero":0e99999999999999999999999,"deleted":null,"enabled":true}`},
		{"metadata set", `{"negative":-9223372036854775808.0,"one":100000000000000000000e-20,"small":1e-0,"negativeZero":-0.0}`},
		{"transactions revert 42", `{"force":true,"atEffectiveDate":false,"metadata":{"score":9007199254740993}}`},
		{"indexes create", `{"id":"metadata:TARGET_TYPE_ACCOUNT:key","future":{"option":true}}`},
		{"bulk", `[{"action":"CREATE_TRANSACTION","ik":"tx","data":{}},{"action":"REVERT_TRANSACTION","data":{"id":18446744073709551615}},{"action":"ADD_METADATA","data":{"targetType":"ACCOUNT","targetId":"users:42","metadata":{"exact":9007199254740993}}},{"action":"DELETE_METADATA","data":{"targetType":"TRANSACTION","targetId":42,"key":"key"},"skippableReasons":["METADATA_NOT_FOUND"]},{"action":"FUTURE_ACTION","data":{"future":[1.5]}}]`},
	}
	for _, tc := range cases {
		t.Run(tc.command+"/"+tc.body, func(t *testing.T) {
			t.Parallel()
			tokens := append([]string{"--ledger", "books"}, strings.Fields(tc.command)...)
			response := `{"data":[]}`
			_, requests, err := execute(t, tokens, tc.body, response, 200)
			if err != nil || len(requests) != 1 || requests[0].body != tc.body {
				t.Fatalf("HTTP changed valid payload: requests=%v err=%v", requests, err)
			}
			calls := 0
			p := NewWithExecutor("", func(_ context.Context, req pluginsdk.ExecuteRequest) (pluginsdk.ExecuteResponse, error) {
				calls++
				if string(req.Body) != tc.body {
					t.Fatalf("alternate executor changed exact JSON: %s", req.Body)
				}

				return pluginsdk.ExecuteResponse{Data: json.RawMessage(response)}, nil
			})
			manifest, err := p.GetManifest(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.Execute(t.Context(), testRequest(manifest, tokens, tc.body)); err != nil || calls != 1 {
				t.Fatalf("alternate executor rejected valid payload: calls=%d err=%v", calls, err)
			}
		})
	}
}
