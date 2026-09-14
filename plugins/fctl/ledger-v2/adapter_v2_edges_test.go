package ledgerv2

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
)

func TestExecuteV2RejectsUnknownAndIncompatibleCommandsBeforeProductTraffic(t *testing.T) {
	t.Parallel()
	host := sdk.NewMemoryHost(nil)
	if err := (Plugin{}).Execute(context.Background(), sdk.ExecuteRequest{CommandID: "ledger.v2.unknown"}, host); err == nil {
		t.Fatal("unknown command accepted")
	}
	request := execution("ledger.v2.stats", nil, sdk.FlagOccurrence{Name: "ledger", Value: "primary"})
	request.ServiceVersions[0].Major = 3
	if err := (Plugin{}).Execute(context.Background(), request, host); err == nil {
		t.Fatal("incompatible service accepted")
	}
	if len(host.Requests()) != 0 {
		t.Fatal("rejected execution reached product traffic")
	}
}

func TestExecuteV2RejectsInvalidFilterMetadataBeforeProductTraffic(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		commandID string
		metadata  []string
	}{
		{name: "accounts malformed", commandID: "ledger.v2.accounts.list", metadata: []string{"missing-separator"}},
		{name: "accounts duplicate", commandID: "ledger.v2.accounts.list", metadata: []string{"region=eu", "region=us"}},
		{name: "transactions malformed", commandID: "ledger.v2.transactions.list", metadata: []string{"missing-separator"}},
		{name: "transactions duplicate", commandID: "ledger.v2.transactions.list", metadata: []string{"region=eu", "region=us"}},
		{name: "volumes malformed", commandID: "ledger.v2.volumes.list", metadata: []string{"missing-separator"}},
		{name: "volumes duplicate", commandID: "ledger.v2.volumes.list", metadata: []string{"region=eu", "region=us"}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			host := sdk.NewMemoryHost(nil)
			flags := []sdk.FlagOccurrence{{Name: "ledger", Value: "primary"}}
			for _, metadata := range test.metadata {
				flags = append(flags, sdk.FlagOccurrence{Name: "metadata", Value: metadata})
			}

			err := (Plugin{}).Execute(context.Background(), execution(test.commandID, nil, flags...), host)
			if err == nil {
				t.Fatal("invalid filter metadata accepted")
			}
			if got := len(host.Requests()); got != 0 {
				t.Fatalf("invalid filter metadata reached product traffic: %d requests", got)
			}
			if got := len(host.Events()); got != 0 {
				t.Fatalf("invalid filter metadata emitted output: %d events", got)
			}
		})
	}
}

func TestExecuteV2RejectsInvalidFiltersWithACursorBeforeProductTraffic(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		commandID string
		flags     []sdk.FlagOccurrence
	}{
		{
			name:      "accounts malformed metadata",
			commandID: "ledger.v2.accounts.list",
			flags:     []sdk.FlagOccurrence{{Name: "metadata", Value: "missing-separator"}},
		},
		{
			name:      "accounts duplicate metadata",
			commandID: "ledger.v2.accounts.list",
			flags:     []sdk.FlagOccurrence{{Name: "metadata", Value: "region=eu"}, {Name: "metadata", Value: "region=us"}},
		},
		{
			name:      "transactions malformed metadata",
			commandID: "ledger.v2.transactions.list",
			flags:     []sdk.FlagOccurrence{{Name: "metadata", Value: "missing-separator"}},
		},
		{
			name:      "transactions duplicate metadata",
			commandID: "ledger.v2.transactions.list",
			flags:     []sdk.FlagOccurrence{{Name: "metadata", Value: "region=eu"}, {Name: "metadata", Value: "region=us"}},
		},
		{
			name:      "volumes malformed metadata",
			commandID: "ledger.v2.volumes.list",
			flags:     []sdk.FlagOccurrence{{Name: "metadata", Value: "missing-separator"}},
		},
		{
			name:      "volumes duplicate metadata",
			commandID: "ledger.v2.volumes.list",
			flags:     []sdk.FlagOccurrence{{Name: "metadata", Value: "region=eu"}, {Name: "metadata", Value: "region=us"}},
		},
		{
			name:      "volumes malformed start time",
			commandID: "ledger.v2.volumes.list",
			flags:     []sdk.FlagOccurrence{{Name: "start-time", Value: "not-a-time"}},
		},
		{
			name:      "volumes malformed end time",
			commandID: "ledger.v2.volumes.list",
			flags:     []sdk.FlagOccurrence{{Name: "end-time", Value: "not-a-time"}},
		},
		{
			name:      "volumes malformed insertion date",
			commandID: "ledger.v2.volumes.list",
			flags:     []sdk.FlagOccurrence{{Name: "insertion-date", Value: "not-a-boolean"}},
		},
		{
			name:      "volumes malformed group by",
			commandID: "ledger.v2.volumes.list",
			flags:     []sdk.FlagOccurrence{{Name: "group-by", Value: "not-an-integer"}},
		},
		{
			name:      "volumes malformed page size",
			commandID: "ledger.v2.volumes.list",
			flags:     []sdk.FlagOccurrence{{Name: "page-size", Value: "not-an-integer"}},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			host := sdk.NewMemoryHost(nil)
			flags := append([]sdk.FlagOccurrence{
				{Name: "ledger", Value: "primary"},
				{Name: "cursor", Value: "opaque-page"},
			}, test.flags...)

			err := (Plugin{}).Execute(context.Background(), execution(test.commandID, nil, flags...), host)
			if err == nil {
				t.Fatal("invalid cursor filter accepted")
			}
			if got := len(host.Requests()); got != 0 {
				t.Fatalf("invalid cursor filter reached product traffic: %d requests", got)
			}
			if got := len(host.Events()); got != 0 {
				t.Fatalf("invalid cursor filter emitted output: %d events", got)
			}
		})
	}
}

func TestExecuteV2RejectsOutOfRangePaginationFlagsBeforeProductTraffic(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		commandID string
		flag      sdk.FlagOccurrence
	}{
		{name: "zero page size", commandID: "ledger.v2.accounts.list", flag: sdk.FlagOccurrence{Name: "page-size", Value: "0"}},
		{name: "negative page size", commandID: "ledger.v2.transactions.list", flag: sdk.FlagOccurrence{Name: "page-size", Value: "-1"}},
		{name: "oversized page size", commandID: "ledger.v2.schemas.list", flag: sdk.FlagOccurrence{Name: "page-size", Value: "1001"}},
		{name: "negative group by", commandID: "ledger.v2.volumes.list", flag: sdk.FlagOccurrence{Name: "group-by", Value: "-1"}},
		{name: "oversized group by", commandID: "ledger.v2.volumes.list", flag: sdk.FlagOccurrence{Name: "group-by", Value: "1001"}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			host := sdk.NewMemoryHost(func(context.Context, sdk.Request) (sdk.Responses, error) {
				t.Fatal("out-of-range pagination flag reached product traffic")
				return nil, nil
			})
			err := (Plugin{}).Execute(context.Background(), execution(
				test.commandID,
				nil,
				sdk.FlagOccurrence{Name: "ledger", Value: "primary"},
				test.flag,
			), host)
			if err == nil {
				t.Fatal("out-of-range pagination flag accepted")
			}
			if got := len(host.Requests()); got != 0 {
				t.Fatalf("out-of-range pagination flag made %d requests", got)
			}
		})
	}
}

func TestExecuteV2PropagatesProductFailureWithoutEmittingPartialOutput(t *testing.T) {
	t.Parallel()
	host := sdk.NewMemoryHost(func(context.Context, sdk.Request) (sdk.Responses, error) { return nil, errors.New("offline") })
	err := (Plugin{}).Execute(context.Background(), execution("ledger.v2.stats", nil, sdk.FlagOccurrence{Name: "ledger", Value: "primary"}), host)
	if err == nil {
		t.Fatal("product failure hidden")
	}
	if len(host.Events()) != 0 {
		t.Fatal("product failure emitted partial output")
	}
	if got := len(host.Requests()); got != 1 {
		t.Fatalf("generated client retried host-owned traffic: %d requests", got)
	}
}

// The pinned SDK classifies an in-range non-2xx product response as a typed
// HTTP failure and keeps only an out-of-range status opaque. Both codes must
// survive the generated client and the adapter's operation wrapping unchanged,
// so the host still sees the retryable flag and the originating status.
func TestExecuteV2PropagatesTypedProductHTTPFailures(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		status     int32
		code       sdk.FailureCode
		retryable  bool
		httpStatus int32
	}{
		{name: "client error", status: 404, code: sdk.FailureProductHTTPError, httpStatus: 404},
		{name: "server error", status: 503, code: sdk.FailureProductHTTPError, retryable: true, httpStatus: 503},
		{name: "out of range", status: 600, code: sdk.FailureProductResponseFailed},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			host := sdk.NewMemoryHost(func(context.Context, sdk.Request) (sdk.Responses, error) {
				return sdk.NewResponseStream(sdk.Response{
					Status:      test.status,
					ContentType: mediaTypeJSON,
					Body:        []byte(`{"errorCode":"NOT_FOUND"}`),
				}), nil
			})

			err := (Plugin{}).Execute(context.Background(), execution("ledger.v2.stats", nil, sdk.FlagOccurrence{Name: "ledger", Value: "primary"}), host)
			var failure sdk.Failure
			if !errors.As(err, &failure) {
				t.Fatalf("error = %v, want an sdk.Failure", err)
			}
			if failure.Code != string(test.code) || failure.Retryable != test.retryable {
				t.Fatalf("failure = %q retryable %v, want %q retryable %v", failure.Code, failure.Retryable, test.code, test.retryable)
			}
			if test.httpStatus != 0 {
				var details struct {
					HTTPStatus int32 `json:"httpStatus"`
				}
				if err := json.Unmarshal(failure.Details, &details); err != nil {
					t.Fatalf("failure details = %q: %v", failure.Details, err)
				}
				if details.HTTPStatus != test.httpStatus {
					t.Fatalf("failure httpStatus = %d, want %d", details.HTTPStatus, test.httpStatus)
				}
			}
			if len(host.Events()) != 0 {
				t.Fatal("product failure emitted partial output")
			}
			if got := len(host.Requests()); got != 1 {
				t.Fatalf("generated client retried host-owned traffic: %d requests", got)
			}
		})
	}
}

func TestExecuteV2EmitsEmptyAndBinaryResults(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, commandID string
		body            []byte
		media           string
		wantShape       sdk.ResultShape
		wantData        string
	}{
		{name: "empty mutation", commandID: "ledger.v2.delete-metadata", media: "application/json", wantShape: sdk.ResultObject, wantData: "{}"},
		{name: "binary export", commandID: "ledger.v2.export", body: []byte{0, 1, 2}, media: "application/octet-stream", wantShape: sdk.ResultObject, wantData: string([]byte{0, 1, 2})},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			host := sdk.NewMemoryHost(func(context.Context, sdk.Request) (sdk.Responses, error) {
				status := int32(200)
				if test.commandID == "ledger.v2.delete-metadata" {
					status = 204
				}
				return sdk.NewResponseStream(sdk.Response{Status: status, ContentType: test.media, Body: test.body}), nil
			})
			args := []string(nil)
			flags := []sdk.FlagOccurrence{{Name: "ledger", Value: "primary"}}
			if test.commandID == "ledger.v2.delete-metadata" {
				args = []string{"primary", "key"}
				flags = nil
			}
			if err := (Plugin{}).Execute(context.Background(), execution(test.commandID, args, flags...), host); err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			result := host.Events()[0].Result
			if result.OperationID != test.commandID || result.Shape != test.wantShape || string(result.Data) != test.wantData {
				t.Fatalf("result = %#v", result)
			}
		})
	}
}

func TestExecuteV2ExportEnforcesTheExactBinaryResponseCeiling(t *testing.T) {
	command, ok := commandByID("ledger.v2.export")
	if !ok {
		t.Fatal("export command missing")
	}
	limits := command.Operations[0].HTTP.GeneratedClient.ResponseLimits
	if limits.MaxMessageBytes != responseLargeBytes || limits.MaxAggregateBytes != responseLargeBytes {
		t.Fatalf("export response limits = %#v, want %d bytes", limits, responseLargeBytes)
	}

	for _, test := range []struct {
		name    string
		size    int
		wantErr bool
	}{
		{name: "exactly 4 MiB", size: int(responseLargeBytes)},
		{name: "4 MiB plus one", size: int(responseLargeBytes) + 1, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := make([]byte, test.size)
			for index := range payload {
				payload[index] = byte(index % 251)
			}
			host := sdk.NewMemoryHost(func(context.Context, sdk.Request) (sdk.Responses, error) {
				return sdk.NewResponseStream(sdk.Response{Status: 200, ContentType: mediaTypeOctetStream, Body: payload}), nil
			})
			err := (Plugin{}).Execute(context.Background(), execution("ledger.v2.export", nil, sdk.FlagOccurrence{Name: "ledger", Value: "primary"}), host)
			if test.wantErr {
				if err == nil {
					t.Fatal("oversized binary export accepted")
				}
				if len(host.Events()) != 0 {
					t.Fatal("oversized binary export emitted partial output")
				}
				return
			}
			if err != nil {
				t.Fatalf("exact-limit export failed: %v", err)
			}
			if got := host.Events()[0].Result.Data; !slices.Equal(got, payload) {
				t.Fatalf("export payload changed: got %d bytes", len(got))
			}
		})
	}
}

func TestExecuteV2RejectsBrokenPaginationContracts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		pages []string
	}{
		{name: "missing next", pages: []string{`{"cursor":{"data":[],"hasMore":true}}`}},
		{name: "repeated next", pages: []string{`{"cursor":{"data":[],"hasMore":true,"next":"same"}}`, `{"cursor":{"data":[],"hasMore":true,"next":"same"}}`}},
		{name: "malformed", pages: []string{`{`}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			index := 0
			host := sdk.NewMemoryHost(func(context.Context, sdk.Request) (sdk.Responses, error) {
				body := test.pages[index]
				if index+1 < len(test.pages) {
					index++
				}
				return sdk.NewResponseStream(sdk.Response{Status: 200, ContentType: "application/json", Body: []byte(body)}), nil
			})
			request := execution("ledger.v2.list", nil)
			request.Continuation = sdk.AllPagesContinuationControl()
			if err := (Plugin{}).Execute(context.Background(), request, host); err == nil {
				t.Fatal("broken pagination accepted")
			}
			if len(host.Events()) != 0 {
				t.Fatal("broken pagination emitted partial output")
			}
		})
	}
}

type artifactHost struct {
	*sdk.MemoryHost
	chunks []sdk.InputArtifactChunk
	index  int
}

func (h *artifactHost) ReadInput(context.Context, string) (sdk.InputArtifactChunk, error) {
	if h.index >= len(h.chunks) {
		return sdk.InputArtifactChunk{}, errors.New("exhausted")
	}
	chunk := h.chunks[h.index]
	h.index++
	return chunk, nil
}

func TestExecuteV2ReadsImportAndNumscriptArtifacts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		commandID string
		args      []string
		flags     []sdk.FlagOccurrence
		chunks    []sdk.InputArtifactChunk
		wantPath  string
	}{
		{commandID: "ledger.v2.import", args: []string{"primary", "opaque"}, chunks: []sdk.InputArtifactChunk{{Bytes: []byte("log-"), Final: false}, {Bytes: []byte("data"), Final: true}}, wantPath: "/v2/primary/logs/import"},
		{commandID: "ledger.v2.transactions.num", args: []string{"opaque"}, flags: []sdk.FlagOccurrence{{Name: "ledger", Value: "primary"}, {Name: "metadata", Value: "region=eu"}, {Name: "account-var", Value: "origin=users:001"}}, chunks: []sdk.InputArtifactChunk{{Bytes: []byte("send [USD 1]"), Final: true}}, wantPath: "/v2/primary/transactions"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.commandID, func(t *testing.T) {
			host := &artifactHost{chunks: test.chunks}
			host.MemoryHost = sdk.NewMemoryHost(func(_ context.Context, request sdk.Request) (sdk.Responses, error) {
				if request.HTTP == nil || request.HTTP.Path != test.wantPath {
					t.Fatalf("request = %#v", request.HTTP)
				}
				if test.commandID == "ledger.v2.import" {
					if got := string(request.HTTP.Body); got != "log-data" {
						t.Fatalf("import body = %q", got)
					}
					return sdk.NewResponseStream(sdk.Response{Status: 204, ContentType: mediaTypeJSON}), nil
				}
				if !reflect.DeepEqual(jsonKeys(t, request.HTTP.Body), []string{"metadata", "script"}) {
					t.Fatalf("numscript body = %s", request.HTTP.Body)
				}
				body := `{"data":{"timestamp":"2026-09-12T00:00:00Z","postings":[],"metadata":{},"id":1,"reverted":false}}`
				return sdk.NewResponseStream(sdk.Response{Status: 200, ContentType: mediaTypeJSON, Body: []byte(body)}), nil
			})
			if err := (Plugin{}).Execute(context.Background(), execution(test.commandID, test.args, test.flags...), host); err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
		})
	}
}

func TestExecuteV2SendsRequiredEmptyAndCanonicalFilterBodies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		commandID string
		flags     []sdk.FlagOccurrence
		response  string
		wantBody  string
	}{
		{
			name:      "accounts without filters",
			commandID: "ledger.v2.accounts.list",
			response:  `{"cursor":{"data":[],"hasMore":false,"pageSize":15}}`,
			wantBody:  `{"$and":[]}`,
		},
		{
			name:      "account metadata uses query bracket access",
			commandID: "ledger.v2.accounts.list",
			flags:     []sdk.FlagOccurrence{{Name: "metadata", Value: "region=eu"}},
			response:  `{"cursor":{"data":[],"hasMore":false,"pageSize":15}}`,
			wantBody:  `{"$and":[{"$match":{"metadata[region]":"eu"}}]}`,
		},
		{
			name:      "transaction times use timestamp bounds",
			commandID: "ledger.v2.transactions.list",
			flags: []sdk.FlagOccurrence{
				{Name: "start", Value: "2026-09-12T00:00:00Z"},
				{Name: "end", Value: "2026-09-13T00:00:00Z"},
			},
			response: `{"cursor":{"data":[],"hasMore":false,"pageSize":5}}`,
			wantBody: `{"$and":[{"$gte":{"timestamp":"2026-09-12T00:00:00Z"}},{"$lte":{"timestamp":"2026-09-13T00:00:00Z"}}]}`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			host := sdk.NewMemoryHost(func(_ context.Context, request sdk.Request) (sdk.Responses, error) {
				if request.HTTP == nil {
					t.Fatal("missing HTTP request")
				}
				if got := string(request.HTTP.Body); got != test.wantBody {
					t.Fatalf("filter body = %s, want %s", got, test.wantBody)
				}
				return sdk.NewResponseStream(sdk.Response{Status: 200, ContentType: mediaTypeJSON, Body: []byte(test.response)}), nil
			})
			flags := append([]sdk.FlagOccurrence{{Name: "ledger", Value: "primary"}}, test.flags...)
			if err := (Plugin{}).Execute(context.Background(), execution(test.commandID, nil, flags...), host); err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
		})
	}
}

func TestExecuteV2NormalizesLegacyAmountVariablesForNumscript(t *testing.T) {
	t.Parallel()

	host := &artifactHost{chunks: []sdk.InputArtifactChunk{{Bytes: []byte("send [USD 1]"), Final: true}}}
	host.MemoryHost = sdk.NewMemoryHost(func(_ context.Context, request sdk.Request) (sdk.Responses, error) {
		var body struct {
			Script struct {
				Vars map[string]string `json:"vars"`
			} `json:"script"`
		}
		if err := json.Unmarshal(request.HTTP.Body, &body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if got, want := body.Script.Vars["payment"], "USD 10"; got != want {
			t.Fatalf("amount variable = %q, want %q", got, want)
		}
		return sdk.NewResponseStream(sdk.Response{Status: 200, ContentType: mediaTypeJSON, Body: []byte(`{"data":{"timestamp":"2026-09-12T00:00:00Z","postings":[],"metadata":{},"id":1,"reverted":false}}`)}), nil
	})

	err := (Plugin{}).Execute(context.Background(), execution(
		"ledger.v2.transactions.num",
		[]string{"artifact"},
		sdk.FlagOccurrence{Name: "ledger", Value: "primary"},
		sdk.FlagOccurrence{Name: "amount-var", Value: "payment=10/USD"},
	), host)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
}

func TestExecuteV2ReadsSchemaInsertFromAHostOwnedArtifact(t *testing.T) {
	t.Parallel()

	const wantBody = `{"chart":{"users":{".pattern":"^[a-z]+$"}}}`
	for _, test := range []struct {
		name, source string
	}{
		{name: "JSON", source: wantBody},
		{name: "YAML", source: "chart:\n  users:\n    .pattern: ^[a-z]+$\n"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			host := &artifactHost{chunks: []sdk.InputArtifactChunk{{Bytes: []byte(test.source), Final: true}}}
			host.MemoryHost = sdk.NewMemoryHost(func(_ context.Context, request sdk.Request) (sdk.Responses, error) {
				if got := string(request.HTTP.Body); got != wantBody {
					t.Fatalf("schema body = %s, want %s", got, wantBody)
				}
				return sdk.NewResponseStream(sdk.Response{Status: 204, ContentType: mediaTypeJSON}), nil
			})

			if err := (Plugin{}).Execute(context.Background(), execution(
				"ledger.v2.schemas.insert",
				[]string{"v1", "schema-handle"},
				sdk.FlagOccurrence{Name: "ledger", Value: "primary"},
			), host); err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
		})
	}
}

func TestExecuteV2ResolvesLegacyRelativeTransactionIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		commandID string
		args      []string
		flags     []sdk.FlagOccurrence
		operation string
	}{
		{name: "show", commandID: "ledger.v2.transactions.show", args: []string{"last-2"}, operation: "v2GetTransaction"},
		{name: "set metadata", commandID: "ledger.v2.transactions.set-metadata", args: []string{"last-2", "region=eu"}, operation: "v2AddMetadataOnTransaction"},
		{name: "delete metadata", commandID: "ledger.v2.transactions.delete-metadata", args: []string{"last-2", "region"}, operation: "v2DeleteTransactionMetadata"},
		{name: "revert", commandID: "ledger.v2.transactions.revert", args: []string{"last-2"}, operation: "v2RevertTransaction"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var operations []string
			host := sdk.NewMemoryHost(func(_ context.Context, request sdk.Request) (sdk.Responses, error) {
				operations = append(operations, request.Operation)
				switch request.Operation {
				case "v2ListTransactions":
					if request.HTTP == nil || request.HTTP.Path != "/v2/primary/transactions" || string(request.HTTP.Body) != `{"$and":[]}` || !reflect.DeepEqual(request.HTTP.Query, map[string][]string{"pageSize": {"1"}, "sort": {"id:desc"}}) {
						t.Fatalf("relative-ID lookup request = %#v", request.HTTP)
					}
					return sdk.NewResponseStream(sdk.Response{Status: 200, ContentType: mediaTypeJSON, Body: []byte(`{"cursor":{"data":[{"timestamp":"2026-09-12T00:00:00Z","postings":[],"metadata":{},"id":9,"reverted":false}],"hasMore":false,"pageSize":1}}`)}), nil
				case test.operation:
					if request.HTTP == nil || !strings.Contains(request.HTTP.Path, "/transactions/7") {
						t.Fatalf("resolved transaction request = %#v", request.HTTP)
					}
					if test.operation == "v2GetTransaction" || test.operation == "v2RevertTransaction" {
						status := int32(200)
						if test.operation == "v2RevertTransaction" {
							status = 201
						}
						return sdk.NewResponseStream(sdk.Response{Status: status, ContentType: mediaTypeJSON, Body: []byte(`{"data":{"timestamp":"2026-09-12T00:00:00Z","postings":[],"metadata":{},"id":7,"reverted":false}}`)}), nil
					}
					return sdk.NewResponseStream(sdk.Response{Status: 204, ContentType: mediaTypeJSON}), nil
				default:
					t.Fatalf("unexpected operation %q", request.Operation)
					return nil, nil
				}
			})
			flags := append([]sdk.FlagOccurrence{{Name: "ledger", Value: "primary"}}, test.flags...)
			if err := (Plugin{}).Execute(context.Background(), execution(test.commandID, test.args, flags...), host); err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if !reflect.DeepEqual(operations, []string{"v2ListTransactions", test.operation}) {
				t.Fatalf("operations = %v", operations)
			}
		})
	}
}

func TestExecuteV2RejectsMalformedRelativeTransactionIDBeforeTraffic(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"last-", "last-nope"} {
		host := sdk.NewMemoryHost(nil)
		err := (Plugin{}).Execute(context.Background(), execution(
			"ledger.v2.transactions.show",
			[]string{value},
			sdk.FlagOccurrence{Name: "ledger", Value: "primary"},
		), host)
		if err == nil {
			t.Fatalf("relative transaction ID %q accepted", value)
		}
		if got := len(host.Requests()); got != 0 {
			t.Fatalf("relative transaction ID %q made %d requests", value, got)
		}
	}
}

func TestExecuteV2ResumesImportAfterTheLatestProductLog(t *testing.T) {
	t.Parallel()

	host := &artifactHost{
		MemoryHost: sdk.NewMemoryHost(func(_ context.Context, request sdk.Request) (sdk.Responses, error) {
			switch request.Operation {
			case "v2ListLogs":
				if request.HTTP == nil || request.HTTP.Method != "GET" || request.HTTP.Path != "/v2/primary/logs" || !reflect.DeepEqual(request.HTTP.Query, map[string][]string{"pageSize": {"1"}}) {
					t.Fatalf("resume probe = %#v", request.HTTP)
				}
				return sdk.NewResponseStream(sdk.Response{Status: 200, ContentType: mediaTypeJSON, Body: []byte(`{"cursor":{"data":[{"id":2,"type":"SET_METADATA","data":{"targetType":"ACCOUNT","targetId":"users:001","metadata":{}},"hash":"sha256","date":"2026-09-12T00:00:00Z"}],"pageSize":1,"hasMore":false}}`)}), nil
			case "v2ImportLogs":
				if got, want := string(request.HTTP.Body), "{\"id\":3}\n"; got != want {
					t.Fatalf("resumed import body = %q, want %q", got, want)
				}
				return sdk.NewResponseStream(sdk.Response{Status: 204, ContentType: mediaTypeJSON}), nil
			default:
				t.Fatalf("unexpected operation %q", request.Operation)
				return nil, nil
			}
		}),
		chunks: []sdk.InputArtifactChunk{{Bytes: []byte("{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n"), Final: true}},
	}
	request := execution("ledger.v2.import", []string{"primary", "artifact"}, sdk.FlagOccurrence{Name: "resume-from-last-log", Value: "true"})
	if err := (Plugin{}).Execute(context.Background(), request, host); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := len(host.Requests()); got != 2 {
		t.Fatalf("host requests = %d, want 2", got)
	}
	result := host.Events()[0].Result
	if result.OperationID != request.CommandID || result.Shape != sdk.ResultObject || string(result.Data) != `{}` {
		t.Fatalf("result = %#v", result)
	}
}

func TestExecuteV2NamesTheResumeProbeOnProductFailure(t *testing.T) {
	t.Parallel()

	host := &artifactHost{
		MemoryHost: sdk.NewMemoryHost(func(_ context.Context, request sdk.Request) (sdk.Responses, error) {
			if request.Operation != "v2ListLogs" {
				t.Fatalf("operation = %q, want v2ListLogs", request.Operation)
			}
			return nil, errors.New("offline")
		}),
		chunks: []sdk.InputArtifactChunk{{Bytes: []byte("{\"id\":1}\n"), Final: true}},
	}
	err := (Plugin{}).Execute(context.Background(), execution(
		"ledger.v2.import",
		[]string{"primary", "artifact"},
		sdk.FlagOccurrence{Name: "resume-from-last-log", Value: "true"},
	), host)
	if err == nil {
		t.Fatal("resume probe failure was hidden")
	}
	if got := err.Error(); !strings.Contains(got, "v2ListLogs") || strings.Contains(got, "v2ImportLogs") {
		t.Fatalf("resume probe error = %q, want only the executed v2ListLogs operation", got)
	}
}

func TestExecuteV2ChunksImportAtOneHundredLogsAcrossArtifactChunks(t *testing.T) {
	t.Parallel()

	var source strings.Builder
	for id := 1; id <= 101; id++ {
		fmt.Fprintf(&source, "{\"id\":%d}\n", id)
	}
	payload := source.String()
	split := strings.Index(payload, `{"id":51}`) + 5
	host := &artifactHost{chunks: []sdk.InputArtifactChunk{
		{Bytes: []byte(payload[:split]), Final: false},
		{Bytes: []byte(payload[split:]), Final: true},
	}}
	var bodies []string
	host.MemoryHost = sdk.NewMemoryHost(func(_ context.Context, request sdk.Request) (sdk.Responses, error) {
		if request.Operation != "v2ImportLogs" {
			t.Fatalf("operation = %q", request.Operation)
		}
		bodies = append(bodies, string(request.HTTP.Body))
		return sdk.NewResponseStream(sdk.Response{Status: 204, ContentType: mediaTypeJSON}), nil
	})

	if err := (Plugin{}).Execute(context.Background(), execution("ledger.v2.import", []string{"primary", "artifact"}), host); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := len(bodies); got != 2 {
		t.Fatalf("import requests = %d, want 2", got)
	}
	if got := strings.Count(bodies[0], "\n"); got != 100 {
		t.Fatalf("first request log count = %d, want 100", got)
	}
	if got, want := bodies[1], "{\"id\":101}\n"; got != want {
		t.Fatalf("second request = %q, want %q", got, want)
	}
}

func TestSplitImportBatchesEnforcesThePerRequestByteCeiling(t *testing.T) {
	t.Parallel()

	atLimit := bytes.Repeat([]byte{'x'}, int(requestBulkBytes))
	batches, err := splitImportBatches(atLimit)
	if err != nil {
		t.Fatalf("exact-limit log rejected: %v", err)
	}
	if len(batches) != 1 || len(batches[0]) != len(atLimit) {
		t.Fatalf("exact-limit batches = %d/%d", len(batches), len(batches[0]))
	}
	if _, err := splitImportBatches(append(atLimit, 'x')); err == nil {
		t.Fatal("oversized individual log accepted")
	}
}

func TestExecuteV2RejectsImportBeyondTheRequestBudgetBeforeTraffic(t *testing.T) {
	t.Parallel()

	var source strings.Builder
	for id := 1; id <= 25_601; id++ {
		fmt.Fprintf(&source, "{\"id\":%d}\n", id)
	}
	host := &artifactHost{
		MemoryHost: sdk.NewMemoryHost(func(context.Context, sdk.Request) (sdk.Responses, error) {
			t.Fatal("over-budget import reached product traffic")
			return nil, nil
		}),
		chunks: []sdk.InputArtifactChunk{{Bytes: []byte(source.String()), Final: true}},
	}

	if err := (Plugin{}).Execute(context.Background(), execution("ledger.v2.import", []string{"primary", "artifact"}), host); err == nil {
		t.Fatal("over-budget import accepted")
	}
	if got := len(host.Requests()); got != 0 {
		t.Fatalf("over-budget import made %d requests", got)
	}
}

func jsonKeys(t *testing.T, body []byte) []string {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(body, &value); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
