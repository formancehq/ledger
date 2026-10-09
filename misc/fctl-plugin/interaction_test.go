package ledger

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/formancehq/fctl/pkg/pluginsdk"
)

func TestLedgerInteractionCoverage(t *testing.T) {
	t.Parallel()
	m, operations := buildLayout()
	if m.Root.Target != "stack" {
		t.Fatal("Ledger must target stack context")
	}
	walkLedgerInputs(t, m.Root)
	for path, op := range operations {
		command, err := pluginsdk.FindCommand(m, strings.Split(path, "/"))
		if err != nil {
			t.Fatal(err)
		}
		checkLedgerTargetInput(t, command, op)
		checkArgumentCoverage(t, command, op)
		for _, input := range command.Inputs {
			checkLedgerInputContract(t, m, command, input)
		}
	}
}

func walkLedgerInputs(t *testing.T, command pluginsdk.CommandSpec) {
	t.Helper()
	if !command.Runnable && len(command.Inputs) != 0 {
		t.Fatalf("group %s declares inherited inputs", command.Use)
	}
	for _, child := range command.Subcommands {
		walkLedgerInputs(t, child)
	}
}

func checkLedgerTargetInput(t *testing.T, command pluginsdk.CommandSpec, op operation) {
	t.Helper()
	index := slices.IndexFunc(command.Inputs, func(input pluginsdk.InputSpec) bool { return input.Flag == "ledger" })
	if op.global {
		if index >= 0 {
			t.Fatalf("global %s asks for a ledger", command.Use)
		}

		return
	}
	if index != 0 || !command.Inputs[index].Required {
		t.Fatalf("%s must resolve the ledger first", command.Use)
	}
	input := command.Inputs[index]
	if op.ledgerArg && (input.AlternativeArgument == nil || *input.AlternativeArgument != 0) {
		t.Fatalf("%s lost positional ledger binding", command.Use)
	}
	if op.ledgerArg && op.method == http.MethodPost {
		if input.Kind != "input" || input.Title != "Ledger name" || input.Source != nil {
			t.Fatal("ledger creation must ask for a new name")
		}

		return
	}
	if input.Kind != "select" || input.Source == nil || input.Source.ValueField != "name" || !slices.Equal(input.Source.CommandPath, []string{"ledger", "list"}) {
		t.Fatalf("invalid ledger selector: %#v", input)
	}
}

func checkArgumentCoverage(t *testing.T, command pluginsdk.CommandSpec, op operation) {
	t.Helper()
	if op.ledgerArg {
		return
	}
	for i := range op.args {
		index := slices.IndexFunc(command.Inputs, func(input pluginsdk.InputSpec) bool { return input.Argument != nil && *input.Argument == i })
		if index < 0 || !command.Inputs[index].Required {
			t.Fatalf("%s is missing required argument %d", command.Use, i)
		}
	}
}

func checkLedgerInputContract(t *testing.T, m pluginsdk.Manifest, command pluginsdk.CommandSpec, input pluginsdk.InputSpec) {
	t.Helper()
	bindings := 0
	for _, bound := range []bool{input.Flag != "", input.Argument != nil, input.BodyPointer != "", input.Context != ""} {
		if bound {
			bindings++
		}
	}
	if bindings != 1 || input.Title == "" || !slices.Contains([]string{"input", "text", "select", "confirm"}, input.Kind) || !slices.Contains([]string{"", "string", "json", "bool", "number"}, input.ValueType) {
		t.Fatalf("invalid input: %#v", input)
	}
	if input.Argument != nil && (*input.Argument < 0 || *input.Argument >= command.Args.Max) {
		t.Fatalf("argument binding out of range: %#v", input)
	}
	if input.Flag != "" && input.Flag != "ledger" && !slices.ContainsFunc(command.Flags, func(flag pluginsdk.FlagSpec) bool { return flag.Name == input.Flag }) {
		t.Fatalf("unknown flag binding: %#v", input)
	}
	if input.BodyPointer != "" {
		checkLedgerBodyPointer(t, input.BodyPointer)
	}
	if input.Source != nil {
		checkLedgerSourceContract(t, m, input.Source)
	}
}

func checkLedgerBodyPointer(t *testing.T, pointer string) {
	t.Helper()
	if !strings.HasPrefix(pointer, "/") {
		t.Fatalf("invalid JSON pointer %q", pointer)
	}
	for i := 0; i < len(pointer); i++ {
		if pointer[i] == '~' {
			i++
			if i >= len(pointer) || (pointer[i] != '0' && pointer[i] != '1') {
				t.Fatalf("invalid JSON pointer escape %q", pointer)
			}
		}
	}
}

func checkLedgerSourceContract(t *testing.T, m pluginsdk.Manifest, source *pluginsdk.ChoiceSource) {
	t.Helper()
	command, err := pluginsdk.FindCommand(m, source.CommandPath)
	if err != nil || !command.Runnable || command.Confirm || len(source.Args) != command.Args.Min || source.ValueField == "" || source.EmptyMessage == "" {
		t.Fatalf("invalid source: %#v, %v", source, err)
	}
	_, operations := buildLayout()
	if operations[strings.Join(source.CommandPath, "/")].method != http.MethodGet {
		t.Fatalf("source can mutate resources: %#v", source)
	}
	continuationFields := map[string]string{
		"ledger/accounts/list":     "address",
		"ledger/transactions/list": "id",
	}
	if source.AfterField != continuationFields[strings.Join(source.CommandPath, "/")] {
		t.Fatalf("incorrect pagination descriptor: %#v", source)
	}
	for flag, value := range source.Flags {
		if flag != "ledger" || value != "$ledger" {
			t.Fatalf("unexpected source flag %s=%s", flag, value)
		}
	}
}

type ledgerChoiceCase struct {
	command, field, response, want, path string
}

func TestLedgerChoiceSourcesPreserveExactValues(t *testing.T) {
	t.Parallel()
	for _, tc := range []ledgerChoiceCase{
		{"show", "name", `{"data":[{"name":"books"}]}`, "books", "/v3/"},
		{"accounts show", "address", `{"data":[{"address":"users:a/b% c"}]}`, "users:a/b% c", "/v3/books/accounts"},
		{"transactions show", "id", `{"data":[{"id":18446744073709551615,"reference":"payment-42"}]}`, "18446744073709551615", "/v3/books/transactions"},
		{"transactions show", "id", `{"data":[{"id":"18446744073709551615","reference":"payment-42"}]}`, "18446744073709551615", "/v3/books/transactions"},
	} {
		t.Run(tc.command, func(t *testing.T) { checkLedgerChoice(t, tc) })
	}
}

func checkLedgerChoice(t *testing.T, tc ledgerChoiceCase) {
	t.Helper()
	command, err := pluginsdk.FindCommand(manifestForInputs(), strings.Fields("ledger "+tc.command))
	if err != nil {
		t.Fatal(err)
	}
	index := slices.IndexFunc(command.Inputs, func(input pluginsdk.InputSpec) bool {
		return input.Source != nil && input.Source.ValueField == tc.field
	})
	if index < 0 {
		t.Fatal("missing choice source")
	}
	result := executeLedgerChoice(t, command.Inputs[index].Source, tc.path, tc.response)
	decoder := json.NewDecoder(bytes.NewReader(result))
	decoder.UseNumber()
	var envelope struct {
		Data []map[string]any `json:"data"`
	}
	if err := decoder.Decode(&envelope); err != nil || len(envelope.Data) != 1 {
		t.Fatalf("source result=%s, error=%v", result, err)
	}
	value := exactChoiceValue(t, envelope.Data[0][tc.field])
	if value != tc.want {
		t.Fatalf("choice value=%q, want %q", value, tc.want)
	}
	if tc.field == "id" && validateArgs(operation{transactionID: true}, []string{value}) != nil {
		t.Fatal("exact selected transaction ID is invalid")
	}
}

func manifestForInputs() pluginsdk.Manifest {
	m, _ := buildLayout()

	return m
}

func executeLedgerChoice(t *testing.T, source *pluginsdk.ChoiceSource, path, body string) json.RawMessage {
	t.Helper()
	calls := 0
	p := New(&http.Client{Transport: transportFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if req.Method != http.MethodGet || req.URL.Path != "/gateway/ledger"+path || req.Context() != t.Context() {
			t.Fatalf("choice request=%s %s", req.Method, req.URL)
		}

		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})})
	flags := make(map[string]string)
	for flag, value := range source.Flags {
		if value != "$ledger" {
			t.Fatalf("unknown substitution %q", value)
		}
		flags[flag] = "books"
	}
	result, err := p.Execute(t.Context(), pluginsdk.ExecuteRequest{CommandPath: source.CommandPath, Args: source.Args, Flags: flags, Endpoint: "https://example.test/gateway/ledger"})
	if err != nil || calls != 1 {
		t.Fatalf("source calls=%d, error=%v", calls, err)
	}

	return result.Data
}

func exactChoiceValue(t *testing.T, value any) string {
	t.Helper()
	switch value := value.(type) {
	case string:
		return value
	case json.Number:
		return value.String()
	default:
		t.Fatalf("unsupported choice value %T", value)

		return ""
	}
}

func TestLedgerStructuredBodyAndAdvancedJSONInputs(t *testing.T) {
	t.Parallel()
	m := manifestForInputs()
	command, err := pluginsdk.FindCommand(m, strings.Fields("ledger transactions create"))
	if err != nil {
		t.Fatal(err)
	}
	fields := map[string]pluginsdk.InputSpec{}
	for _, input := range command.Inputs {
		if input.BodyPointer != "" {
			fields[input.BodyPointer] = input
		}
	}
	if len(fields) != 4 || fields["/script/plain"].Kind != "text" || !fields["/script/plain"].Required || !strings.Contains(fields["/script/plain"].Default, "send [USD/2 100]") {
		t.Fatalf("invalid Numscript form: %#v", fields)
	}
	if fields["/reference"].Required || fields["/metadata"].Required || fields["/metadata"].Default != "" || fields["/metadata"].ValueType != "json" {
		t.Fatal("optional transaction fields cannot be forced")
	}
	if fields["/force"].Kind != "confirm" || fields["/force"].ValueType != "bool" || fields["/force"].Default != "false" {
		t.Fatal("force must be an explicit boolean choice")
	}
	for _, path := range []string{"ledger metadata set", "ledger accounts metadata set", "ledger transactions metadata set", "ledger bulk"} {
		checkAdvancedJSONInput(t, m, path)
	}
	index, err := pluginsdk.FindCommand(m, strings.Fields("ledger indexes create"))
	if err != nil {
		t.Fatal(err)
	}
	input := index.Inputs[1]
	if input.BodyPointer != "/id" || input.Kind != "input" || input.Default != "log_builtin:LOG_BUILTIN_INDEX_DATE" || input.Source != nil {
		t.Fatal("new indexes must accept a canonical ID that does not exist yet")
	}
}

func checkAdvancedJSONInput(t *testing.T, m pluginsdk.Manifest, path string) {
	t.Helper()
	command, err := pluginsdk.FindCommand(m, strings.Fields(path))
	if err != nil {
		t.Fatal(err)
	}
	index := slices.IndexFunc(command.Inputs, func(input pluginsdk.InputSpec) bool { return input.Flag == "data" })
	if index < 0 {
		t.Fatalf("%s has no guided JSON editor", path)
	}
	input := command.Inputs[index]
	if input.Kind != "text" || input.ValueType != "json" || !input.Required || !json.Valid([]byte(input.Default)) {
		t.Fatalf("invalid JSON input: %#v", input)
	}
	if path == "ledger bulk" && !strings.HasPrefix(input.Default, "[") {
		t.Fatal("bulk requires an operation array")
	}
	if path != "ledger bulk" && !strings.HasPrefix(input.Default, "{") {
		t.Fatal("metadata requires a JSON object")
	}
}

func TestLedgerCreationSettingsMatchV3Contract(t *testing.T) {
	t.Parallel()
	command, err := pluginsdk.FindCommand(manifestForInputs(), []string{"ledger", "create"})
	if err != nil {
		t.Fatal(err)
	}
	if len(command.Inputs) != 5 {
		t.Fatalf("creation form inputs=%d, want name and four settings", len(command.Inputs))
	}
	enforcement := command.Inputs[1]
	if enforcement.BodyPointer != "/defaultEnforcementMode" || enforcement.Kind != "select" || enforcement.Default != "STRICT" {
		t.Fatalf("incorrect server default: %#v", enforcement)
	}
	values := make([]string, 0, len(enforcement.Options))
	for _, option := range enforcement.Options {
		values = append(values, option.Value)
	}
	if !slices.Equal(values, []string{"STRICT", "AUDIT"}) {
		t.Fatalf("unsupported enforcement modes: %v", values)
	}
	for i, pointer := range []string{"/metadata", "/initialSchema", "/accountTypes"} {
		input := command.Inputs[i+2]
		if input.BodyPointer != pointer || input.Kind != "text" || input.ValueType != "json" || input.Required || input.Default != "" {
			t.Fatalf("optional creation setting must stay omitted when blank: %#v", input)
		}
	}
	// This payload follows the release/v3.0 HTTP createLedgerBody and its
	// initial-metadata fixture. Integer metadata is never decoded as float64.
	body := `{"defaultEnforcementMode":"STRICT","metadata":{"count":9007199254740993,"purpose":"tests","enabled":true},"initialSchema":[{"targetType":"account","key":"color","type":"string"}],"accountTypes":{"user-checking":{"name":"user-checking","pattern":"users:{id}:checking","persistence":"EPHEMERAL","segmentTypes":{"id":{"type":"uint64"}}}}}`
	calls := 0
	p := New(&http.Client{Transport: transportFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		data, err := io.ReadAll(req.Body)
		if err != nil || string(data) != body || req.Method != http.MethodPost || req.URL.Path != "/v3/books" {
			t.Fatalf("create request=%s %s body=%s, error=%v", req.Method, req.URL, data, err)
		}

		return &http.Response{StatusCode: http.StatusCreated, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"data":{"name":"books"}}`))}, nil
	})})
	_, err = p.Execute(t.Context(), pluginsdk.ExecuteRequest{CommandPath: []string{"ledger", "create"}, Args: []string{"books"}, Body: json.RawMessage(body), Endpoint: "https://example.test"})
	if err != nil || calls != 1 {
		t.Fatalf("create calls=%d, error=%v", calls, err)
	}
}

func TestV3ListResponseWithoutPaginationEnvelope(t *testing.T) {
	t.Parallel()
	for _, tc := range []ledgerChoiceCase{
		{"accounts show", "address", `{"data":[{"address":"users:next"}]}`, "users:next", "/v3/books/accounts"},
		{"transactions show", "id", `{"data":[{"id":9007199254740993}]}`, "9007199254740993", "/v3/books/transactions"},
	} {
		t.Run(tc.command, func(t *testing.T) {
			m := manifestForInputs()
			command, err := pluginsdk.FindCommand(m, strings.Fields("ledger "+tc.command))
			if err != nil {
				t.Fatal(err)
			}
			source := command.Inputs[1].Source
			data := executeLedgerChoice(t, source, tc.path, tc.response)
			var envelope map[string]json.RawMessage
			if err := json.Unmarshal(data, &envelope); err != nil || len(envelope) != 1 || envelope["data"] == nil {
				t.Fatalf("v3 list envelope=%s, error=%v", data, err)
			}
			request, err := pluginsdk.NormalizeRequest(m, pluginsdk.ExecuteRequest{CommandPath: source.CommandPath, Flags: map[string]string{"ledger": "books", "after": tc.want}})
			if err != nil || request.Flags["after"] != tc.want {
				t.Fatalf("last item cannot be used as after=%s: %v", tc.want, err)
			}
			cursor := base64.RawURLEncoding.EncodeToString([]byte(`{"key":"` + tc.want + `"}`))
			request, err = pluginsdk.NormalizeRequest(m, pluginsdk.ExecuteRequest{CommandPath: source.CommandPath, Flags: map[string]string{"ledger": "books", "cursor": cursor}})
			if err != nil || request.Flags["cursor"] != cursor {
				t.Fatalf("entity list rejected a page cursor: request=%#v err=%v", request, err)
			}
			tokens := append([]string{"--ledger", "books"}, source.CommandPath[1:]...)
			assertPaginationRejected(t, append(tokens, "--cursor", tc.want), "invalid --cursor")
		})
	}
}
