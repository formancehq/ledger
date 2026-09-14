package ledgerv2

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/ledger/pkg/client/models/components"
)

// renderOracle is the committed, ordered table layout for every command whose
// emitted result carries field evidence. It is the oracle: a descriptor that
// disagrees with it is a surface change, not a formatting detail.
//
// Each entry is {header, field}. A field is a dot-separated path into one
// emitted result element; every path below happens to be a single top-level
// segment because no Ledger v2 result exposes a statically named nested scalar
// (see TestRenderHintFieldsNameNoDynamicOrCompositeProjection).
var renderOracle = map[string][][2]string{
	"ledger.v2.list": {
		{"Name", "name"}, {"Bucket", "bucket"}, {"Added At", "addedAt"},
	},
	"ledger.v2.stats": {
		{"Accounts", "accounts"}, {"Transactions", "transactions"},
	},
	"ledger.v2.accounts.list": {
		{"Address", "address"}, {"Insertion Date", "insertionDate"}, {"Updated At", "updatedAt"},
	},
	"ledger.v2.accounts.show": {
		{"Address", "address"}, {"Insertion Date", "insertionDate"}, {"Updated At", "updatedAt"},
	},
	"ledger.v2.transactions.list": {
		{"ID", "id"}, {"Timestamp", "timestamp"}, {"Reference", "reference"}, {"Reverted", "reverted"},
	},
	"ledger.v2.transactions.show": {
		{"ID", "id"}, {"Timestamp", "timestamp"}, {"Reference", "reference"}, {"Reverted", "reverted"},
	},
	"ledger.v2.transactions.num": {
		{"ID", "id"}, {"Timestamp", "timestamp"}, {"Reference", "reference"}, {"Reverted", "reverted"},
	},
	"ledger.v2.transactions.revert": {
		{"ID", "id"}, {"Timestamp", "timestamp"}, {"Reference", "reference"}, {"Reverted", "reverted"},
	},
	"ledger.v2.send": {
		{"ID", "id"}, {"Timestamp", "timestamp"}, {"Reference", "reference"}, {"Reverted", "reverted"},
	},
	"ledger.v2.volumes.list": {
		{"Account", "account"}, {"Asset", "asset"}, {"Input", "input"}, {"Output", "output"}, {"Balance", "balance"},
	},
	"ledger.v2.schemas.list": {
		{"Version", "version"}, {"Created At", "createdAt"},
	},
	"ledger.v2.schemas.get": {
		{"Version", "version"}, {"Created At", "createdAt"},
	},
}

// renderAbsent lists every command whose result carries no field evidence at
// all: nine publish the empty object because their product operation returns no
// content, and `ledger export` publishes an opaque base64 payload. Inventing a
// column for either would name a field the plugin never emits.
var renderAbsent = []string{
	"ledger.v2.accounts.delete-metadata",
	"ledger.v2.accounts.set-metadata",
	"ledger.v2.create",
	"ledger.v2.delete-metadata",
	"ledger.v2.export",
	"ledger.v2.import",
	"ledger.v2.schemas.insert",
	"ledger.v2.set-metadata",
	"ledger.v2.transactions.delete-metadata",
	"ledger.v2.transactions.set-metadata",
}

func TestRenderHintsPartitionTheWholeCatalogue(t *testing.T) {
	t.Parallel()

	commands := Commands()
	if len(commands) != 22 {
		t.Fatalf("Commands() = %d, want the 22-command denominator", len(commands))
	}
	for _, command := range commands {
		_, hinted := renderOracle[command.ID]
		absent := slices.Contains(renderAbsent, command.ID)
		if hinted == absent {
			t.Errorf("%s: classified as hinted=%t absent=%t, want exactly one", command.ID, hinted, absent)
		}
	}
	if got := len(renderOracle) + len(renderAbsent); got != 22 {
		t.Fatalf("render classification covers %d commands, want 22", got)
	}
}

// The manifest carries the split as a machine-checked invariant, so a
// descriptor that grows or loses a hint fails here rather than leaving the
// manifest quietly stale.
func TestManifestRecordsTheRenderHintCount(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile("manifest.json")
	if err != nil {
		t.Fatalf("read manifest.json: %v", err)
	}
	var manifest struct {
		Baseline struct {
			ExecutableCommands int `json:"executable_commands"`
			V1OnlyAtBaseline   int `json:"v1_only_at_baseline"`
			ConvertedV1ToV2    int `json:"converted_v1_to_v2"`
			ExcludedHostOwned  int `json:"excluded_host_owned"`
		} `json:"baseline"`
		CoverageDenominator  int      `json:"coverage_denominator"`
		RenderHintedCommands int      `json:"render_hinted_commands"`
		Invariants           []string `json:"invariants"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("decode manifest.json: %v", err)
	}

	hinted := 0
	for _, command := range Commands() {
		if command.Render.Table != nil {
			hinted++
		}
	}
	if hinted != manifest.RenderHintedCommands {
		t.Errorf("catalogue declares %d render hints, manifest records %d", hinted, manifest.RenderHintedCommands)
	}
	if manifest.CoverageDenominator != len(Commands()) {
		t.Errorf("manifest denominator = %d, catalogue = %d", manifest.CoverageDenominator, len(Commands()))
	}
	inventory := loadInventory(t)
	if manifest.Baseline.ExecutableCommands != inventory.Counts.BaselineExecutable ||
		manifest.Baseline.V1OnlyAtBaseline != inventory.Counts.BaselineV1Only ||
		manifest.Baseline.ConvertedV1ToV2 != inventory.Counts.ConvertedV1ToV2 ||
		manifest.Baseline.ExcludedHostOwned != inventory.Counts.ExcludedHostOwned ||
		manifest.CoverageDenominator != inventory.Counts.Included {
		t.Errorf("manifest baseline/denominator = %#v/%d, inventory counts = %#v", manifest.Baseline, manifest.CoverageDenominator, inventory.Counts)
	}
	if !slices.Contains(manifest.Invariants, "publishes a table render hint only where the emitted result proves the field exists") {
		t.Error("manifest does not record the render-hint invariant")
	}
}

func TestRenderHintsDeclareExactOrderedHeadersAndFields(t *testing.T) {
	t.Parallel()

	for id, want := range renderOracle {
		command, ok := commandByID(id)
		if !ok {
			t.Errorf("%s: command missing", id)
			continue
		}
		if command.Render.Table == nil {
			t.Errorf("%s: declares no table render hint", id)
			continue
		}
		got := make([][2]string, 0, len(command.Render.Table.Columns))
		for _, column := range command.Render.Table.Columns {
			got = append(got, [2]string{column.Header, column.Field})
		}
		if !slices.Equal(got, want) {
			t.Errorf("%s: columns = %v, want %v", id, got, want)
		}
	}
}

func TestCommandsWithoutResultFieldEvidenceDeclareNoRenderHint(t *testing.T) {
	t.Parallel()

	for _, id := range renderAbsent {
		command, ok := commandByID(id)
		if !ok {
			t.Errorf("%s: command missing", id)
			continue
		}
		if command.Render.Table != nil {
			t.Errorf("%s: declares a table render hint for a result carrying no fields", id)
		}
	}
}

// blockedRenderFields are the emitted members a column must never project: a
// dynamic key/value blob, a nested volume aggregation, or a composite the host
// would have to serialise back into a cell.
var blockedRenderFields = []string{
	"metadata", "features", "volumes", "effectiveVolumes",
	"preCommitVolumes", "postCommitVolumes",
	"preCommitEffectiveVolumes", "postCommitEffectiveVolumes",
	"postings", "chart", "transactions", "queries",
}

func TestRenderHintFieldsNameNoDynamicOrCompositeProjection(t *testing.T) {
	t.Parallel()

	for id, columns := range renderOracle {
		// `ledger stats` publishes a scalar transaction count, not the
		// transaction-template map `ledger schemas get` publishes.
		blocked := blockedRenderFields
		if id == "ledger.v2.stats" {
			blocked = slices.DeleteFunc(slices.Clone(blockedRenderFields), func(name string) bool { return name == "transactions" })
		}
		for _, column := range columns {
			for _, segment := range strings.Split(column[1], ".") {
				if slices.Contains(blocked, segment) {
					t.Errorf("%s: column %q projects the blocked member %q", id, column[1], segment)
				}
			}
		}
	}
}

func resolveRenderSchemaField(schema map[string]any, field string) (map[string]any, bool) {
	current := schema
	if root, _ := current["type"].(string); root == "array" {
		items, ok := current["items"].(map[string]any)
		if !ok {
			return nil, false
		}
		current = items
	}
	for _, segment := range strings.Split(field, ".") {
		properties, ok := current["properties"].(map[string]any)
		if !ok {
			return nil, false
		}
		next, ok := properties[segment].(map[string]any)
		if !ok {
			return nil, false
		}
		current = next
	}
	return current, true
}

func isRenderScalarSchema(schema map[string]any) bool {
	isScalarType := func(value string) bool {
		return value == "string" || value == "integer" || value == "number" || value == "boolean" || value == "null"
	}
	switch value := schema["type"].(type) {
	case string:
		return isScalarType(value)
	case []any:
		if len(value) == 0 {
			return false
		}
		for _, member := range value {
			name, ok := member.(string)
			if !ok || !isScalarType(name) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func TestRenderHintsCohereWithThePublishedOutputSchema(t *testing.T) {
	t.Parallel()

	for _, command := range Commands() {
		if !bytes.Equal(command.RawOutputSchema, command.PublicOutputSchema) {
			t.Errorf("%s: raw and public output schemas diverge", command.ID)
		}
		var schema map[string]any
		if err := json.Unmarshal(command.PublicOutputSchema, &schema); err != nil {
			t.Fatalf("%s: decode public output schema: %v", command.ID, err)
		}
		root, _ := schema["type"].(string)
		if command.Render.Table == nil {
			continue
		}
		// A render hint describes the columns of one result element, so the
		// schema root must be a JSON document the host can project: an array of
		// elements, or one object element. A base64 string root cannot be.
		if root != "array" && root != "object" {
			t.Errorf("%s: render hint on a %q-rooted output schema", command.ID, root)
		}
		if command.Pagination.Supported && root != "array" {
			t.Errorf("%s: paginated command has a %q-rooted output schema", command.ID, root)
		}
		for _, column := range command.Render.Table.Columns {
			leaf, found := resolveRenderSchemaField(schema, column.Field)
			if !found {
				t.Errorf("%s: column %q (%q) is absent from the public output schema", command.ID, column.Header, column.Field)
				continue
			}
			if !isRenderScalarSchema(leaf) {
				t.Errorf("%s: column %q (%q) resolves to non-scalar schema %#v", command.ID, column.Header, column.Field, leaf)
			}
		}
	}
}

// resolveRenderField walks one dot-separated TableColumn.Field path through a
// decoded result element. It is the traversal the coherence tests owe the
// declared contract: a field MAY name a nested scalar leaf, so a test that only
// looked at top-level keys would silently accept an unreachable dotted path.
func resolveRenderField(element any, field string) (any, bool) {
	current := element
	for _, segment := range strings.Split(field, ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[segment]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func isRenderScalar(value any) bool {
	switch value.(type) {
	case map[string]any, []any:
		return false
	default:
		return true
	}
}

func renderSchemaAllowsScalar(schema map[string]any, value any) bool {
	allows := func(name string) bool {
		switch name {
		case "null":
			return value == nil
		case "string":
			_, ok := value.(string)
			return ok
		case "boolean":
			_, ok := value.(bool)
			return ok
		case "number":
			_, ok := value.(json.Number)
			return ok
		case "integer":
			number, ok := value.(json.Number)
			if !ok {
				return false
			}
			_, ok = new(big.Int).SetString(number.String(), 10)
			return ok
		default:
			return false
		}
	}
	switch declared := schema["type"].(type) {
	case string:
		return allows(declared)
	case []any:
		for _, member := range declared {
			if name, ok := member.(string); ok && allows(name) {
				return true
			}
		}
	}
	return false
}

func TestResolveRenderFieldTraversesDottedPathsToScalarLeaves(t *testing.T) {
	t.Parallel()

	element := map[string]any{
		"name":   "primary",
		"status": map[string]any{"phase": "ready", "detail": map[string]any{"code": "ok"}},
		"items":  []any{"a"},
	}
	for _, test := range []struct {
		field  string
		found  bool
		scalar bool
	}{
		{field: "name", found: true, scalar: true},
		{field: "status.phase", found: true, scalar: true},
		{field: "status.detail.code", found: true, scalar: true},
		{field: "status", found: true, scalar: false},
		{field: "items", found: true, scalar: false},
		{field: "status.missing", found: false},
		{field: "name.phase", found: false},
	} {
		value, ok := resolveRenderField(element, test.field)
		if ok != test.found {
			t.Errorf("resolveRenderField(%q) found = %t, want %t", test.field, ok, test.found)
			continue
		}
		if ok && isRenderScalar(value) != test.scalar {
			t.Errorf("resolveRenderField(%q) scalar = %t, want %t", test.field, isRenderScalar(value), test.scalar)
		}
	}
}

// renderFixture executes one command against a fully populated generated-client
// DTO, so the coherence assertion runs over the bytes the adapter really emits
// rather than over a hand-written sample of them.
type renderFixture struct {
	commandID string
	arguments []string
	flags     []sdk.FlagOccurrence
	body      []byte
	chunks    []sdk.InputArtifactChunk
	// status is the success code the generated client accepts for this
	// operation. v2RevertTransaction is the only bound operation that
	// answers 201 rather than 200.
	status int32
}

func renderSampleTime() time.Time {
	return time.Date(2026, 9, 12, 8, 30, 0, 0, time.UTC)
}

func renderSampleLedger() components.V2Ledger {
	id := int64(7)
	deleted := renderSampleTime()
	return components.V2Ledger{
		Name: "primary", AddedAt: renderSampleTime(), Bucket: "default", DeletedAt: &deleted,
		Metadata: map[string]string{"region": "eu"}, Features: map[string]string{"MOVES_HISTORY": "ON"}, ID: &id,
	}
}

func renderSampleAccount() components.V2Account {
	moment := renderSampleTime()
	return components.V2Account{
		Address: "users:001", Metadata: map[string]string{"tier": "gold"},
		InsertionDate: &moment, UpdatedAt: &moment, FirstUsage: &moment,
		Volumes: map[string]components.V2Volume{"USD": {Input: big.NewInt(10), Output: big.NewInt(4)}},
	}
}

func renderSampleTransaction() components.V2Transaction {
	moment := renderSampleTime()
	reference := "ref-1"
	return components.V2Transaction{
		InsertedAt: &moment, UpdatedAt: &moment, Timestamp: moment,
		Postings:  []components.V2Posting{{Amount: big.NewInt(100), Asset: "USD", Destination: "users:001", Source: "world"}},
		Reference: &reference, Metadata: map[string]string{"tier": "gold"},
		ID: big.NewInt(42), Reverted: false,
	}
}

func renderSampleSchema() components.V2Schema {
	return components.V2Schema{
		Version: "v1", CreatedAt: renderSampleTime(), Chart: map[string]components.V2ChartSegment{},
	}
}

func renderSampleVolumes() components.V2VolumesWithBalance {
	return components.V2VolumesWithBalance{
		Account: "users:001", Asset: "USD", Input: big.NewInt(10), Output: big.NewInt(4), Balance: big.NewInt(6),
	}
}

func mustRenderJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode fixture: %v", err)
	}
	return encoded
}

func renderFixtures(t *testing.T) []renderFixture {
	t.Helper()
	ledger := sdk.FlagOccurrence{Name: "ledger", Value: "primary"}
	return []renderFixture{
		{commandID: "ledger.v2.list", body: mustRenderJSON(t, components.V2LedgerListResponse{
			Cursor: components.V2LedgerListResponseCursor{PageSize: 1, Data: []components.V2Ledger{renderSampleLedger()}},
		})},
		{commandID: "ledger.v2.stats", flags: []sdk.FlagOccurrence{ledger}, body: mustRenderJSON(t, components.V2StatsResponse{
			Data: components.V2Stats{Accounts: 3, Transactions: big.NewInt(9)},
		})},
		{commandID: "ledger.v2.accounts.list", flags: []sdk.FlagOccurrence{ledger}, body: mustRenderJSON(t, components.V2AccountsCursorResponse{
			Cursor: components.V2AccountsCursorResponseCursor{PageSize: 1, Data: []components.V2Account{renderSampleAccount()}},
		})},
		{commandID: "ledger.v2.accounts.show", arguments: []string{"users:001"}, flags: []sdk.FlagOccurrence{ledger},
			body: mustRenderJSON(t, components.V2AccountResponse{Data: renderSampleAccount()})},
		{commandID: "ledger.v2.transactions.list", flags: []sdk.FlagOccurrence{ledger}, body: mustRenderJSON(t, components.V2TransactionsCursorResponse{
			Cursor: components.V2TransactionsCursorResponseCursor{PageSize: 1, Data: []components.V2Transaction{renderSampleTransaction()}},
		})},
		{commandID: "ledger.v2.transactions.show", arguments: []string{"42"}, flags: []sdk.FlagOccurrence{ledger},
			body: mustRenderJSON(t, components.V2GetTransactionResponse{Data: renderSampleTransaction()})},
		{commandID: "ledger.v2.transactions.revert", arguments: []string{"42"}, flags: []sdk.FlagOccurrence{ledger}, status: 201,
			body: mustRenderJSON(t, components.V2RevertTransactionResponse{Data: renderSampleTransaction()})},
		{commandID: "ledger.v2.send", arguments: []string{"users:001", "100", "USD"}, flags: []sdk.FlagOccurrence{ledger},
			body: mustRenderJSON(t, components.V2CreateTransactionResponse{Data: renderSampleTransaction()})},
		{commandID: "ledger.v2.transactions.num", arguments: []string{"artifact"}, flags: []sdk.FlagOccurrence{ledger},
			chunks: []sdk.InputArtifactChunk{{Bytes: []byte("send [USD 100] (source = @world destination = @users:001)"), Final: true}},
			body:   mustRenderJSON(t, components.V2CreateTransactionResponse{Data: renderSampleTransaction()})},
		{commandID: "ledger.v2.volumes.list", flags: []sdk.FlagOccurrence{ledger}, body: mustRenderJSON(t, components.V2VolumesWithBalanceCursorResponse{
			Cursor: components.V2VolumesWithBalanceCursorResponseCursor{PageSize: 1, Data: []components.V2VolumesWithBalance{renderSampleVolumes()}},
		})},
		{commandID: "ledger.v2.schemas.list", flags: []sdk.FlagOccurrence{ledger}, body: mustRenderJSON(t, components.V2SchemasCursorResponse{
			Cursor: components.V2SchemasCursor{PageSize: 1, Data: []components.V2Schema{renderSampleSchema()}},
		})},
		{commandID: "ledger.v2.schemas.get", arguments: []string{"v1"}, flags: []sdk.FlagOccurrence{ledger},
			body: mustRenderJSON(t, components.V2SchemaResponse{Data: renderSampleSchema()})},
	}
}

func TestRenderHintFieldsResolveToScalarLeavesInEmittedResults(t *testing.T) {
	t.Parallel()

	fixtures := renderFixtures(t)
	if len(fixtures) != len(renderOracle) {
		t.Fatalf("render fixtures = %d, want one per hinted command (%d)", len(fixtures), len(renderOracle))
	}
	for _, fixture := range fixtures {
		t.Run(fixture.commandID, func(t *testing.T) {
			t.Parallel()

			command, ok := commandByID(fixture.commandID)
			if !ok {
				t.Fatalf("command %q missing", fixture.commandID)
			}
			if command.Render.Table == nil {
				t.Fatalf("%s declares no table render hint", fixture.commandID)
			}
			var publicSchema map[string]any
			if err := json.Unmarshal(command.PublicOutputSchema, &publicSchema); err != nil {
				t.Fatalf("decode public output schema: %v", err)
			}

			status := fixture.status
			if status == 0 {
				status = 200
			}
			respond := func(context.Context, sdk.Request) (sdk.Responses, error) {
				return sdk.NewResponseStream(sdk.Response{Status: status, ContentType: mediaTypeJSON, Body: fixture.body}), nil
			}
			var host interface {
				sdk.Host
				Events() []sdk.Event
			}
			if fixture.chunks != nil {
				host = &artifactHost{MemoryHost: sdk.NewMemoryHost(respond), chunks: fixture.chunks}
			} else {
				host = sdk.NewMemoryHost(respond)
			}

			if err := (Plugin{}).Execute(context.Background(), execution(fixture.commandID, fixture.arguments, fixture.flags...), host); err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			events := host.Events()
			if len(events) != 1 || events[0].Result == nil {
				t.Fatalf("events = %#v, want one result", events)
			}

			decoder := json.NewDecoder(bytes.NewReader(events[0].Result.Data))
			decoder.UseNumber()
			var decoded any
			if err := decoder.Decode(&decoded); err != nil {
				t.Fatalf("decode emitted result: %v", err)
			}
			element := decoded
			if collection, ok := decoded.([]any); ok {
				if len(collection) == 0 {
					t.Fatal("emitted collection is empty, so no column can be proven")
				}
				element = collection[0]
			}
			for _, column := range command.Render.Table.Columns {
				value, found := resolveRenderField(element, column.Field)
				if !found {
					t.Errorf("column %q (%q) does not resolve in the emitted result %s", column.Header, column.Field, events[0].Result.Data)
					continue
				}
				if !isRenderScalar(value) {
					t.Errorf("column %q (%q) resolves to the composite %#v", column.Header, column.Field, value)
				}
				leafSchema, found := resolveRenderSchemaField(publicSchema, column.Field)
				if !found {
					t.Errorf("column %q (%q) is absent from the public output schema", column.Header, column.Field)
					continue
				}
				if !renderSchemaAllowsScalar(leafSchema, value) {
					t.Errorf("column %q (%q) emits %#v, rejected by schema %#v", column.Header, column.Field, value, leafSchema)
				}
			}
		})
	}
}
