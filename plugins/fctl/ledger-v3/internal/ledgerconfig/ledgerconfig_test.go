package ledgerconfig

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestFilterTextRoundTripsEverySupportedConditionFamily(t *testing.T) {
	t.Parallel()

	tests := []struct {
		target commonpb.QueryTarget
		input  string
	}{
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "metadata[category] == premium"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "metadata[name] == \"hello world\""},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "metadata[active] == true"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "metadata[active] == false"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "metadata[age] == 42"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "metadata[category] == $value"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "metadata[category] != premium"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "metadata[age] > 18"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "metadata[age] >= $minimum"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "metadata[age] < 65"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "metadata[age] <= $maximum"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "metadata[age] between 18 and 65"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "metadata[age] between $minimum and $maximum"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "metadata[category] in (premium, gold, $tier)"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "metadata[category] exists"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "address == \"users:alice\""},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "address ^= $prefix"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "source in (\"bank:main\", \"bank:secondary\")"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "destination ^= \"users:\""},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "has asset USD"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "has asset USD/2"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "ledger == main"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "timestamp == \"2023-11-14T22:13:20Z\""},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "timestamp >= \"2023-11-14T22:13:20Z\""},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "timestamp > 1700000000000000 and timestamp < 1700000001000000"},
		{commonpb.QueryTarget_QUERY_TARGET_LOGS, "date between \"2023-11-14T22:13:20Z\" and \"2023-11-15T22:13:20Z\""},
		{commonpb.QueryTarget_QUERY_TARGET_AUDIT, "outcome == failure"},
		{commonpb.QueryTarget_QUERY_TARGET_AUDIT, "caller_subject == \"svc:payments\""},
		{commonpb.QueryTarget_QUERY_TARGET_AUDIT, "order_type in (create_transaction, revert_transaction)"},
		{commonpb.QueryTarget_QUERY_TARGET_AUDIT, "seq between 1000 and 2000"},
		{commonpb.QueryTarget_QUERY_TARGET_AUDIT, "proposal_id == 42"},
		{commonpb.QueryTarget_QUERY_TARGET_AUDIT, "timestamp >= \"2023-11-14T22:13:20Z\""},
		{commonpb.QueryTarget_QUERY_TARGET_AUDIT, "not (outcome == failure or ledger == main)"},
		{commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, "(metadata[a] == x or metadata[b] == y) and metadata[c] == z"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.input, func(t *testing.T) {
			t.Parallel()
			parsed, err := Parse(test.input, test.target)
			if err != nil {
				t.Fatalf("Parse(%q): %v", test.input, err)
			}
			formatted := Format(parsed)
			reparsed, err := Parse(formatted, test.target)
			if err != nil {
				t.Fatalf("Parse(Format(%q) = %q): %v", test.input, formatted, err)
			}
			if !proto.Equal(parsed, reparsed) {
				t.Fatalf("filter round-trip changed semantics: %q -> %q", test.input, formatted)
			}
		})
	}
}

func TestFilterParserRejectsMalformedAndUnsafeInputs(t *testing.T) {
	t.Parallel()

	inputs := []string{"", "metadata[key]", "metadata[key] ==", "metadata[name] > alice",
		"metadata[age] between 100 and 10", "metadata[name] between alice and bob",
		"metadata[category] in ()", "address ^= users:", "ledger > main",
		"has asset usd", "has asset USD/0", "has asset USD/02", "has asset USD/999", "bogus == value"}
	for _, input := range inputs {
		if _, err := Parse(input, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS); err == nil {
			t.Fatalf("Parse(%q) unexpectedly succeeded", input)
		}
	}
	if _, err := Parse(strings.Repeat("(", MaxParseDepth+1)+"metadata[a] == b", commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS); !errors.Is(err, ErrFilterTooDeep) {
		t.Fatalf("deep filter error = %v, want ErrFilterTooDeep", err)
	}
}

func TestDecodeFilterAcceptsEachWireFormAndRejectsMalformedValues(t *testing.T) {
	t.Parallel()

	want, err := Parse("metadata[state] == active", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	if err != nil {
		t.Fatal(err)
	}
	structured, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range [][]byte{structured, []byte("\"metadata[state] == active\""), []byte("metadata[state] == active")} {
		got, err := DecodeFilter(raw, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
		if err != nil || !proto.Equal(got, want) {
			t.Fatalf("DecodeFilter(%s) = %v, %v", raw, got, err)
		}
	}
	for _, raw := range [][]byte{nil, []byte(" "), []byte("null"), []byte("\"\"")} {
		got, err := DecodeFilter(raw, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
		if err != nil || got != nil {
			t.Fatalf("DecodeFilter(%s) = %v, %v; want nil", raw, got, err)
		}
	}
	for _, raw := range [][]byte{[]byte("{}"), []byte("{\"bad\":true}"), []byte("\"unterminated"), []byte("metadata[key]")} {
		if _, err := DecodeFilter(raw, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS); err == nil {
			t.Fatalf("DecodeFilter(%s) unexpectedly succeeded", raw)
		}
	}
}

func TestConfigFromProtoPreservesEveryEditableSection(t *testing.T) {
	t.Parallel()

	ledger := &commonpb.LedgerInfo{
		DefaultEnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
		AccountTypes:           map[string]*commonpb.AccountType{"customer": {Pattern: "users:*", Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL}},
		MetadataSchema: &commonpb.MetadataSchema{
			AccountFields:     map[string]*commonpb.MetadataFieldSchema{"tier": {Type: commonpb.MetadataType_METADATA_TYPE_STRING}},
			TransactionFields: map[string]*commonpb.MetadataFieldSchema{"score": {Type: commonpb.MetadataType_METADATA_TYPE_INT64}},
			LedgerFields:      map[string]*commonpb.MetadataFieldSchema{"region": {Type: commonpb.MetadataType_METADATA_TYPE_STRING}},
		},
	}
	indexes := []*commonpb.Index{
		{Id: &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{Metadata: &commonpb.MetadataIndexID{Target: commonpb.TargetType_TARGET_TYPE_ACCOUNT, Key: "tier"}}}},
		{Id: &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE}}},
		{Id: &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP}}},
		{Id: &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS}}},
		{Id: &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS}}},
		{Id: &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS}}},
		{Id: &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT}}},
		{Id: &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT}}},
	}
	filter, err := Parse("metadata[tier] == gold", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	if err != nil {
		t.Fatal(err)
	}
	got := ConfigFromProto(ledger, indexes,
		[]*commonpb.PreparedQuery{{Name: "gold", Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, Filter: filter}},
		[]*commonpb.NumscriptInfo{{Name: "payout", Content: "send [USD 1]", Version: "1.2.3"}})
	if got.DefaultEnforcementMode != "chart_enforcement_audit" || got.AccountTypes["customer"].Persistence != "ephemeral" {
		t.Fatalf("ConfigFromProto lost ledger fields: %#v", got)
	}
	if !got.MetadataSchema["account"]["tier"].Indexed || !got.Indexes.Reference || !got.Indexes.Timestamp || !got.Indexes.Address || !got.Indexes.SourceAddress || !got.Indexes.DestinationAddress || !got.Indexes.InsertedAt || !got.Indexes.RevertedAt {
		t.Fatalf("ConfigFromProto lost indexes: %#v", got)
	}
	if got.PreparedQueries["gold"].Filter == "" || got.Numscripts["payout"].Version != "1.2.3" {
		t.Fatalf("ConfigFromProto lost libraries: %#v", got)
	}
}

func TestComputeDiffCoversEveryEditableMutationFamily(t *testing.T) {
	t.Parallel()

	current := &EditableConfig{
		DefaultEnforcementMode: "strict",
		AccountTypes:           map[string]EditableAccountType{"changed": {Pattern: "old:*"}, "removed": {Pattern: "gone:*", Persistence: "transient"}},
		MetadataSchema: map[string]map[string]EditableMetaField{
			"account":     {"changed": {Type: "string", Indexed: true}, "removed": {Type: "string"}},
			"transaction": {"unindexed": {Type: "integer", Indexed: true}},
		},
		Indexes:         EditableIndexes{Reference: true, Address: true, DestinationAddress: true, RevertedAt: true},
		PreparedQueries: map[string]EditablePreparedQuery{"changed-target": {Target: "accounts"}, "changed-filter": {Target: "logs"}, "removed": {Target: "accounts"}},
		Numscripts:      map[string]EditableNumscript{"existing": {Content: "old", Version: "1.0.0"}},
	}
	desired := &EditableConfig{
		DefaultEnforcementMode: "audit",
		AccountTypes:           map[string]EditableAccountType{"changed": {Pattern: "new:*", Persistence: "ephemeral"}, "added": {Pattern: "new:*", Persistence: "transient"}},
		MetadataSchema: map[string]map[string]EditableMetaField{
			"account":     {"changed": {Type: "int64"}, "added": {Type: "string", Indexed: true}},
			"transaction": {"unindexed": {Type: "int64"}},
			"ledger":      {"region": {Type: "string"}},
		},
		Indexes: EditableIndexes{Timestamp: true, SourceAddress: true, InsertedAt: true},
		PreparedQueries: map[string]EditablePreparedQuery{
			"changed-target": {Target: "transactions", Filter: "metadata[x] == y"},
			"changed-filter": {Target: "logs", Filter: "date >= \"2023-11-14T22:13:20Z\""},
			"added":          {Target: "accounts", Filter: "metadata[tier] == gold"},
		},
		Numscripts: map[string]EditableNumscript{"existing": {Content: "new", Version: "1.1.0"}, "added": {Content: "send [USD 1]", Version: "1.0.0"}},
	}
	actions, err := ComputeDiff("main", current, desired)
	if err != nil {
		t.Fatalf("ComputeDiff: %v", err)
	}
	sections, operations := map[string]bool{}, map[string]bool{}
	for _, action := range actions {
		sections[action.Section], operations[action.Operation] = true, true
		if action.Request == nil {
			t.Fatalf("action has no request: %#v", action)
		}
	}
	for _, section := range []string{"defaultEnforcementMode", "accountType", "metadataSchema", "index", "preparedQuery", "numscript"} {
		if !sections[section] {
			t.Fatalf("ComputeDiff omitted section %q", section)
		}
	}
	for _, operation := range []string{"add", "update", "remove"} {
		if !operations[operation] {
			t.Fatalf("ComputeDiff omitted operation %q", operation)
		}
	}
}

func TestComputeDiffFailsClosedOnInvalidOrDestructiveChanges(t *testing.T) {
	t.Parallel()

	empty := func() *EditableConfig {
		return &EditableConfig{AccountTypes: map[string]EditableAccountType{}, MetadataSchema: map[string]map[string]EditableMetaField{}, PreparedQueries: map[string]EditablePreparedQuery{}, Numscripts: map[string]EditableNumscript{}}
	}
	tests := []struct {
		name    string
		current *EditableConfig
		desired *EditableConfig
	}{
		{"invalid added persistence", empty(), func() *EditableConfig {
			v := empty()
			v.AccountTypes["x"] = EditableAccountType{Persistence: "durable"}
			return v
		}()},
		{"invalid current persistence", func() *EditableConfig {
			v := empty()
			v.AccountTypes["x"] = EditableAccountType{Persistence: "durable"}
			return v
		}(), func() *EditableConfig { v := empty(); v.AccountTypes["x"] = EditableAccountType{}; return v }()},
		{"invalid desired persistence", func() *EditableConfig { v := empty(); v.AccountTypes["x"] = EditableAccountType{}; return v }(), func() *EditableConfig {
			v := empty()
			v.AccountTypes["x"] = EditableAccountType{Persistence: "durable"}
			return v
		}()},
		{"invalid metadata type", empty(), func() *EditableConfig {
			v := empty()
			v.MetadataSchema["account"] = map[string]EditableMetaField{"x": {Type: "bogus"}}
			return v
		}()},
		{"invalid query filter", empty(), func() *EditableConfig {
			v := empty()
			v.PreparedQueries["x"] = EditablePreparedQuery{Target: "accounts", Filter: "metadata[x]"}
			return v
		}()},
		{"content without version bump", func() *EditableConfig {
			v := empty()
			v.Numscripts["x"] = EditableNumscript{Content: "old", Version: "1.0.0"}
			return v
		}(), func() *EditableConfig {
			v := empty()
			v.Numscripts["x"] = EditableNumscript{Content: "new", Version: "1.0.0"}
			return v
		}()},
		{"invalid desired semver", empty(), func() *EditableConfig { v := empty(); v.Numscripts["x"] = EditableNumscript{Version: "1.0"}; return v }()},
		{"invalid stored semver", func() *EditableConfig { v := empty(); v.Numscripts["x"] = EditableNumscript{Version: "1.0"}; return v }(), func() *EditableConfig {
			v := empty()
			v.Numscripts["x"] = EditableNumscript{Version: "2.0.0"}
			return v
		}()},
		{"non-increasing version", func() *EditableConfig {
			v := empty()
			v.Numscripts["x"] = EditableNumscript{Version: "2.0.0"}
			return v
		}(), func() *EditableConfig {
			v := empty()
			v.Numscripts["x"] = EditableNumscript{Version: "1.0.0"}
			return v
		}()},
		{"removal", func() *EditableConfig {
			v := empty()
			v.Numscripts["x"] = EditableNumscript{Version: "1.0.0"}
			return v
		}(), empty()},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := ComputeDiff("main", test.current, test.desired); err == nil {
				t.Fatal("ComputeDiff unexpectedly succeeded")
			}
		})
	}
}
