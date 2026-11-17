package ledgers

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestConfigIDFilterExportApplyRoundTrip(t *testing.T) {
	t.Parallel()
	ledger := &commonpb.LedgerInfo{Name: "ledger1"}
	for _, target := range []commonpb.QueryTarget{
		commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		commonpb.QueryTarget_QUERY_TARGET_LOGS,
	} {
		filter, err := filterexpr.Parse("id > $min and id < $max", target)
		require.NoError(t, err)
		query := &commonpb.PreparedQuery{Name: "by-id", Target: target, Filter: filter}
		exported := ConfigFromProto(ledger, nil, []*commonpb.PreparedQuery{query}, nil)
		var buf bytes.Buffer
		require.NoError(t, exported.WriteJSON(&buf))
		path := filepath.Join(t.TempDir(), "config.json")
		require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o600))
		parsed, err := ReadConfigFile(path)
		require.NoError(t, err)
		current := ConfigFromProto(ledger, nil, nil, nil)
		actions, err := ComputeDiff(ledger.GetName(), current, parsed)
		require.NoError(t, err)
		require.Len(t, actions, 1)
		created := actions[0].Request.GetCreatePreparedQuery().GetQuery()
		require.Equal(t, target, created.GetTarget())
		require.True(t, proto.Equal(filter, created.GetFilter()))
	}
}

// TestConfigStructuredRoundTrip is the regression test for #493: exporting a
// configuration as YAML/JSON (the shape produced by `ledgers configuration
// --yaml/--json` and by `ledgers configuration export`) must be re-readable
// by `ledgers configuration apply`. ComputeDiff against the original config
// then yields zero actions — anything else means the round-trip lost data.
func TestConfigStructuredRoundTrip(t *testing.T) {
	t.Parallel()

	ledger := &commonpb.LedgerInfo{
		Name:                   "bitcoin-mainnet",
		DefaultEnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT,
		AccountTypes: map[string]*commonpb.AccountType{
			"blockchain": {Name: "blockchain", Pattern: "btc:{address}"},
			"burn":       {Name: "burn", Pattern: "burn"},
			// Non-default persistence values must round-trip — otherwise
			// `apply` silently re-creates ephemeral/transient types as
			// `normal`, flipping volume storage behavior (see #493).
			"fees":    {Name: "fees", Pattern: "fees:{height}:{txhash}", Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT},
			"mempool": {Name: "mempool", Pattern: "mempool:{txhash}", Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL},
		},
		MetadataSchema: &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"miner": {Type: commonpb.MetadataType_METADATA_TYPE_BOOL},
			},
			TransactionFields: map[string]*commonpb.MetadataFieldSchema{
				"block_height": {Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
				"coinbase":     {Type: commonpb.MetadataType_METADATA_TYPE_BOOL},
			},
		},
	}

	// Indexes are no longer nested on LedgerInfo (#450). The CLI consumes them
	// via BucketService.ListIndexes; the test wires them in as a separate slice.
	ledgerIndexes := []*commonpb.Index{
		{Id: &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE}}},
		{Id: &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP}}},
		{Id: &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{Metadata: &commonpb.MetadataIndexID{
			Target: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
			Key:    "block_height",
		}}}},
	}

	queries := []*commonpb.PreparedQuery{
		{Name: "btc-txs", Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS},
		// Logs target must round-trip — without the dedicated case in
		// `queryTargetString`, export emitted `target: unknown` and apply
		// silently fell back to ACCOUNTS, swapping the query target.
		{Name: "audit-trail", Target: commonpb.QueryTarget_QUERY_TARGET_LOGS},
	}
	numscripts := []*commonpb.NumscriptInfo{
		{Name: "transfer", Content: "send 1 BTC from @user:alice to @user:bob", Version: "1"},
		{Name: "burn", Content: "send 1 BTC from @user:alice to @burn", Version: "1"},
	}

	original := ConfigFromProto(ledger, ledgerIndexes, queries, numscripts)

	t.Run("yaml", func(t *testing.T) {
		t.Parallel()
		assertRoundTrip(t, ledger.GetName(), original, "config.yaml", (*EditableConfig).WriteYAML)
	})

	t.Run("json", func(t *testing.T) {
		t.Parallel()
		assertRoundTrip(t, ledger.GetName(), original, "config.json", (*EditableConfig).WriteJSON)
	})
}

// TestConfigPreparedQueryFilterExportReapplies is the regression for the
// filter-coverage gap between the two filter forms: a prepared query created
// through the JSON DSL on a field the textual grammar did not know (reference,
// reverted, id, logId, …) exported as `<unknown filter>`, which `apply` could
// not parse back. Every exported filter must now re-create the same proto.
func TestConfigPreparedQueryFilterExportReapplies(t *testing.T) {
	t.Parallel()

	jsonFilters := map[string]struct {
		target commonpb.QueryTarget
		json   string
	}{
		"by-ref":      {commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, `{"$match":{"reference":{"$param":"ref"}}}`},
		"reverted":    {commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, `{"$match":{"reverted":true}}`},
		"id-window":   {commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, `{"$and":[{"$gt":{"id":10}},{"$lt":{"id":20}}]}`},
		"inserted":    {commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, `{"$gte":{"insertedAt":"2023-11-14T22:13:20Z"}}`},
		"recent-logs": {commonpb.QueryTarget_QUERY_TARGET_LOGS, `{"$match":{"logId":7}}`},
		// Parameterized range bounds must survive export: a param-only bound
		// used to export as "id <uint?>", and the param side of a mixed range
		// was dropped, silently widening the re-applied query.
		"id-param":       {commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, `{"$match":{"id":{"$param":"n"}}}`},
		"id-mixed":       {commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, `{"$and":[{"$gte":{"id":3}},{"$lt":{"id":{"$param":"hi"}}}]}`},
		"since-param":    {commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, `{"$gte":{"timestamp":{"$param":"since"}}}`},
		"logs-from-date": {commonpb.QueryTarget_QUERY_TARGET_LOGS, `{"$gte":{"date":{"$param":"from"}}}`},
	}

	var queries []*commonpb.PreparedQuery
	want := map[string]*commonpb.QueryFilter{}
	for name, tc := range jsonFilters {
		filter, err := commonpb.DecodeQueryFilterJSON([]byte(tc.json), tc.target)
		require.NoError(t, err, name)
		queries = append(queries, &commonpb.PreparedQuery{Name: name, Target: tc.target, Filter: filter})
		want[name] = filter
	}

	exported := ConfigFromProto(&commonpb.LedgerInfo{Name: "l"}, nil, queries, nil)

	actions, err := diffPreparedQueries("l", &EditableConfig{}, exported)
	require.NoError(t, err)
	require.Len(t, actions, len(jsonFilters))

	for _, action := range actions {
		created := action.Request.GetCreatePreparedQuery().GetQuery()
		require.NotNil(t, created)
		require.Truef(t, proto.Equal(want[created.GetName()], created.GetFilter()),
			"%s: exported filter %q re-applied as %v", created.GetName(),
			exported.PreparedQueries[created.GetName()].Filter, created.GetFilter())
	}
}

// TestDiffAccountTypePersistenceChange asserts ComputeDiff catches a
// persistence flip — without this, editing `persistence: ephemeral → transient`
// in a manifest would silently be a no-op at apply time (the previous diff
// only inspected `pattern`).
func TestDiffAccountTypePersistenceChange(t *testing.T) {
	t.Parallel()

	current := &EditableConfig{
		AccountTypes: map[string]EditableAccountType{
			"fees": {Pattern: "fees:{height}:{txhash}", Persistence: "ephemeral"},
		},
	}
	desired := &EditableConfig{
		AccountTypes: map[string]EditableAccountType{
			"fees": {Pattern: "fees:{height}:{txhash}", Persistence: "transient"},
		},
	}

	actions, err := ComputeDiff("any-ledger", current, desired)
	require.NoError(t, err)
	require.Len(t, actions, 2)
	require.Equal(t, "accountType", actions[0].Section)
	require.Equal(t, "remove", actions[0].Operation)
	require.Equal(t, "accountType", actions[1].Section)
	require.Equal(t, "add", actions[1].Operation)
}

// TestDiffAccountTypePersistenceSpellingEquivalence asserts ComputeDiff
// normalizes through ParsePersistence so equivalent spellings ("", "normal",
// "NORMAL", "Normal" — same for ephemeral/transient) don't trigger spurious
// remove+add cycles when a user hand-edits the exported manifest.
func TestDiffAccountTypePersistenceSpellingEquivalence(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		current string
		desired string
	}{
		{"normal default vs explicit", "", "normal"},
		{"normal uppercase", "", "NORMAL"},
		{"ephemeral case-insensitive", "ephemeral", "EPHEMERAL"},
		{"transient title-case", "transient", "Transient"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			current := &EditableConfig{
				AccountTypes: map[string]EditableAccountType{
					"x": {Pattern: "x:*", Persistence: tc.current},
				},
			}
			desired := &EditableConfig{
				AccountTypes: map[string]EditableAccountType{
					"x": {Pattern: "x:*", Persistence: tc.desired},
				},
			}

			actions, err := ComputeDiff("any-ledger", current, desired)
			require.NoError(t, err)
			require.Empty(t, actions, "equivalent spellings must not plan changes")
		})
	}
}

// TestDiffPreparedQueryTargetChange asserts that a target change is atomic.
func TestDiffPreparedQueryTargetChange(t *testing.T) {
	t.Parallel()

	current := &EditableConfig{
		PreparedQueries: map[string]EditablePreparedQuery{
			"audit": {Target: "transactions"},
		},
	}
	desired := &EditableConfig{
		PreparedQueries: map[string]EditablePreparedQuery{
			"audit": {Target: "logs"},
		},
	}

	actions, err := ComputeDiff("any-ledger", current, desired)
	require.NoError(t, err)
	require.Len(t, actions, 1)
	require.Equal(t, "preparedQuery", actions[0].Section)
	require.Equal(t, "update", actions[0].Operation)
	require.Equal(t, commonpb.QueryTarget_QUERY_TARGET_LOGS, actions[0].Request.GetUpdatePreparedQuery().GetTarget())
	require.NotNil(t, actions[0].Request.GetUpdatePreparedQuery().Target)
}

// TestDiffPreparedQueryFilterChange asserts that when only the filter (not the
// target) moves, ComputeDiff emits an in-place update with an explicit target.
func TestDiffPreparedQueryFilterChange(t *testing.T) {
	t.Parallel()

	current := &EditableConfig{
		PreparedQueries: map[string]EditablePreparedQuery{
			"audit": {Target: "transactions", Filter: `address == "users:alice"`},
		},
	}
	desired := &EditableConfig{
		PreparedQueries: map[string]EditablePreparedQuery{
			"audit": {Target: "transactions", Filter: `address == "users:bob"`},
		},
	}

	actions, err := ComputeDiff("any-ledger", current, desired)
	require.NoError(t, err)
	require.Len(t, actions, 1)
	require.Equal(t, "preparedQuery", actions[0].Section)
	require.Equal(t, "update", actions[0].Operation)
	require.NotNil(t, actions[0].Request.GetUpdatePreparedQuery().Target)
}

func assertRoundTrip(
	t *testing.T,
	ledgerName string,
	original *EditableConfig,
	filename string,
	write func(*EditableConfig, io.Writer) error,
) {
	t.Helper()

	var buf bytes.Buffer
	require.NoError(t, write(original, &buf))

	dir := t.TempDir()
	path := filepath.Join(dir, filename)
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o600))

	parsed, err := ReadConfigFile(path)
	require.NoError(t, err, "round-trip should re-parse cleanly")

	actions, err := ComputeDiff(ledgerName, original, parsed)
	require.NoError(t, err)
	require.Empty(t, actions, "round-trip should yield zero diff actions; payload:\n%s", buf.String())
}
