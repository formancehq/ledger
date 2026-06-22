package ledgers

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

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
		Indexes: []*commonpb.Index{
			{Id: &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE}}},
			{Id: &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP}}},
			{Id: &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{Metadata: &commonpb.MetadataIndexID{
				Target: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
				Key:    "block_height",
			}}}},
		},
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

	original := ConfigFromProto(ledger, queries, numscripts)

	t.Run("yaml", func(t *testing.T) {
		t.Parallel()
		assertRoundTrip(t, ledger.GetName(), original, "config.yaml", (*EditableConfig).WriteYAML)
	})

	t.Run("json", func(t *testing.T) {
		t.Parallel()
		assertRoundTrip(t, ledger.GetName(), original, "config.json", (*EditableConfig).WriteJSON)
	})
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

// TestDiffPreparedQueryTargetChange asserts ComputeDiff emits a delete + create
// pair when only the target of an existing prepared query is edited.
// UpdatePreparedQueryRequest carries no Target field — an in-place update
// would silently drop the new target and leave the original in place
// (#502 review, Paul Nicolas).
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
	require.Len(t, actions, 2, "target change must produce delete + create, not an in-place update")
	require.Equal(t, "preparedQuery", actions[0].Section)
	require.Equal(t, "remove", actions[0].Operation)
	require.Equal(t, "preparedQuery", actions[1].Section)
	require.Equal(t, "add", actions[1].Operation)
}

// TestDiffPreparedQueryFilterChange asserts that when only the filter (not the
// target) moves, ComputeDiff still emits the cheap in-place update — the
// remove+create path is only justified when the server has no equivalent
// updater.
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
