//go:build it

package bucket_test

import (
	"encoding/json"
	"io/fs"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/connect"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"

	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/system"
)

func TestMigrationLogHashLookup(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	options := srv.NewDatabase(t).ConnectionOptions()
	db, err := connect.OpenSQLDB(ctx, options)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, system.Migrate(ctx, db))

	names, err := bucket.WalkMigrations(bucket.MigrationsFS, func(entry fs.DirEntry) (*string, error) {
		return pointer.For(entry.Name()), nil
	})
	require.NoError(t, err)
	migrationIndex := slices.Index(names, "54-fix-log-hash-lookup")
	require.NotEqual(t, -1, migrationIndex)

	const schema = "hashbucket"
	migrator := bucket.GetMigrator(db, schema)
	for range migrationIndex {
		require.NoError(t, migrator.UpByOne(ctx))
	}

	// The target is common, but another ledger owns the highest IDs. An ID-only
	// backward scan must discard thousands of unrelated logs before finding it.
	_, err = db.ExecContext(ctx, `
		INSERT INTO ?.logs (ledger, id, type, hash, date, data, memento)
		SELECT 'target', n, 'NEW_TRANSACTION', public.digest(n::text, 'sha256'),
		       '2026-01-01'::timestamp, '{}'::jsonb, convert_to('{}', 'UTF8')
		FROM generate_series(1, 32000) n;
		INSERT INTO ?.logs (ledger, id, type, hash, date, data, memento)
		SELECT 'other', n, 'NEW_TRANSACTION', public.digest(n::text, 'sha256'),
		       '2026-01-01'::timestamp, '{}'::jsonb, convert_to('{}', 'UTF8')
		FROM generate_series(1, 70000) n;
		ANALYZE ?.logs;
		CREATE TRIGGER test_log_hash BEFORE INSERT ON ?.logs
		FOR EACH ROW EXECUTE FUNCTION ?.set_log_hash();
	`, bun.Ident(schema), bun.Ident(schema), bun.Ident(schema), bun.Ident(schema), bun.Ident(schema))
	require.NoError(t, err)

	type logInput struct {
		ledger string
		id     string
	}
	inputs := []logInput{
		{"target", "32001"},
		{"other", "70001"},
		{"empty", "1"},
		{"target", "32010"}, // A rolled-back sequence allocation can leave gaps.
		{"empty", "9"},
		{"TARGET", "1"},
		{strings.Repeat("x", 63), "1"},
		{"target", "1000000000000000000000000000000"},
	}

	// Run the actual trigger, including multiple links in each hash chain. Each
	// run rolls back so both function definitions see identical logs and inputs.
	// It returns the hashes and, for each input, the plan of the trigger's lookup.
	writeLogs := func(t *testing.T, mode string) ([][]byte, []string) {
		t.Helper()
		// auto_explain observes nested statements in the real trigger, rather
		// than a separately maintained copy of its lookup query.
		var notices []string
		config, err := pgx.ParseConfig(options.DatabaseSourceName)
		require.NoError(t, err)
		config.OnNotice = func(_ *pgconn.PgConn, notice *pgconn.Notice) {
			notices = append(notices, notice.Message)
		}
		conn, err := pgx.ConnectConfig(ctx, config)
		require.NoError(t, err)
		defer func() { require.NoError(t, conn.Close(ctx)) }()
		_, err = conn.Exec(ctx, `
			LOAD 'auto_explain';
			SET auto_explain.log_min_duration = 0;
			SET auto_explain.log_nested_statements = on;
			SET auto_explain.log_analyze = on;
			SET auto_explain.log_format = json;
			SET auto_explain.log_level = notice;
		`)
		require.NoError(t, err)
		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		defer func() { require.NoError(t, tx.Rollback(ctx)) }()
		_, err = tx.Exec(ctx, "SELECT set_config('plan_cache_mode', $1, true)", mode)
		require.NoError(t, err)

		hashes := make([][]byte, 0, len(inputs))
		plans := make([]string, 0, len(inputs))
		for _, input := range inputs {
			notices = nil
			var hash []byte
			err := tx.QueryRow(ctx, `
				INSERT INTO `+schema+`.logs (ledger, id, type, date, data, memento)
				VALUES ($1, $2::numeric, 'NEW_TRANSACTION', '2026-01-02'::timestamp,
				        '{"test": true}'::jsonb, convert_to('{"test": true}', 'UTF8'))
				RETURNING hash
			`, input.ledger, input.id).Scan(&hash)
			require.NoError(t, err)
			require.Len(t, hash, 32)
			var lookupPlans []string
			for _, notice := range notices {
				start := strings.Index(notice, "{")
				if start < 0 {
					continue
				}
				var explained struct {
					Query string          `json:"Query Text"`
					Plan  json.RawMessage `json:"Plan"`
				}
				require.NoError(t, json.Unmarshal([]byte(notice[start:]), &explained))
				if strings.Contains(explained.Query, "from logs") {
					lookupPlans = append(lookupPlans, string(explained.Plan))
				}
			}
			require.Len(t, lookupPlans, 1, "must observe the trigger's lookup plan")
			plans = append(plans, lookupPlans[0])
			hashes = append(hashes, hash)
		}
		return hashes, plans
	}

	modes := []string{"force_custom_plan", "force_generic_plan"}
	want, _ := writeLogs(t, "force_custom_plan")
	// The dataset must reproduce the defect, otherwise the checks below pass
	// without guarding anything.
	for _, mode := range modes {
		_, plans := writeLogs(t, mode)
		require.Contains(t, plans[0], `"Filter":`, "%s: original lookup must scan other ledgers", mode)
	}

	require.NoError(t, migrator.UpByOne(ctx))
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			hashes, plans := writeLogs(t, mode)
			require.Equal(t, want, hashes)
			for i, plan := range plans {
				// Index conditions bound the lookup to this ledger. A filter
				// on an ID-only scan can walk all of another ledger's rows.
				require.Contains(t, plan, `"Index Cond":`, inputs[i].ledger)
				require.NotContains(t, plan, `"Filter":`, inputs[i].ledger)
			}
		})
	}

	// The replacement must keep the bucket scope and SECURITY DEFINER contract.
	var securityDefiner bool
	var scopedSearchPath bool
	require.NoError(t, db.NewRaw(`SELECT prosecdef FROM pg_proc WHERE oid = ?::regprocedure`, schema+".set_log_hash()").Scan(ctx, &securityDefiner))
	require.True(t, securityDefiner)
	require.NoError(t, db.NewRaw(`SELECT ? = ANY(proconfig) FROM pg_proc WHERE oid = ?::regprocedure`, "search_path="+schema, schema+".set_log_hash()").Scan(ctx, &scopedSearchPath))
	require.True(t, scopedSearchPath)
}
