//go:build clickhouse

package events_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/application/events"
)

func TestClickHouseSinkIntegration_AccountMetadataMigration(t *testing.T) {
	t.Parallel()

	for _, fixture := range []struct {
		name     string
		metadata string
		failure  string
	}{
		{name: "stringified object", metadata: `{"name":"Alice","empty":""}`},
		{name: "invalid JSON", metadata: `not-json`, failure: "invalid JSON"},
		{name: "array", metadata: `[]`, failure: "not an object"},
		{name: "numeric value", metadata: `{"name":123}`, failure: "non-string values"},
		{name: "duplicate key", metadata: `{"name":"Alice","name":"Bob"}`, failure: "duplicate keys"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			conn := openTestClickHouseConn(t, sharedClickHouseDSN)
			source, target := uniqueTopic("events_legacy"), uniqueTopic("events_nested")
			//nolint:dupword // Preserve the exact nested type when constructing the legacy schema fixture.
			legacyDDL := strings.Replace(events.ClickHouseCreateTableDDL(source),
				"accountMetadata Map(String, Map(String, String))", "accountMetadata Map(String, String)", 1)
			require.Contains(t, legacyDDL, "accountMetadata Map(String, String)")
			require.NoError(t, conn.Exec(ctx, legacyDDL))
			require.NoError(t, conn.Exec(ctx, events.ClickHouseCreateTableDDL(target)))
			t.Cleanup(func() {
				require.NoError(t, conn.Exec(ctx, "DROP TABLE "+source))
				require.NoError(t, conn.Exec(ctx, "DROP TABLE "+target))
			})

			var version string
			require.NoError(t, conn.QueryRow(ctx, "SELECT version()").Scan(&version))
			t.Logf("ClickHouse migration server: %s", version)

			// Use the old column type deliberately: nested objects that an old
			// insertion accepted/coerced appear here as JSON strings. This does
			// not assume all old ClickHouse versions accepted nested objects.
			batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+source)
			require.NoError(t, err)
			date := time.Date(2026, 1, 1, 0, 0, 0, 123456000, time.UTC)
			require.NoError(t, batch.Append(uint64(1), "committed_transaction", "orders", date,
				`{"transaction":{"id":1,"postings":[{"source":"world","destination":"users:001","amount":"18446744073709551617","asset":"USD"}]}}`))
			metadataJSON := `{"accountMetadata":{"users:001":` + quoteMigrationJSONString(t, fixture.metadata) + `}}`
			require.NoError(t, batch.Append(uint64(2), "committed_transaction", "orders", date, metadataJSON))
			require.NoError(t, batch.Append(uint64(3), "reverted_transaction", "orders", date,
				`{"accountMetadata":{"users:001":`+quoteMigrationJSONString(t, fixture.metadata)+`},`+
					`"revertTransaction":{"id":2,"postings":[{"source":"users:001","destination":"world",`+
					`"amount":"115792089237316195423570985008687907853269984665640564039457584007913129639935","asset":"USD"}]},`+
					`"unknown":{"integer":18446744073709551615,"text":"accountMetadata"}}`))
			require.NoError(t, batch.Send())
			before := queryClickHouseEvents(t, sharedClickHouseDSN, source)
			require.Len(t, before, 3)

			statements := accountMetadataMigrationStatements(t, source, target)
			var migrationErr error
			for _, statement := range statements[:5] {
				if strings.HasPrefix(statement, "SELECT") {
					var result uint8
					migrationErr = conn.QueryRow(ctx, statement).Scan(&result)
				} else {
					migrationErr = conn.Exec(ctx, statement)
				}
				if migrationErr != nil {
					break
				}
			}
			require.Equal(t, before, queryClickHouseEvents(t, sharedClickHouseDSN, source), "source must remain intact")
			if fixture.failure != "" {
				require.ErrorContains(t, migrationErr, fixture.failure)
				require.Empty(t, queryClickHouseEvents(t, sharedClickHouseDSN, target))

				return
			}
			require.NoError(t, migrationErr)
			// Execute the documented export and converter, then import through
			// the same typed destination schema. The source stores an exact
			// UInt256; quoting its fixture input only lets 24.x ingest it.
			exported := clickHouseMigrationHTTP(t, ctx, statements[5], nil)
			command := exec.CommandContext(ctx, "go", "run", "../../../scripts/clickhouse-account-metadata")
			command.Stdin = bytes.NewReader(exported)
			var converted, stderr bytes.Buffer
			command.Stdout, command.Stderr = &converted, &stderr
			require.NoError(t, command.Run(), stderr.String())
			clickHouseMigrationHTTP(t, ctx, "INSERT INTO "+target+" FORMAT JSONEachRow", &converted)
			after := queryClickHouseEvents(t, sharedClickHouseDSN, target)
			require.Len(t, after, 3)
			for i := range before {
				require.Equal(t, before[i].LogSequence, after[i].LogSequence)
				require.Equal(t, before[i].Type, after[i].Type)
				require.Equal(t, before[i].Ledger, after[i].Ledger)
				require.Equal(t, before[i].Date, after[i].Date)
			}
			var name, amount string
			require.NoError(t, conn.QueryRow(ctx, "SELECT data.accountMetadata['users:001']['name'] FROM "+target+" WHERE log_sequence = 2").Scan(&name))
			require.Equal(t, "Alice", name)
			require.NoError(t, conn.QueryRow(ctx, "SELECT toString(data.transaction.postings.amount[1]) FROM "+target+" WHERE log_sequence = 1").Scan(&amount))
			require.Equal(t, "18446744073709551617", amount)
			require.NoError(t, conn.QueryRow(ctx, "SELECT toString(data.revertTransaction.postings.amount[1]) FROM "+target+" WHERE log_sequence = 3").Scan(&amount))
			require.Equal(t, "115792089237316195423570985008687907853269984665640564039457584007913129639935", amount)
			require.JSONEq(t, before[0].Data, after[0].Data, "row without account metadata preserves every field")
			for i := range before {
				var original, migrated map[string]json.RawMessage
				require.NoError(t, json.Unmarshal([]byte(before[i].Data), &original))
				require.NoError(t, json.Unmarshal([]byte(after[i].Data), &migrated))
				delete(original, "accountMetadata")
				delete(migrated, "accountMetadata")
				require.Equal(t, original, migrated, "all other JSON values retain their raw representation")
			}
			// Reusing a populated destination must stop before another copy.
			var preflight uint8
			require.ErrorContains(t, conn.QueryRow(ctx, statements[4]).Scan(&preflight), "destination must be empty")
		})
	}
}

func clickHouseMigrationHTTP(t *testing.T, ctx context.Context, query string, input io.Reader) []byte {
	t.Helper()

	endpoint, err := url.Parse(sharedClickHouseHTTPURL)
	require.NoError(t, err)
	values := endpoint.Query()
	values.Set("query", query)
	values.Set("allow_experimental_json_type", "1")
	values.Set("allow_experimental_variant_type", "1")
	values.Set("date_time_input_format", "best_effort")
	endpoint.RawQuery = values.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), input)
	require.NoError(t, err)
	client := &http.Client{Timeout: time.Minute}
	response, err := client.Do(request)
	require.NoError(t, err)
	defer func() { require.NoError(t, response.Body.Close()) }()
	data, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode, string(data))

	return data
}

func accountMetadataMigrationStatements(t *testing.T, source, target string) []string {
	t.Helper()

	content, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "ops", "clickhouse-account-metadata.sql"))
	require.NoError(t, err)
	var statements []string
	var lines []string
	for line := range strings.SplitSeq(string(content), "\n") {
		if !strings.HasPrefix(line, "--") {
			lines = append(lines, line)
		}
	}
	for statement := range strings.SplitSeq(strings.Join(lines, "\n"), ";") {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		statement = strings.ReplaceAll(statement, "ledger_events_nested", target)
		statement = strings.ReplaceAll(statement, "ledger_events", source)
		statements = append(statements, statement)
	}
	require.Len(t, statements, 6)

	return statements
}

func quoteMigrationJSONString(t *testing.T, value string) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	require.NoError(t, err)

	return string(encoded)
}
