package v2

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v5/pkg/authn/jwt"

	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func TestInsertSchemaUnknownChartPropertyCompatibility(t *testing.T) {
	t.Parallel()

	const payload = `{"chart":{"bank":{".patern":{}}}}`
	const idempotencyKey = "unknown-chart-property-compatibility"

	// Exercise the HTTP boundary with a successful insertion followed by an
	// idempotency hit. Both requests must reach the controller with the same
	// normalized schema; rejecting either body would bypass idempotency lookup.
	systemController, ledgerController := newTestingSystemController(t, false)
	ledgerController.EXPECT().IsDatabaseUpToDate(gomock.Any()).Return(true, nil).AnyTimes()
	parameters := ledgercontroller.Parameters[ledgercontroller.InsertSchema]{
		IdempotencyKey: idempotencyKey,
		Input: ledgercontroller.InsertSchema{
			Version: "v1.0.0",
			Data: ledger.SchemaData{
				Chart: ledger.ChartOfAccounts{
					"bank": {Account: &ledger.ChartAccount{}},
				},
			},
		},
	}
	gomock.InOrder(
		ledgerController.EXPECT().InsertSchema(gomock.Any(), gomock.Eq(parameters)).Return(nil, nil, false, nil),
		ledgerController.EXPECT().InsertSchema(gomock.Any(), gomock.Eq(parameters)).Return(nil, nil, true, nil),
	)
	router := NewRouter(systemController, jwt.NewNoAuth(), "develop")

	for _, expectedHit := range []string{"", "true"} {
		// Keep the original wire JSON: marshalling ChartOfAccounts would already
		// discard the unknown property and fail to exercise this regression.
		req := httptest.NewRequest(http.MethodPost, "/default/schemas/v1.0.0", strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
		require.Equal(t, expectedHit, rec.Header().Get("Idempotency-Hit"))
		require.Empty(t, rec.Body.String())
	}
}

func TestLogsImportUnknownChartPropertyCompatibility(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		charts []string
	}{
		{
			name:   "single schema log",
			charts: []string{`{"bank":{".patern":{}}}`},
		},
		{
			name: "unknown property between valid logs",
			charts: []string{
				`{"bank":{}}`,
				`{"bank":{".patern":{}}}`,
				`{"bank":{}}`,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var body strings.Builder
			var expected []ledger.Log
			for i, chart := range tc.charts {
				version := fmt.Sprintf("v%d.0.0", i+1)
				// Write raw chart JSON so the misspelled property survives until
				// importLogs calls Log.UnmarshalJSON and HydrateLog.
				fmt.Fprintf(&body, "{\"id\":%d,\"type\":\"INSERTED_SCHEMA\",\"data\":{\"schema\":{\"version\":%q,\"chart\":%s}}}\n", i+1, version, chart)
				expected = append(expected, ledger.NewLog(ledger.InsertedSchema{
					Schema: ledger.Schema{
						Version: version,
						SchemaData: ledger.SchemaData{
							Chart: ledger.ChartOfAccounts{
								"bank": {Account: &ledger.ChartAccount{}},
							},
						},
					},
				}).WithID(uint64(i+1)))
			}

			systemController, ledgerController := newTestingSystemController(t, true)
			received := make(chan []ledger.Log, 1)
			ledgerController.EXPECT().Import(gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, stream chan ledger.Log) error {
					var logs []ledger.Log
					defer func() { received <- logs }()
					for {
						select {
						case log, ok := <-stream:
							if !ok {
								return nil
							}
							logs = append(logs, log)
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				})

			router := NewRouter(systemController, jwt.NewNoAuth(), "develop")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req := httptest.NewRequest(http.MethodPost, "/default/logs/import", strings.NewReader(body.String())).WithContext(ctx)
			req.Header.Set("Content-Type", "application/x-ndjson")
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)
			// A decoder regression can return before closing the stream. Cancel
			// the mock consumer in that case, rather than leaking a goroutine.
			cancel()

			select {
			case logs := <-received:
				require.Equal(t, expected, logs, "all logs must reach the consumer in order")
			case <-time.After(5 * time.Second):
				t.Fatal("import consumer did not exit")
			}
			require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
			require.Empty(t, rec.Body.String())
		})
	}
}
