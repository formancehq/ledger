package query_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// TestExecute_InvalidCursorIsValidationError pins the second of the two decode
// sites EN-1791 converted from a bare fmt.Errorf to a validation sentinel.
//
// A cursor is caller input. Wrapped in a bare error it reached the HTTP layer's
// 500 sanitizer (internal/adapter/http/error_handler.go) and codes.Internal
// over gRPC, so a client that sent a truncated or hand-edited cursor was told
// the server had failed. ctrl.InspectIndex has the same fix and is pinned by
// tests/e2e/business/inspect_index_test.go; executeList had no coverage at any
// level, so reverting those two lines was a silent regression.
//
// Driven through the exported query.Execute rather than executeList directly:
// the reachable surfaces are HTTP
// POST /v3/{ledgerName}/prepared-queries/{queryName}/execute and gRPC
// ExecutePreparedQuery, and both need an existing prepared query in
// QUERY_MODE_LIST — which is exactly what makes this site hard to reach and is
// why it was missed. The sentinel -> 400 / "VALIDATION" hop is already covered
// by internal/adapter/http/error_handler_test.go.
func TestExecute_InvalidCursorIsValidationError(t *testing.T) {
	t.Parallel()

	const (
		ledger    = "cursor-ledger"
		queryName = "all-accounts"
	)

	for _, tc := range []struct {
		name   string
		cursor string
	}{
		// Fails at base64.RawURLEncoding.DecodeString.
		{name: "not base64", cursor: "not-a-valid-cursor!!"},
		// Valid base64, but the payload is not the cursorData JSON object:
		// base64url of "abc". Fails at json.Unmarshal instead, so both
		// branches of decodeCursor are covered.
		{name: "base64 but not json", cursor: "YWJj"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			logger := logging.FromContext(logging.TestingContext())

			s := newTestStore(t)
			attrs := attributes.New()

			rs, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
			require.NoError(t, err)
			t.Cleanup(func() { _ = rs.Close() })

			registerLedger(t, s, ledger)

			// Seeded through the attribute rather than state.SavePreparedQuery:
			// Execute reads via query.ReadPreparedQuery -> attr.Get, which keys
			// off the attributes zone, while SavePreparedQuery writes the
			// per-ledger zone. Only the former is on the read path.
			batch := s.OpenWriteSession()
			_, err = attrs.PreparedQuery.Set(batch,
				domain.PreparedQueryKey{LedgerName: ledger, Name: queryName}.Bytes(),
				&commonpb.PreparedQuery{
					Name:   queryName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
				})
			require.NoError(t, err)
			require.NoError(t, batch.Commit())

			_, err = query.Execute(context.Background(), rs, s, nil,
				attrs.Volume, attrs.PreparedQuery, attrs.Index,
				&servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledger,
					QueryName: queryName,
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
					Cursor:    tc.cursor,
				}, nil, nil)

			require.Error(t, err)

			// The contract is the classification, not the message: a validation
			// Describable maps to 400 / InvalidArgument, a bare error to 500 /
			// Internal.
			var describable domain.Describable
			require.ErrorAs(t, err, &describable,
				"a malformed cursor must classify as a validation error, not a server fault")
			require.Contains(t, err.Error(), "invalid cursor")
		})
	}
}
