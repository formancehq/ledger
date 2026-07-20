package query_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// TestExecute_LedgerNotFound asserts that query.Execute maps the internal
// domain.ErrNotFound sentinel returned by GetLedgerByName to a typed
// *domain.ErrLedgerNotFound (Reason LEDGER_NOT_FOUND -> HTTP 404) for both a
// missing and a soft-deleted target ledger.
func TestExecute_LedgerNotFound(t *testing.T) {
	t.Parallel()

	logger := logging.FromContext(logging.TestingContext())

	for _, tc := range []struct {
		name string
		seed func(t *testing.T, s *dal.Store, ledger string)
	}{
		{
			name: "missing ledger",
			seed: func(*testing.T, *dal.Store, string) {},
		},
		{
			name: "soft-deleted ledger",
			seed: func(t *testing.T, s *dal.Store, ledger string) {
				batch := s.OpenWriteSession()
				require.NoError(t, state.SaveLedger(batch, &commonpb.LedgerInfo{
					Name:      ledger,
					CreatedAt: commonpb.NewTimestamp(libtime.Now()),
					DeletedAt: commonpb.NewTimestamp(libtime.Now()),
				}))
				require.NoError(t, batch.Commit())
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStore(t)
			attrs := attributes.New()

			rs, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
			require.NoError(t, err)
			t.Cleanup(func() { _ = rs.Close() })

			const ledger = "missing-ledger"
			tc.seed(t, s, ledger)

			req := &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledger,
				QueryName: "q",
			}

			_, err = query.Execute(context.Background(), rs, s, attrs.Volume, attrs.PreparedQuery, attrs.Index, req, nil, nil)
			require.Error(t, err)

			var notFound *domain.ErrLedgerNotFound
			require.ErrorAs(t, err, &notFound)
			require.Equal(t, ledger, notFound.Name)
		})
	}
}
