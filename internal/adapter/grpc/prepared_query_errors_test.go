package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestExecutePreparedQueryErrorClassification(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		target  commonpb.QueryTarget
		mode    commonpb.QueryMode
		missing bool
		want    domain.Describable
		code    codes.Code
	}{
		{
			name: "aggregate transactions", target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
			mode: commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES,
			want: domain.ErrPreparedQueryAggregateTarget, code: codes.InvalidArgument,
		},
		{
			name: "aggregate logs", target: commonpb.QueryTarget_QUERY_TARGET_LOGS,
			mode: commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES,
			want: domain.ErrPreparedQueryAggregateTarget, code: codes.InvalidArgument,
		},
		{
			name: "unsupported mode", target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			mode: commonpb.QueryMode(999),
			want: domain.ErrQueryModeUnsupported, code: codes.InvalidArgument,
		},
		{
			name: "missing query", missing: true, mode: commonpb.QueryMode_QUERY_MODE_LIST,
			want: &domain.ErrPreparedQueryNotFound{Ledger: "ledger", Name: "query"}, code: codes.NotFound,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			logger := logging.Testing()
			store, err := dal.NewStore(t.TempDir(), logger, noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			rs, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, rs.Close()) })
			attrs := attributes.New()
			batch := store.OpenWriteSession()
			require.NoError(t, state.SaveLedger(batch, "ledger", &commonpb.LedgerInfo{Name: "ledger"}))
			if !tc.missing {
				_, err = attrs.PreparedQuery.Set(batch, domain.PreparedQueryKey{LedgerName: "ledger", Name: "query"}.Bytes(), &commonpb.PreparedQuery{
					Name: "query", Target: tc.target,
				})
				require.NoError(t, err)
			}
			require.NoError(t, batch.Commit())

			req := &servicepb.ExecutePreparedQueryRequest{Ledger: "ledger", QueryName: "query", Mode: tc.mode}
			// Run the real executor through the production unary error boundary:
			// replacing either validation guard with a bare error must fail here.
			_, err = errorConversionInterceptor(logger)(t.Context(), req, &ggrpc.UnaryServerInfo{}, func(ctx context.Context, request any) (any, error) {
				return query.Execute(ctx, rs, store, attrs.Volume, attrs.PreparedQuery, attrs.Index,
					request.(*servicepb.ExecutePreparedQueryRequest), nil, nil)
			})
			require.Error(t, err)
			st := status.Convert(err)
			require.Equal(t, tc.code, st.Code())
			require.Equal(t, tc.want.Error(), st.Message())
			info := extractErrorInfo(t, st)
			require.Equal(t, errorDomain, info.GetDomain())
			require.Equal(t, tc.want.Reason(), info.GetReason())
		})
	}
}
