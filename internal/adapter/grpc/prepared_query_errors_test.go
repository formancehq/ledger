package grpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/query"
)

// TestExecutePreparedQueryErrorClassification pins the EN-2081 contract for the
// classification-only tier. Both errors reject a malformed
// ExecutePreparedQuery request, so the caller must see InvalidArgument rather
// than the codes.Unknown the sanitiser answered when they were bare
// errors.New values.
//
// Neither carries an ErrorInfo, and that absence is the point: a Reason is a
// versioned wire contract, and these two never declared one. Attaching a
// borrowed reason here would ship an identifier clients could start matching
// on and the server could then never rename.
func TestExecutePreparedQueryErrorClassification(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		err     error
		message string
	}{
		{
			name:    "aggregate on a non-accounts target",
			err:     &query.ErrPreparedQueryAggregateTarget{Target: ledgerpb.QueryTarget_QUERY_TARGET_TRANSACTIONS},
			message: "invalid request",
		},
		{
			name:    "unsupported query mode",
			err:     &query.ErrQueryModeUnsupported{Mode: ledgerpb.QueryMode(99)},
			message: "invalid request",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := status.Convert(convertToGRPCError(tc.err, testLogger()))

			require.Equal(t, codes.InvalidArgument, st.Code())
			require.Equal(t, tc.message, st.Message())

			for _, detail := range st.Details() {
				_, isErrorInfo := detail.(*errdetails.ErrorInfo)
				require.False(t, isErrorInfo,
					"a Classifiable owns no public Reason, so it must not ship an ErrorInfo")
			}
		})
	}
}

// TestQueryArgumentErrorsAreClassifiableOnly states the tier directly, so a
// later change that quietly adds Reason() to one of these types — and with it a
// permanent wire contract — fails here rather than shipping.
func TestQueryArgumentErrorsAreClassifiableOnly(t *testing.T) {
	t.Parallel()

	for name, err := range map[string]domain.Classifiable{
		"ErrPreparedQueryAggregateTarget": &query.ErrPreparedQueryAggregateTarget{},
		"ErrQueryModeUnsupported":         &query.ErrQueryModeUnsupported{},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, domain.KindValidation, err.Kind())

			_, describable := any(err).(domain.Describable)
			require.False(t, describable,
				"%s must not declare a Reason: it would become a wire contract this build can never rename", name)
		})
	}
}
