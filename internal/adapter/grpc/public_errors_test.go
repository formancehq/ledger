package grpc

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/state"
)

func TestGRPCPublicInternalDetails(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		err     domain.Describable
		message string
	}{
		{"index", &domain.ErrIndexInconsistent{Index: "private-index", Detail: "reading /private/pebble: secret failure"}, "index is inconsistent"},
		{"coverage", &state.ErrCoverageMiss{Attribute: "private-attribute", CanonicalHex: "deadbeef", IDHex: "0102", RaftIndex: 42}, "preload coverage miss"},
	} {
		for _, wrapped := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/wrapped=%t", tc.name, wrapped), func(t *testing.T) {
				t.Parallel()
				var err error = tc.err
				if wrapped {
					err = fmt.Errorf("private wrapper: %w", &domain.BusinessError{Err: tc.err})
				}
				var logs bytes.Buffer
				st := status.Convert(convertToGRPCError(err, logging.NewDefaultLogger(&logs, false, false, false)))
				require.Equal(t, codes.Internal, st.Code())
				require.Equal(t, tc.message, st.Message())
				info := extractErrorInfo(t, st)
				require.Equal(t, tc.err.Reason(), info.GetReason())
				require.Equal(t, errorDomain, info.GetDomain())
				require.Empty(t, info.GetMetadata())
				require.Contains(t, logs.String(), tc.err.Error())
				require.Contains(t, logs.String(), "correlation_id")
			})
		}
	}
}

func TestGRPCPublicDetailsPreserveNumscriptDiagnostics(t *testing.T) {
	t.Parallel()
	err := &domain.ErrNumscriptRuntime{Detail: "negative posting amount"}
	st := status.Convert(convertToGRPCError(&domain.BusinessError{Err: err}, logging.Testing()))
	require.Equal(t, codes.Internal, st.Code())
	require.Equal(t, err.Error(), st.Message())
	info := extractErrorInfo(t, st)
	require.Equal(t, domain.ErrReasonNumscriptRuntime, info.GetReason())
	require.Equal(t, map[string]string{"detail": "negative posting amount"}, info.GetMetadata())
}
