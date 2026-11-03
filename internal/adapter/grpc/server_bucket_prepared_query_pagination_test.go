package grpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestExecutePreparedQueryAdapterPagination(t *testing.T) {
	t.Parallel()
	for _, reverse := range []bool{false, true} {
		for _, tc := range []struct {
			name     string
			size     uint32
			expected uint32
			mode     ledgerpb.QueryMode
		}{
			{"default", 0, 100, ledgerpb.QueryMode_QUERY_MODE_LIST},
			{"maximum", 1000, 1000, ledgerpb.QueryMode_QUERY_MODE_LIST},
			{"over-maximum", 1001, 1000, ledgerpb.QueryMode_QUERY_MODE_LIST},
			{"uint32-maximum", 4294967295, 1000, ledgerpb.QueryMode_QUERY_MODE_LIST},
			{"aggregate", 4294967295, 4294967295, ledgerpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES},
		} {
			t.Run(fmt.Sprintf("%s/reverse=%t", tc.name, reverse), func(t *testing.T) {
				t.Parallel()
				controller := NewMockController(gomock.NewController(t))
				server := &BucketServiceServerImpl{ctrl: controller}
				request := &servicepb.ExecutePreparedQueryRequest{Ledger: "ledger1", QueryName: "accounts", PageSize: tc.size, Reverse: reverse, Mode: tc.mode, Cursor: "cursor", Parameters: map[string]*ledgerpb.ParameterValue{"address": {Value: &ledgerpb.ParameterValue_StringValue{StringValue: "users"}}}}
				original := proto.Clone(request).(*servicepb.ExecutePreparedQueryRequest)
				expected := &servicepb.ExecutePreparedQueryResponse{}
				controller.EXPECT().ExecutePreparedQuery(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, received *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
						require.Equal(t, tc.expected, received.GetPageSize())
						normalized := proto.Clone(original).(*servicepb.ExecutePreparedQueryRequest)
						normalized.PageSize = tc.expected
						require.True(t, proto.Equal(normalized, received), "all non-pagination request fields must be preserved")

						return expected, nil
					})
				response, err := server.ExecutePreparedQuery(context.Background(), request)
				require.NoError(t, err)
				require.Same(t, expected, response)
				require.True(t, proto.Equal(original, request), "adapter must preserve caller-owned request")
			})
		}
	}
}
