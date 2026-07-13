package http

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Structural decode of the `filter` field (JSON DSL and textual filterexpr) now
// lives in the shared filterexpr.DecodeDualFormat helper; see
// internal/pkg/filterexpr/decode_test.go for its coverage.

func TestParsePreparedQueryTarget(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		target  string
		want    commonpb.QueryTarget
		wantErr string
	}{
		{name: "accounts", target: "ACCOUNTS", want: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS},
		{name: "transactions", target: "TRANSACTIONS", want: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS},
		{name: "logs", target: "LOGS", want: commonpb.QueryTarget_QUERY_TARGET_LOGS},
		{name: "empty", target: "", wantErr: "target is required"},
		{name: "unknown", target: "BOGUS", wantErr: "unknown or unsupported target"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := parsePreparedQueryTarget(tc.target)
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
