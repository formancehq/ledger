package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsRetryableMembershipRefreshError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		code codes.Code
		want bool
	}{
		{name: "unavailable", code: codes.Unavailable, want: true},
		{name: "old peer missing rpc", code: codes.Unimplemented, want: true},
		{name: "invalid request", code: codes.InvalidArgument, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, isRetryableMembershipRefreshError(status.Error(tc.code, "refresh failed")))
		})
	}
}
