package grpcerr

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
)

func TestSigningStatusRoundTrip(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		err  error
		code codes.Code
		kind domain.ErrorKind
	}{
		{signing.ErrMissingSignature, codes.Unauthenticated, domain.KindUnauthenticated},
		{signing.ErrInvalidSignature, codes.PermissionDenied, domain.KindPermissionDenied},
		{signing.ErrUnknownKeyID, codes.PermissionDenied, domain.KindPermissionDenied},
	} {
		t.Run(tc.err.Error(), func(t *testing.T) {
			t.Parallel()
			original := fmt.Errorf("verification: %w", tc.err)
			st := SigningStatus(original)
			require.NotNil(t, st)
			require.Equal(t, tc.code, st.Code())
			require.Equal(t, original.Error(), st.Message())
			decoded := FromStatusError(st.Err())
			classified, ok := errors.AsType[domain.Classifiable](decoded)
			require.True(t, ok)
			require.Equal(t, tc.kind, classified.Kind())
			require.Equal(t, st.Proto(), OriginalStatus(fmt.Errorf("routing: %w", decoded)).Proto())
		})
	}
	require.Nil(t, SigningStatus(errors.New("unrelated")))
}

func TestSigningStatusDoesNotReclassifyTransportFailures(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		code           codes.Code
		domain, reason string
	}{
		{codes.Unauthenticated, "", ""}, {codes.PermissionDenied, "", ""},
		{codes.PermissionDenied, "another-service", "INVALID_SIGNATURE"},
		{codes.Internal, "ledger.signing", "INVALID_SIGNATURE"},
		{codes.PermissionDenied, "ledger.signing", "UNKNOWN_REASON"},
		{codes.PermissionDenied, "ledger.signing", "MISSING_SIGNATURE"},
	} {
		t.Run(fmt.Sprint(tc), func(t *testing.T) {
			t.Parallel()
			st := status.New(tc.code, "private peer credentials")
			if tc.domain != "" {
				var err error
				st, err = st.WithDetails(&errdetails.ErrorInfo{Domain: tc.domain, Reason: tc.reason})
				require.NoError(t, err)
			}
			decoded := FromStatusError(st.Err())
			_, ok := errors.AsType[domain.Classifiable](decoded)
			require.False(t, ok)
			require.Nil(t, OriginalStatus(decoded))
		})
	}
}
