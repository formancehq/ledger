package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v4/oidc"
)

func TestWithClaimsAndClaimsFromContext(t *testing.T) {
	t.Parallel()

	claims := &oidc.AccessTokenClaims{}
	claims.Subject = "user-123"
	claims.Scopes = oidc.SpaceDelimitedArray{"ledger:read", "ledger:write"}

	ctx := WithClaims(context.Background(), claims)

	got := ClaimsFromContext(ctx)
	require.NotNil(t, got)
	require.Equal(t, "user-123", got.GetSubject())
	require.Equal(t, oidc.SpaceDelimitedArray{"ledger:read", "ledger:write"}, got.Scopes)
}

func TestClaimsFromContextNil(t *testing.T) {
	t.Parallel()

	got := ClaimsFromContext(context.Background())
	require.Nil(t, got)
}

func TestSubjectFromContext(t *testing.T) {
	t.Parallel()

	claims := &oidc.AccessTokenClaims{}
	claims.Subject = "user-456"

	ctx := WithClaims(context.Background(), claims)
	require.Equal(t, "user-456", SubjectFromContext(ctx))
}

func TestSubjectFromContextEmpty(t *testing.T) {
	t.Parallel()

	require.Empty(t, SubjectFromContext(context.Background()))
}
