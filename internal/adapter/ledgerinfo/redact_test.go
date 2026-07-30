package ledgerinfo

import (
	"errors"
	"io"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestRedactSecrets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		original *commonpb.LedgerInfo
		check    func(*testing.T, *commonpb.LedgerInfo, *commonpb.LedgerInfo)
	}{
		{
			name: "HTTP OAuth credentials",
			original: &commonpb.LedgerInfo{Name: "mirror", MirrorSource: &commonpb.MirrorSourceConfig{
				LedgerName: "source",
				Type: &commonpb.MirrorSourceConfig_Http{Http: &commonpb.HttpMirrorSourceConfig{
					BaseUrl: "https://source.example",
					Oauth2ClientCredentials: &commonpb.OAuth2ClientCredentials{
						ClientId:      "client-id",
						ClientSecret:  "oauth-secret",
						TokenEndpoint: "https://source.example/oauth/token",
						Scopes:        []string{"ledger:read"},
					},
				}},
			}},
			check: func(t *testing.T, original, redacted *commonpb.LedgerInfo) {
				t.Helper()
				require.NotSame(t, original, redacted)
				assert.Empty(t, redacted.GetMirrorSource().GetHttp().GetOauth2ClientCredentials().GetClientSecret())
				assert.Equal(t, "client-id", redacted.GetMirrorSource().GetHttp().GetOauth2ClientCredentials().GetClientId())
				assert.Equal(t, "oauth-secret", original.GetMirrorSource().GetHttp().GetOauth2ClientCredentials().GetClientSecret())
			},
		},
		{
			name: "HTTP without OAuth credentials",
			original: &commonpb.LedgerInfo{Name: "mirror", MirrorSource: &commonpb.MirrorSourceConfig{
				Type: &commonpb.MirrorSourceConfig_Http{Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: "https://source.example"}},
			}},
			check: func(t *testing.T, original, redacted *commonpb.LedgerInfo) {
				t.Helper()
				assert.Nil(t, redacted.GetMirrorSource().GetHttp().GetOauth2ClientCredentials())
				assert.Equal(t, original.GetMirrorSource().GetHttp().GetBaseUrl(), redacted.GetMirrorSource().GetHttp().GetBaseUrl())
			},
		},
		{
			name:     "mirror source without type",
			original: &commonpb.LedgerInfo{Name: "mirror", MirrorSource: &commonpb.MirrorSourceConfig{LedgerName: "source"}},
			check: func(t *testing.T, original, redacted *commonpb.LedgerInfo) {
				t.Helper()
				assert.Equal(t, original, redacted)
				require.NotSame(t, original, redacted)
			},
		},
		{
			name:     "PostgreSQL URI",
			original: postgresLedger("postgres://user:p%40ss@db.example:5432/ledger?application_name=mirror&password=query-secret&sslmode=require"),
			check: func(t *testing.T, original, redacted *commonpb.LedgerInfo) {
				t.Helper()
				redactedDSN := redacted.GetMirrorSource().GetPostgres().GetDsn()
				assert.NotContains(t, redactedDSN, "p%40ss")
				assert.NotContains(t, redactedDSN, "query-secret")
				assert.Contains(t, redactedDSN, "db.example:5432/ledger")
				assert.Contains(t, redactedDSN, "application_name=mirror")
				assert.Contains(t, redactedDSN, "sslmode=require")

				parsed, err := url.Parse(redactedDSN)
				require.NoError(t, err)
				_, hasPassword := parsed.User.Password()
				assert.False(t, hasPassword)
				assert.False(t, parsed.Query().Has("password"))
				assert.Contains(t, original.GetMirrorSource().GetPostgres().GetDsn(), "query-secret")
			},
		},
		{
			name:     "unknown DSN form uses explicit marker",
			original: postgresLedger("host=db.example user=mirror password=keyword-secret"),
			check: func(t *testing.T, original, redacted *commonpb.LedgerInfo) {
				t.Helper()
				assert.Equal(t, redactedDSNPlaceholder, redacted.GetMirrorSource().GetPostgres().GetDsn())
				assert.Contains(t, original.GetMirrorSource().GetPostgres().GetDsn(), "keyword-secret")
			},
		},
		{
			name:     "Unix socket URI uses explicit marker",
			original: postgresLedger("postgres:///ledger?host=/var/run/postgresql&password=socket-secret"),
			check: func(t *testing.T, original, redacted *commonpb.LedgerInfo) {
				t.Helper()
				assert.Equal(t, redactedDSNPlaceholder, redacted.GetMirrorSource().GetPostgres().GetDsn())
				assert.Contains(t, original.GetMirrorSource().GetPostgres().GetDsn(), "socket-secret")
			},
		},
		{
			name:     "passwordless PostgreSQL URI is preserved",
			original: postgresLedger("postgres://iam-user@db.example/ledger?sslmode=require"),
			check: func(t *testing.T, original, redacted *commonpb.LedgerInfo) {
				t.Helper()
				assert.Equal(t, original.GetMirrorSource().GetPostgres().GetDsn(), redacted.GetMirrorSource().GetPostgres().GetDsn())
			},
		},
		{
			name:     "nil",
			original: nil,
			check: func(t *testing.T, _, redacted *commonpb.LedgerInfo) {
				t.Helper()
				assert.Nil(t, redacted)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.check(t, tt.original, RedactSecrets(tt.original))
		})
	}
}

func TestNewRedactingCursor(t *testing.T) {
	t.Parallel()

	original := postgresLedger("postgres://user:cursor-secret@db.example/ledger")
	inner := &redactingCursorTestSource{
		items:      []*commonpb.LedgerInfo{original},
		nextCursor: "leader-next-page",
	}
	redactedCursor := NewRedactingCursor(inner)

	redacted, err := redactedCursor.Next()
	require.NoError(t, err)
	assert.NotContains(t, redacted.GetMirrorSource().GetPostgres().GetDsn(), "cursor-secret")
	assert.Contains(t, original.GetMirrorSource().GetPostgres().GetDsn(), "cursor-secret")
	provider, ok := redactedCursor.(interface{ NextCursor() string })
	require.True(t, ok)
	assert.Equal(t, "leader-next-page", provider.NextCursor())
	require.NoError(t, redactedCursor.Close())
	assert.True(t, inner.closed)

	boom := errors.New("cursor failed")
	failing := NewRedactingCursor(&redactingCursorTestSource{nextErr: boom})
	_, err = failing.Next()
	require.ErrorIs(t, err, boom)
}

type redactingCursorTestSource struct {
	items      []*commonpb.LedgerInfo
	index      int
	nextCursor string
	nextErr    error
	closed     bool
}

func (c *redactingCursorTestSource) Next() (*commonpb.LedgerInfo, error) {
	if c.index < len(c.items) {
		item := c.items[c.index]
		c.index++

		return item, nil
	}
	if c.nextErr != nil {
		return nil, c.nextErr
	}

	return nil, io.EOF
}

func (c *redactingCursorTestSource) NextCursor() string {
	return c.nextCursor
}

func (c *redactingCursorTestSource) Close() error {
	c.closed = true

	return nil
}

func postgresLedger(dsn string) *commonpb.LedgerInfo {
	return &commonpb.LedgerInfo{
		Name: "mirror",
		MirrorSource: &commonpb.MirrorSourceConfig{
			LedgerName: "source",
			Type: &commonpb.MirrorSourceConfig_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfig{
				Dsn: dsn,
			}},
		},
	}
}
