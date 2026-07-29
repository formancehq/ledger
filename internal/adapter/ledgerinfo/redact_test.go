package ledgerinfo

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestRedactSecrets(t *testing.T) {
	t.Parallel()

	t.Run("HTTP OAuth credentials", func(t *testing.T) {
		t.Parallel()

		original := &commonpb.LedgerInfo{
			Name: "mirror",
			MirrorSource: &commonpb.MirrorSourceConfig{
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
			},
		}

		redacted := RedactSecrets(original)

		require.NotSame(t, original, redacted)
		assert.Empty(t, redacted.GetMirrorSource().GetHttp().GetOauth2ClientCredentials().GetClientSecret())
		assert.Equal(t, "client-id", redacted.GetMirrorSource().GetHttp().GetOauth2ClientCredentials().GetClientId())
		assert.Equal(t, "oauth-secret", original.GetMirrorSource().GetHttp().GetOauth2ClientCredentials().GetClientSecret())
	})

	t.Run("PostgreSQL URI", func(t *testing.T) {
		t.Parallel()

		original := postgresLedger("postgres://user:p%40ss@db.example:5432/ledger?application_name=mirror&password=query-secret&sslmode=require")
		redacted := RedactSecrets(original)
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
	})

	t.Run("unknown DSN form fails closed", func(t *testing.T) {
		t.Parallel()

		original := postgresLedger("host=db.example user=mirror password=keyword-secret")
		redacted := RedactSecrets(original)

		assert.Empty(t, redacted.GetMirrorSource().GetPostgres().GetDsn())
		assert.Contains(t, original.GetMirrorSource().GetPostgres().GetDsn(), "keyword-secret")
	})

	t.Run("passwordless PostgreSQL URI is preserved", func(t *testing.T) {
		t.Parallel()

		dsn := "postgres://iam-user@db.example/ledger?sslmode=require"
		redacted := RedactSecrets(postgresLedger(dsn))

		assert.Equal(t, dsn, redacted.GetMirrorSource().GetPostgres().GetDsn())
	})

	t.Run("nil", func(t *testing.T) {
		t.Parallel()

		assert.Nil(t, RedactSecrets(nil))
	})
}

func TestNewRedactingCursor(t *testing.T) {
	t.Parallel()

	original := postgresLedger("postgres://user:cursor-secret@db.example/ledger")
	redactedCursor := NewRedactingCursor(cursor.NewSliceCursor([]*commonpb.LedgerInfo{original}))

	redacted, err := redactedCursor.Next()
	require.NoError(t, err)
	assert.NotContains(t, redacted.GetMirrorSource().GetPostgres().GetDsn(), "cursor-secret")
	assert.Contains(t, original.GetMirrorSource().GetPostgres().GetDsn(), "cursor-secret")
	require.NoError(t, redactedCursor.Close())
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
