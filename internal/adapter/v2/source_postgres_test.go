package v2

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestEncodeDSNPassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no special chars",
			input:    "postgres://user:simple@host:5432/db",
			expected: "postgres://user:simple@host:5432/db",
		},
		{
			name:     "password with pipe and question mark",
			input:    "postgres://formance:YCA[sRR-~X]Pqdv|Ms3?hzc0u#f_@host:5432/db",
			expected: "postgres://formance:YCA%5BsRR-~X%5DPqdv%7CMs3%3Fhzc0u%23f_@host:5432/db",
		},
		{
			name:     "keyword value format unchanged",
			input:    "host=localhost user=admin password=secret dbname=mydb",
			expected: "host=localhost user=admin password=secret dbname=mydb",
		},
		{
			name:     "no password",
			input:    "postgres://user@host:5432/db",
			expected: "postgres://user@host:5432/db",
		},
		{
			name:     "no credentials",
			input:    "postgres://host:5432/db",
			expected: "postgres://host:5432/db",
		},
		{
			name:     "postgresql scheme",
			input:    "postgresql://user:p@ss|word@host:5432/db",
			expected: "postgresql://user:p%40ss%7Cword@host:5432/db",
		},
		{
			// Operator's controller assembles DSNs via url.URL.String(), so
			// the password reaches this helper already percent-encoded. Without
			// idempotency the % itself would be re-encoded (%40 -> %2540),
			// breaking authentication silently.
			name:     "already-encoded password is left untouched (idempotent)",
			input:    "postgres://user:p%40ss%7Cword%3F%23@host:5432/db",
			expected: "postgres://user:p%40ss%7Cword%3F%23@host:5432/db",
		},
		{
			// A literal '%' that does not form a valid escape sequence is
			// detected (PathUnescape errors), so we still encode it.
			name:     "literal percent sign is escaped",
			input:    "postgres://user:50%off@host:5432/db",
			expected: "postgres://user:50%25off@host:5432/db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, encodeDSNPassword(tt.input))
		})
	}
}

func TestBuildPgxPoolConfig_NoIAMLeavesBeforeConnectNil(t *testing.T) {
	t.Parallel()

	cfg, err := buildPgxPoolConfig(context.Background(), &commonpb.PostgresMirrorSourceConfig{
		Dsn: "postgres://user:pass@host:5432/db?sslmode=disable",
	})
	require.NoError(t, err)
	require.Nil(t, cfg.BeforeConnect, "BeforeConnect must remain nil when IAM auth is not configured")
	require.Equal(t, "host", cfg.ConnConfig.Host)
}

func TestBuildPgxPoolConfig_IAMRegionRequired(t *testing.T) {
	t.Parallel()

	_, err := buildPgxPoolConfig(context.Background(), &commonpb.PostgresMirrorSourceConfig{
		Dsn:        "postgres://iam-user@host:5432/db?sslmode=require",
		AwsIamAuth: &commonpb.PostgresAwsIamAuth{Region: ""},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "region is required")
}

func TestBuildPgxPoolConfig_IAMWiresBeforeConnect(t *testing.T) {
	// Cannot run in parallel: t.Setenv mutates process env, and the AWS SDK
	// default credential chain resolves env lazily inside BuildAuthToken.
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIATEST")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "SECRETTEST")
	t.Setenv("AWS_REGION", "eu-west-1")

	cfg, err := buildPgxPoolConfig(context.Background(), &commonpb.PostgresMirrorSourceConfig{
		Dsn: "postgres://iam-user@db.example.com:5432/app?sslmode=require",
		AwsIamAuth: &commonpb.PostgresAwsIamAuth{
			Region: "eu-west-1",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, cfg.BeforeConnect, "WithPgxPoolIAMAuth must install a BeforeConnect hook")

	// Sanity-check the hook actually mints a SigV4-signed RDS IAM token and
	// overrides the connection password.
	require.NoError(t, cfg.BeforeConnect(context.Background(), cfg.ConnConfig))
	pw := cfg.ConnConfig.Password
	require.NotEmpty(t, pw, "expected BeforeConnect to set ConnConfig.Password")
	require.True(t, strings.Contains(pw, "X-Amz-Signature"), "expected SigV4 signature, got %q", pw)
	require.True(t, strings.Contains(pw, "DBUser=iam-user"), "expected DBUser claim, got %q", pw)
}
