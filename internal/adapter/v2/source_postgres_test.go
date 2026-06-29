package v2

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

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

func TestBuildPgxPoolConfig_IAMRejectsAdversarialKeywordDSN(t *testing.T) {
	t.Parallel()

	// Bypass attempt flagged by NumaryBot: a libpq keyword=value DSN with
	// a quoted application_name embedding "sslmode=require" but the real
	// sslmode is "disable". A naive whitespace-split scanner would pick
	// the embedded value; pgxpool.ParseConfig sees the actual sslmode.
	dsn := `host=db.example.com user=iam-user dbname=ledger application_name='x sslmode=require y' sslmode=disable`

	_, err := buildPgxPoolConfig(context.Background(), &commonpb.PostgresMirrorSourceConfig{
		Dsn:        dsn,
		AwsIamAuth: &commonpb.PostgresAwsIamAuth{Region: "eu-west-1"},
	})
	require.Error(t, err, "adversarial keyword=value DSN with embedded sslmode= substring must not bypass the TLS gate")
	require.Contains(t, err.Error(), "sslmode")
}

func TestBuildPgxPoolConfig_IAMRejectsNonTLSSSLMode(t *testing.T) {
	t.Parallel()

	// The SigV4 token is a 15-min bearer credential; sslmode in
	// {disable, allow, prefer, ""} would let it travel in cleartext.
	for _, mode := range []string{"disable", "allow", "prefer", ""} {
		t.Run("sslmode="+mode, func(t *testing.T) {
			t.Parallel()

			dsn := "postgres://iam-user@host:5432/db"
			if mode != "" {
				dsn += "?sslmode=" + mode
			}

			_, err := buildPgxPoolConfig(context.Background(), &commonpb.PostgresMirrorSourceConfig{
				Dsn:        dsn,
				AwsIamAuth: &commonpb.PostgresAwsIamAuth{Region: "eu-west-1"},
			})
			require.Error(t, err, "sslmode=%q must be rejected when awsIamAuth is set", mode)
			require.Contains(t, err.Error(), "sslmode")
		})
	}
}

func TestBuildPgxPoolConfig_IAMAcceptsTLSSSLModes(t *testing.T) {
	// Cannot run in parallel: t.Setenv mutates process env, and the AWS SDK
	// default credential chain resolves env lazily inside BuildAuthToken.
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIATEST")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "SECRETTEST")
	t.Setenv("AWS_REGION", "eu-west-1")

	for _, mode := range []string{"require", "verify-ca", "verify-full"} {
		cfg, err := buildPgxPoolConfig(context.Background(), &commonpb.PostgresMirrorSourceConfig{
			Dsn:        "postgres://iam-user@host:5432/db?sslmode=" + mode,
			AwsIamAuth: &commonpb.PostgresAwsIamAuth{Region: "eu-west-1"},
		})
		require.NoError(t, err, "sslmode=%q must be accepted with awsIamAuth", mode)
		require.NotNil(t, cfg.BeforeConnect, "BeforeConnect must be installed for sslmode=%q", mode)
	}
}

func TestBuildPgxPoolConfig_IAMAssumeRoleInstallsBeforeConnect(t *testing.T) {
	t.Parallel()

	// AssumeRoleArn branch wires an STS-AssumeRole credentials provider before
	// installing iamBeforeConnect. We only assert the hook is in place; the
	// actual sts:AssumeRole call would only fire on connect (no real network
	// here) and is mocked away by NewCredentialsCache's lazy semantics.
	cfg, err := buildPgxPoolConfig(context.Background(), &commonpb.PostgresMirrorSourceConfig{
		Dsn: "postgres://iam-user@host:5432/db?sslmode=require",
		AwsIamAuth: &commonpb.PostgresAwsIamAuth{
			Region:        "eu-west-1",
			AssumeRoleArn: "arn:aws:iam::222222222222:role/cross-tenant-mirror",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, cfg.BeforeConnect, "BeforeConnect must be installed even when AssumeRoleArn is set")
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
