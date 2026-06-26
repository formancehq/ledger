package ledgers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseMirrorFlags_PostgresEmptyIAMRegionRejected(t *testing.T) {
	t.Parallel()

	cmd := NewCreateCommand()
	require.NoError(t, cmd.ParseFlags([]string{
		"--mode=mirror",
		"--mirror-source-type=postgres",
		"--mirror-dsn=postgres://iam-user@host:5432/db",
		"--mirror-aws-iam-region=",
	}))

	_, _, err := parseMirrorFlags(cmd, "ledger-x")
	require.Error(t, err, "explicit but empty --mirror-aws-iam-region must NOT silently fall back to password auth")
	require.Contains(t, err.Error(), "non-empty region")
}

func TestParseMirrorFlags_PostgresIAMRegionWiresAwsIamAuth(t *testing.T) {
	t.Parallel()

	cmd := NewCreateCommand()
	require.NoError(t, cmd.ParseFlags([]string{
		"--mode=mirror",
		"--mirror-source-type=postgres",
		"--mirror-dsn=postgres://iam-user@host:5432/db",
		"--mirror-aws-iam-region=eu-west-1",
	}))

	_, cfg, err := parseMirrorFlags(cmd, "ledger-x")
	require.NoError(t, err)
	require.Equal(t, "eu-west-1", cfg.GetPostgres().GetAwsIamAuth().GetRegion())
}

func TestParseMirrorFlags_PostgresNoIAMFlagLeavesAwsIamAuthNil(t *testing.T) {
	t.Parallel()

	cmd := NewCreateCommand()
	require.NoError(t, cmd.ParseFlags([]string{
		"--mode=mirror",
		"--mirror-source-type=postgres",
		"--mirror-dsn=postgres://user:pass@host:5432/db",
	}))

	_, cfg, err := parseMirrorFlags(cmd, "ledger-x")
	require.NoError(t, err)
	require.Nil(t, cfg.GetPostgres().GetAwsIamAuth())
}
