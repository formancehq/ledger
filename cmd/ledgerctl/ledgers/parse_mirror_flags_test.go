package ledgers

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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

func TestParseMirrorFlags_AddressRewriteRules(t *testing.T) {
	t.Parallel()

	cmd := NewCreateCommand()
	require.NoError(t, cmd.ParseFlags([]string{
		"--mode=mirror",
		"--mirror-base-url=http://v2:3068",
		`--mirror-address-rewrite=(:worker:\d+)=`,
		"--mirror-address-rewrite=^payments:=psp:",
	}))

	_, cfg, err := parseMirrorFlags(cmd, "ledger-x")
	require.NoError(t, err)
	require.Len(t, cfg.GetAddressRewriteRules(), 2)
	require.Equal(t, `(:worker:\d+)`, cfg.GetAddressRewriteRules()[0].GetPattern())
	require.Equal(t, "", cfg.GetAddressRewriteRules()[0].GetReplacement())
	require.Equal(t, "^payments:", cfg.GetAddressRewriteRules()[1].GetPattern())
	require.Equal(t, "psp:", cfg.GetAddressRewriteRules()[1].GetReplacement())
}

func TestParseMirrorFlags_AddressRewriteInfersMirrorMode(t *testing.T) {
	t.Parallel()

	cmd := NewCreateCommand()
	require.NoError(t, cmd.ParseFlags([]string{
		"--mirror-base-url=http://v2:3068",
		`--mirror-address-rewrite=(:worker:\d+)=`,
	}))

	mode, cfg, err := parseMirrorFlags(cmd, "ledger-x")
	require.NoError(t, err)
	require.Equal(t, commonpb.LedgerMode_LEDGER_MODE_MIRROR, mode)
	require.Len(t, cfg.GetAddressRewriteRules(), 1)
}

func TestParseMirrorFlags_AddressRewriteMissingSeparatorRejected(t *testing.T) {
	t.Parallel()

	cmd := NewCreateCommand()
	require.NoError(t, cmd.ParseFlags([]string{
		"--mode=mirror",
		"--mirror-base-url=http://v2:3068",
		"--mirror-address-rewrite=no-separator",
	}))

	_, _, err := parseMirrorFlags(cmd, "ledger-x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "pattern=replacement")
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

func TestParseMirrorFlags_PostgresIAMAssumeRoleWiresArn(t *testing.T) {
	t.Parallel()

	cmd := NewCreateCommand()
	require.NoError(t, cmd.ParseFlags([]string{
		"--mode=mirror",
		"--mirror-source-type=postgres",
		"--mirror-dsn=postgres://iam-user@host:5432/db",
		"--mirror-aws-iam-region=eu-west-1",
		"--mirror-aws-iam-assume-role-arn=arn:aws:iam::222222222222:role/cross-tenant-mirror",
	}))

	_, cfg, err := parseMirrorFlags(cmd, "ledger-x")
	require.NoError(t, err)
	iam := cfg.GetPostgres().GetAwsIamAuth()
	require.Equal(t, "eu-west-1", iam.GetRegion())
	require.Equal(t, "arn:aws:iam::222222222222:role/cross-tenant-mirror", iam.GetAssumeRoleArn())
}

func TestParseMirrorFlags_PostgresEmptyAssumeRoleArnRejected(t *testing.T) {
	t.Parallel()

	cmd := NewCreateCommand()
	require.NoError(t, cmd.ParseFlags([]string{
		"--mode=mirror",
		"--mirror-source-type=postgres",
		"--mirror-dsn=postgres://iam-user@host:5432/db",
		"--mirror-aws-iam-region=eu-west-1",
		"--mirror-aws-iam-assume-role-arn=",
	}))

	_, _, err := parseMirrorFlags(cmd, "ledger-x")
	require.Error(t, err, "explicit but empty --mirror-aws-iam-assume-role-arn must NOT silently fall back to no-role IAM")
	require.Contains(t, err.Error(), "non-empty ARN")
}

func TestParseMirrorFlags_PostgresAssumeRoleWithoutRegionRejected(t *testing.T) {
	t.Parallel()

	cmd := NewCreateCommand()
	require.NoError(t, cmd.ParseFlags([]string{
		"--mode=mirror",
		"--mirror-source-type=postgres",
		"--mirror-dsn=postgres://iam-user@host:5432/db",
		"--mirror-aws-iam-assume-role-arn=arn:aws:iam::222222222222:role/cross-tenant-mirror",
	}))

	_, _, err := parseMirrorFlags(cmd, "ledger-x")
	require.Error(t, err, "assumeRoleArn without region must be rejected: AssumeRole without RDS IAM auth is meaningless")
	require.Contains(t, err.Error(), "requires --mirror-aws-iam-region")
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
