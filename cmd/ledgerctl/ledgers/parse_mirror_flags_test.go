package ledgers

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestParseMirrorFlags_PostgresEmptyIAMRegionRejected(t *testing.T) {
	t.Parallel()

	cmd := NewCreateCommand()
	require.NoError(t, cmd.ParseFlags([]string{
		"--mode=mirror",
		"--mirror-source-type=ledgerV2Database",
		"--mirror-dsn=postgres://iam-user@host:5432/db",
		"--mirror-aws-iam-region=",
	}))

	_, _, err := parseMirrorFlags(cmd, "ledger-x")
	require.Error(t, err, "explicit but empty --mirror-aws-iam-region must NOT silently fall back to password auth")
	require.Contains(t, err.Error(), "non-empty region")
}

func TestParseMirrorFlags_RewriteRulesInline(t *testing.T) {
	t.Parallel()

	cmd := NewCreateCommand()
	require.NoError(t, cmd.ParseFlags([]string{
		"--mode=mirror",
		"--mirror-base-url=http://v2:3068",
		"--mirror-rewrite-rule", `{"anyVariant":{"actions":[{"rewriteAddress":{"pattern":":worker:\\d+","replacement":""}}]}}`,
		"--mirror-rewrite-rule", `{"createdTransaction":{"match":"log.metadata[\"type\"].string_value == \"payout\"","actions":[{"setMetadata":{"key":"category","value":"external"}}]},"stop":true}`,
	}))

	_, cfg, err := parseMirrorFlags(cmd, "ledger-x")
	require.NoError(t, err)
	require.Len(t, cfg.GetRewriteRules(), 2)

	// Rule 0: any-variant, rewrite_address action.
	anyRule := cfg.GetRewriteRules()[0].GetAnyVariant()
	require.NotNil(t, anyRule)
	require.Len(t, anyRule.GetActions(), 1)
	require.Equal(t, ":worker:\\d+", anyRule.GetActions()[0].GetRewriteAddress().GetPattern())

	// Rule 1: created scope, match + set_metadata, stop.
	created := cfg.GetRewriteRules()[1].GetCreatedTransaction()
	require.NotNil(t, created)
	require.Equal(t, `log.metadata["type"].string_value == "payout"`, created.GetMatch())
	require.True(t, cfg.GetRewriteRules()[1].GetStop())
}

func TestParseMirrorFlags_RewriteFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "rules.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
- anyVariant:
    actions:
      - rewriteAddress: {pattern: ":worker:\\d+", replacement: ""}
- createdTransaction:
    match: 'log.metadata["type"].string_value == "payout"'
    actions:
      - setMetadata: {key: category, value: external}
  stop: true
`), 0o600))

	cmd := NewCreateCommand()
	require.NoError(t, cmd.ParseFlags([]string{
		"--mode=mirror",
		"--mirror-base-url=http://v2:3068",
		"--mirror-rewrite-file=" + path,
	}))

	_, cfg, err := parseMirrorFlags(cmd, "ledger-x")
	require.NoError(t, err)
	require.Len(t, cfg.GetRewriteRules(), 2)
	require.NotNil(t, cfg.GetRewriteRules()[0].GetAnyVariant())
	require.True(t, cfg.GetRewriteRules()[1].GetStop())
}

func TestParseMirrorFlags_RewriteInfersMirrorMode(t *testing.T) {
	t.Parallel()

	cmd := NewCreateCommand()
	require.NoError(t, cmd.ParseFlags([]string{
		"--mirror-base-url=http://v2:3068",
		"--mirror-rewrite-rule", `{"anyVariant":{"match":"true"}}`,
	}))

	mode, cfg, err := parseMirrorFlags(cmd, "ledger-x")
	require.NoError(t, err)
	require.Equal(t, commonpb.LedgerMode_LEDGER_MODE_MIRROR, mode)
	require.Len(t, cfg.GetRewriteRules(), 1)
}

func TestParseMirrorFlags_PostgresIAMRegionWiresAwsIamAuth(t *testing.T) {
	t.Parallel()

	cmd := NewCreateCommand()
	require.NoError(t, cmd.ParseFlags([]string{
		"--mode=mirror",
		"--mirror-source-type=ledgerV2Database",
		"--mirror-dsn=postgres://iam-user@host:5432/db",
		"--mirror-aws-iam-region=eu-west-1",
	}))

	_, cfg, err := parseMirrorFlags(cmd, "ledger-x")
	require.NoError(t, err)
	require.Equal(t, "eu-west-1", cfg.GetLedgerV2Database().GetAwsIamAuth().GetRegion())
}

func TestParseMirrorFlags_PostgresIAMAssumeRoleWiresArn(t *testing.T) {
	t.Parallel()

	cmd := NewCreateCommand()
	require.NoError(t, cmd.ParseFlags([]string{
		"--mode=mirror",
		"--mirror-source-type=ledgerV2Database",
		"--mirror-dsn=postgres://iam-user@host:5432/db",
		"--mirror-aws-iam-region=eu-west-1",
		"--mirror-aws-iam-assume-role-arn=arn:aws:iam::222222222222:role/cross-tenant-mirror",
	}))

	_, cfg, err := parseMirrorFlags(cmd, "ledger-x")
	require.NoError(t, err)
	iam := cfg.GetLedgerV2Database().GetAwsIamAuth()
	require.Equal(t, "eu-west-1", iam.GetRegion())
	require.Equal(t, "arn:aws:iam::222222222222:role/cross-tenant-mirror", iam.GetAssumeRoleArn())
}

func TestParseMirrorFlags_PostgresEmptyAssumeRoleArnRejected(t *testing.T) {
	t.Parallel()

	cmd := NewCreateCommand()
	require.NoError(t, cmd.ParseFlags([]string{
		"--mode=mirror",
		"--mirror-source-type=ledgerV2Database",
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
		"--mirror-source-type=ledgerV2Database",
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
		"--mirror-source-type=ledgerV2Database",
		"--mirror-dsn=postgres://user:pass@host:5432/db",
	}))

	_, cfg, err := parseMirrorFlags(cmd, "ledger-x")
	require.NoError(t, err)
	require.Nil(t, cfg.GetLedgerV2Database().GetAwsIamAuth())
}
