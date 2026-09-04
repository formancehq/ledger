package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/cloud/aws/iam"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/connect"
)

// TestWorkerCommandHasAWSIAMFlags verifies that the worker command registers
// all required AWS IAM flags, so that users running the standalone worker binary
// (e.g. on AWS ECS Fargate) can configure RDS IAM auth for the primary store
// via CLI flags, not just via environment variables.
func TestWorkerCommandHasAWSIAMFlags(t *testing.T) {
	t.Parallel()

	cmd := NewWorkerCommand()
	flags := cmd.Flags()

	// These flags come from connect.AddFlags and enable IAM on the primary store
	t.Run("postgres-aws-enable-iam flag is registered", func(t *testing.T) {
		t.Parallel()
		f := flags.Lookup(connect.PostgresAWSEnableIAMFlag)
		require.NotNil(t, f, "--postgres-aws-enable-iam flag must be registered on the worker command")
		assert.Equal(t, "false", f.DefValue)
	})

	// These flags come from iam.AddFlags (added to fix issue #1556).
	t.Run("aws-region flag is registered", func(t *testing.T) {
		t.Parallel()
		f := flags.Lookup(iam.AWSRegionFlag)
		require.NotNil(t, f, "--aws-region flag must be registered on the worker command")
	})

	t.Run("aws-access-key-id flag is registered", func(t *testing.T) {
		t.Parallel()
		f := flags.Lookup(iam.AWSAccessKeyIDFlag)
		require.NotNil(t, f, "--aws-access-key-id flag must be registered on the worker command")
	})

	t.Run("aws-secret-access-key flag is registered", func(t *testing.T) {
		t.Parallel()
		f := flags.Lookup(iam.AWSSecretAccessKeyFlag)
		require.NotNil(t, f, "--aws-secret-access-key flag must be registered on the worker command")
	})

	t.Run("aws-session-token flag is registered", func(t *testing.T) {
		t.Parallel()
		f := flags.Lookup(iam.AWSSessionTokenFlag)
		require.NotNil(t, f, "--aws-session-token flag must be registered on the worker command")
	})

	t.Run("aws-profile flag is registered", func(t *testing.T) {
		t.Parallel()
		f := flags.Lookup(iam.AWSProfileFlag)
		require.NotNil(t, f, "--aws-profile flag must be registered on the worker command")
	})

	// --aws-role-arn is registered by iam.AddFlags but is hidden on the worker
	// command until iam.LoadOptionFromFlags in go-libs consumes it. Exposing a
	// no-op flag would silently mislead operators configuring RDS IAM role
	// assumption. Full support is tracked for a future go-libs update.
	t.Run("aws-role-arn flag is registered but hidden", func(t *testing.T) {
		t.Parallel()
		f := flags.Lookup(iam.AWSRoleArnFlag)
		require.NotNil(t, f, "--aws-role-arn flag must be registered on the worker command")
		assert.True(t, f.Hidden, "--aws-role-arn must be hidden until go-libs consumes it")
	})
}

// TestWorkerCommandRejectsRoleArnWithIAM verifies that the worker command
// returns a clear error when --aws-role-arn is combined with
// --postgres-aws-enable-iam, rather than silently ignoring the ARN.
func TestWorkerCommandRejectsRoleArnWithIAM(t *testing.T) {
	t.Parallel()

	cmd := NewWorkerCommand()
	require.NoError(t, cmd.Flags().Set(connect.PostgresAWSEnableIAMFlag, "true"))
	require.NoError(t, cmd.Flags().Set(iam.AWSRoleArnFlag, "arn:aws:iam::123456789012:role/MyRole"))

	err := cmd.RunE(cmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--"+iam.AWSRoleArnFlag)
	assert.Contains(t, err.Error(), "not yet supported")
}

// the same AWS IAM flags for parity (regression guard).
func TestServeCommandHasAWSIAMFlags(t *testing.T) {
	t.Parallel()

	cmd := NewServeCommand()
	flags := cmd.Flags()

	// Verify the IAM-enable flag defaults to false on serve as well
	f := flags.Lookup(connect.PostgresAWSEnableIAMFlag)
	require.NotNil(t, f, "--postgres-aws-enable-iam flag must be registered on the serve command")
	assert.Equal(t, "false", f.DefValue)

	iamFlags := []string{
		connect.PostgresAWSEnableIAMFlag,
		iam.AWSRegionFlag,
		iam.AWSAccessKeyIDFlag,
		iam.AWSSecretAccessKeyFlag,
		iam.AWSSessionTokenFlag,
		iam.AWSProfileFlag,
		iam.AWSRoleArnFlag,
	}

	for _, flagName := range iamFlags {
		t.Run(flagName+" is registered on serve", func(t *testing.T) {
			t.Parallel()
			f := flags.Lookup(flagName)
			require.NotNil(t, f, "--%s flag must be registered on the serve command", flagName)
		})
	}

}
