package cmd

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/url"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/formancehq/go-libs/v5/pkg/cloud/aws/iam"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/pflag"
	"github.com/xo/dburl"
)

// roleIAMConnector implements driver.Connector for AWS RDS IAM authentication
// under an assumed IAM role. A fresh RDS auth token (and, transparently, fresh
// STS role credentials) is obtained on every Connect call so that neither the
// 15-minute RDS token window nor the 1-hour STS session window can cause silent
// authentication failures after a long idle period.
type roleIAMConnector struct {
	dsn    string
	region string
	creds  stscreds.AssumeRoleProvider
}

// Connect obtains a short-lived RDS authentication token signed with the
// assumed-role credentials and uses it as the PostgreSQL password.
func (c *roleIAMConnector) Connect(ctx context.Context) (driver.Conn, error) {
	u, err := dburl.Parse(c.dsn)
	if err != nil {
		return nil, fmt.Errorf("aws: parsing postgres uri: %w", err)
	}

	authToken, err := auth.BuildAuthToken(
		ctx,
		u.Host,
		c.region,
		u.URL.User.Username(),
		&c.creds,
	)
	if err != nil {
		return nil, fmt.Errorf("aws: building rds iam auth token for assumed role: %w", err)
	}

	// Inject the auth token as the password in the DSN.
	authedURL := u.URL
	authedURL.User = url.UserPassword(u.URL.User.Username(), authToken)
	authedDSN := authedURL.String()

	pgxCfg, err := pgx.ParseConfig(authedDSN)
	if err != nil {
		return nil, fmt.Errorf("aws: parsing dsn with iam auth token: %w", err)
	}

	return stdlib.GetConnector(*pgxCfg).Connect(ctx)
}

// Driver returns the underlying driver. database/sql calls this only when
// sql.DB.Driver() is invoked by callers, which is uncommon.
func (c *roleIAMConnector) Driver() driver.Driver { return &roleIAMDriver{} }

// roleIAMDriver satisfies driver.Driver so that roleIAMConnector.Driver() has a
// concrete, non-nil return value.
type roleIAMDriver struct{}

func (d *roleIAMDriver) Open(name string) (driver.Conn, error) {
	return nil, fmt.Errorf("aws: roleIAMDriver.Open is not supported; use OpenConnector")
}

// connectionOptionsFromFlags is a drop-in replacement for
// connect.ConnectionOptionsFromFlags that additionally handles the
// --aws-role-arn flag.
//
// When --postgres-aws-enable-iam is true and --aws-role-arn is non-empty, the
// base AWS credentials loaded from the other IAM flags are wrapped with an STS
// AssumeRole provider. The RDS authentication token that is generated on every
// connection will therefore be signed by the assumed role's credentials rather
// than the caller's identity.
//
// In all other cases (IAM disabled, or no role ARN supplied) the function
// delegates directly to connect.ConnectionOptionsFromFlags, preserving
// identical behaviour.
func connectionOptionsFromFlags(flags *pflag.FlagSet, ctx context.Context) (*connect.ConnectionOptions, error) {
	awsEnable, _ := flags.GetBool(connect.PostgresAWSEnableIAMFlag)
	if !awsEnable {
		return connect.ConnectionOptionsFromFlags(flags, ctx)
	}

	roleArn, _ := flags.GetString(iam.AWSRoleArnFlag)
	if roleArn == "" {
		// No role assumption requested; existing code path handles this.
		return connect.ConnectionOptionsFromFlags(flags, ctx)
	}

	// Load the base AWS configuration from the remaining IAM flags
	// (region, static credentials, profile, etc.).
	cfg, err := awsconfig.LoadDefaultConfig(ctx, iam.LoadOptionFromFlags(flags))
	if err != nil {
		return nil, fmt.Errorf("aws: loading base aws config: %w", err)
	}

	// Wrap the base credentials with an STS AssumeRole provider.
	// stscreds.AssumeRoleProvider caches the temporary credentials and refreshes
	// them automatically before they expire, so callers do not need to manage
	// the STS session lifetime explicitly.
	stsClient := sts.NewFromConfig(cfg)
	assumeRoleProvider := stscreds.NewAssumeRoleProvider(stsClient, roleArn)

	postgresURI, _ := flags.GetString(connect.PostgresURIFlag)
	if postgresURI == "" {
		return nil, fmt.Errorf("aws: missing --%s", connect.PostgresURIFlag)
	}

	connector := &roleIAMConnector{
		dsn:    postgresURI,
		region: cfg.Region,
		creds:  *assumeRoleProvider,
	}

	maxIdleConns, _ := flags.GetInt(connect.PostgresMaxIdleConnsFlag)
	connMaxIdleTime, _ := flags.GetDuration(connect.PostgresConnMaxIdleTimeFlag)
	connMaxLifetime, _ := flags.GetDuration(connect.PostgresConnMaxLifetimeFlag)
	maxOpenConns, _ := flags.GetInt(connect.PostgresMaxOpenConnsFlag)

	return &connect.ConnectionOptions{
		DatabaseSourceName: postgresURI,
		MaxIdleConns:       maxIdleConns,
		ConnMaxIdleTime:    connMaxIdleTime,
		ConnMaxLifetime:    connMaxLifetime,
		MaxOpenConns:       maxOpenConns,
		Connector: func(dsn string) (driver.Connector, error) {
			return connector, nil
		},
	}, nil
}

