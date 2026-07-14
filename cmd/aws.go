package cmd

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/pflag"
	"github.com/xo/dburl"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/go-libs/v5/pkg/cloud/aws/iam"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/connect"
)

// roleIAMTracer is the OpenTelemetry tracer used by the assumed-role pgx hooks.
var roleIAMTracer = otel.Tracer("github.com/formancehq/ledger/cmd")

// roleIAMPgxTracer mirrors the pgx tracing hooks that go-libs registers in
// buildPGXConnector so that the assumed-role code path emits the same spans.
type roleIAMPgxTracer struct{}

func (roleIAMPgxTracer) TraceConnectStart(ctx context.Context, _ pgx.TraceConnectStartData) context.Context {
	ctx, _ = roleIAMTracer.Start(ctx, "connect")
	return ctx
}

func (roleIAMPgxTracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	span := trace.SpanFromContext(ctx)
	defer span.End()
	if data.Err != nil {
		span.RecordError(data.Err)
	}
}

func (roleIAMPgxTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	ctx, _ = roleIAMTracer.Start(ctx, "query")
	return ctx
}

func (roleIAMPgxTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span := trace.SpanFromContext(ctx)
	defer span.End()
	if data.Err != nil {
		span.RecordError(data.Err)
	}
}

var (
	_ pgx.ConnectTracer = roleIAMPgxTracer{}
	_ pgx.QueryTracer   = roleIAMPgxTracer{}
)

const roleIAMProbeTimeout = 5 * time.Second

// validateReadWrite rejects read-replica connections by running
// "show transaction_read_only", mirroring go-libs'
// validateConnectTargetSessionAttrsReadWrite.
func validateReadWrite(ctx context.Context, pgConn *pgconn.PgConn) error {
	netConn := pgConn.Conn()
	if err := netConn.SetDeadline(time.Now().Add(roleIAMProbeTimeout)); err != nil {
		return err
	}
	defer netConn.SetDeadline(time.Time{}) //nolint:errcheck

	result, err := pgConn.Exec(ctx, "show transaction_read_only").ReadAll()
	if err != nil {
		return err
	}
	if len(result) > 0 && len(result[0].Rows) > 0 && string(result[0].Rows[0][0]) == "on" {
		return errors.New("aws: read-only connection rejected")
	}
	return nil
}

// resetReadOnly returns driver.ErrBadConn for connections that have drifted
// into read-only mode, mirroring go-libs' resetReadOnlySession hook.
func resetReadOnly(ctx context.Context, conn *pgx.Conn) error {
	var readOnly string
	if err := conn.QueryRow(ctx, "show transaction_read_only").Scan(&readOnly); err != nil {
		return driver.ErrBadConn
	}
	if readOnly == "on" {
		return driver.ErrBadConn
	}
	var defaultReadOnly string
	if err := conn.QueryRow(ctx, "show default_transaction_read_only").Scan(&defaultReadOnly); err != nil {
		return driver.ErrBadConn
	}
	if defaultReadOnly == "on" {
		return driver.ErrBadConn
	}
	return nil
}

// roleIAMConnector implements driver.Connector for AWS RDS IAM authentication
// under an assumed IAM role.
//
//   - A fresh RDS auth token is obtained on every Connect call (RDS tokens
//     expire after 15 minutes).
//   - The underlying STS credentials are managed by an aws.CredentialsCache
//     so that AssumeRole is called only when the cached session is about to
//     expire — not on every database connection.
//   - ValidateConnect, the pgx tracer, and OptionResetSession are applied so
//     that the assumed-role path has the same HA and tracing safeguards as the
//     regular go-libs IAM connector path.
type roleIAMConnector struct {
	dsn    string
	region string
	creds  aws.CredentialsProvider
}

// Connect obtains a fresh RDS IAM auth token and opens a physical connection.
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
		c.creds,
	)
	if err != nil {
		return nil, fmt.Errorf("aws: building rds iam auth token for assumed role: %w", err)
	}

	// Inject the auth token as the PostgreSQL password.
	authedURL := u.URL
	authedURL.User = url.UserPassword(u.URL.User.Username(), authToken)
	authedDSN := authedURL.String()

	pgxCfg, err := pgx.ParseConfig(authedDSN)
	if err != nil {
		return nil, fmt.Errorf("aws: parsing dsn with iam auth token: %w", err)
	}

	// Apply the same safeguards that go-libs' buildPGXConnector uses so that
	// the assumed-role path is equivalent to all other IAM auth paths.
	pgxCfg.ValidateConnect = validateReadWrite
	pgxCfg.Tracer = roleIAMPgxTracer{}

	return stdlib.GetConnector(*pgxCfg, stdlib.OptionResetSession(resetReadOnly)).Connect(ctx)
}

// Driver returns the underlying driver. database/sql calls this only when
// sql.DB.Driver() is invoked by callers, which is uncommon.
func (c *roleIAMConnector) Driver() driver.Driver { return &roleIAMDriver{} }

// roleIAMDriver satisfies driver.Driver so that roleIAMConnector.Driver() has
// a concrete, non-nil return value.
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
// AssumeRole provider. The RDS authentication token generated on every
// connection will be signed by the assumed role's credentials rather than the
// caller's identity.
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

	// Load the base AWS config from the remaining IAM flags
	// (region, static credentials, profile, etc.).
	cfg, err := awsconfig.LoadDefaultConfig(ctx, iam.LoadOptionFromFlags(flags))
	if err != nil {
		return nil, fmt.Errorf("aws: loading base aws config: %w", err)
	}

	// Wrap the base credentials with an STS AssumeRole provider, then cache
	// the resulting temporary credentials with aws.NewCredentialsCache.
	// Without the cache, stscreds.AssumeRoleProvider would call STS on every
	// new physical DB connection, adding latency and the risk of throttling
	// under connection churn. The cache reuses credentials until they are
	// about to expire and only then calls STS to refresh them.
	stsClient := sts.NewFromConfig(cfg)
	cachedCreds := aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(stsClient, roleArn))
	region := cfg.Region

	postgresURI, _ := flags.GetString(connect.PostgresURIFlag)
	if postgresURI == "" {
		return nil, fmt.Errorf("aws: missing --%s", connect.PostgresURIFlag)
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
		// Use the dsn argument (not the captured postgresURI) so that
		// OpenDBWithSchema's search_path injection and any other DSN
		// mutation applied by callers is honoured correctly.
		Connector: func(dsn string) (driver.Connector, error) {
			return &roleIAMConnector{
				dsn:    dsn,
				region: region,
				creds:  cachedCreds,
			}, nil
		},
	}, nil
}
