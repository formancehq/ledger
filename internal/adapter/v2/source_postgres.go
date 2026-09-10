package v2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"

	"github.com/formancehq/ledger/v3/internal/domain/connectionconfig"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

var mirrorTracer = otel.Tracer("mirror.v2.postgres")

// PostgresSource is a Source that reads logs directly from a v2 ledger's
// PostgreSQL database. In v2, the _system.ledgers table maps each ledger
// to a bucket (PostgreSQL schema). The logs table lives in that schema
// and uses a ledger column to distinguish between ledgers sharing a bucket.
type PostgresSource struct {
	pool       *pgxpool.Pool
	ledgerName string
	bucket     string // PostgreSQL schema containing the logs table
}

// NewPostgresSource creates a new PostgreSQL-based v2 log source.
// It looks up the bucket (schema) for the given ledger from _system.ledgers.
// When cfg.AwsIamAuth is set, the pool refreshes an AWS RDS IAM token on every
// new connection; ambient AWS credentials (IRSA, instance profile, env, profile)
// are loaded via the default AWS SDK chain.
func NewPostgresSource(ctx context.Context, cfg *commonpb.PostgresMirrorSourceConfig, ledgerName string) (*PostgresSource, error) {
	poolCfg, err := buildPgxPoolConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("creating pgx pool: %w", err)
	}

	// Look up the bucket (schema) for this ledger from _system.ledgers.
	var bucket string

	err = pool.QueryRow(ctx,
		`SELECT bucket FROM _system.ledgers WHERE name = $1`,
		ledgerName,
	).Scan(&bucket)
	if err != nil {
		pool.Close()

		return nil, fmt.Errorf("looking up bucket for ledger %q: %w", ledgerName, err)
	}

	return &PostgresSource{
		pool:       pool,
		ledgerName: ledgerName,
		bucket:     bucket,
	}, nil
}

// FetchLogs reads logs from the v2 PostgreSQL log table.
// afterID is the last known log ID (0 to start from the beginning).
// Returns logs (oldest first), whether there are more, and any error.
func (s *PostgresSource) FetchLogs(ctx context.Context, afterID uint64, pageSize int) ([]V2Log, bool, error) {
	// Fetch pageSize+1 rows to determine if there are more.
	// COALESCE on hash: v2 ledgers with LEDGER_FEATURE_HASH_LOGS disabled leave
	// logs.hash NULL, which would fail the scan into V2Log.Hash (string).
	query := postgresFetchLogsQuery(s.bucket)

	rows, err := s.pool.Query(ctx, query, s.ledgerName, afterID, pageSize+1)
	if err != nil {
		return nil, false, fmt.Errorf("querying logs: %w", err)
	}
	defer rows.Close()

	var logs []V2Log

	for rows.Next() {
		var (
			l       V2Log
			logDate time.Time
			data    []byte
		)

		err := rows.Scan(&l.ID, &l.Type, &logDate, &data, &l.Hash)
		if err != nil {
			return nil, false, fmt.Errorf("scanning log row: %w", err)
		}

		// Keep PostgreSQL DateStyle out of the mirror protocol: pgx decodes the
		// typed timestamp, then the adapter emits the same RFC3339 shape as the
		// HTTP v2 API.
		l.Date = formatPostgresLogDate(logDate)
		l.Data = json.RawMessage(data)
		logs = append(logs, l)
	}

	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterating log rows: %w", err)
	}

	hasMore := len(logs) > pageSize
	if hasMore {
		logs = logs[:pageSize]
	}

	return logs, hasMore, nil
}

func postgresFetchLogsQuery(bucket string) string {
	return fmt.Sprintf(
		`SELECT id, type, date, data, COALESCE(encode(hash, 'hex'), '') FROM %q.logs WHERE ledger = $1 AND id > $2 ORDER BY id ASC LIMIT $3`,
		bucket,
	)
}

func formatPostgresLogDate(date time.Time) string {
	return date.UTC().Format(time.RFC3339Nano)
}

// GetLatestLogID returns the highest log ID from the v2 PostgreSQL log table.
// Returns 0 if the table is empty.
func (s *PostgresSource) GetLatestLogID(ctx context.Context) (uint64, error) {
	query := fmt.Sprintf(
		`SELECT COALESCE(MAX(id), 0) FROM %q.logs WHERE ledger = $1`,
		s.bucket,
	)

	var maxID uint64

	err := s.pool.QueryRow(ctx, query, s.ledgerName).Scan(&maxID)
	if err != nil {
		return 0, fmt.Errorf("querying latest log ID: %w", err)
	}

	return maxID, nil
}

// Close closes the underlying connection pool.
func (s *PostgresSource) Close() error {
	s.pool.Close()

	return nil
}

// buildPgxPoolConfig parses the source DSN and, when AWS IAM auth is enabled,
// wires a BeforeConnect hook that refreshes a fresh RDS IAM token per new
// pool connection (token TTL is 15 minutes; pgxpool fires BeforeConnect on
// every new connection it opens, so rotation is automatic).
func buildPgxPoolConfig(ctx context.Context, cfg *commonpb.PostgresMirrorSourceConfig) (*pgxpool.Config, error) {
	poolCfg, err := pgxpool.ParseConfig(connectionconfig.RenderDatabase(cfg.GetConnection()))
	if err != nil {
		return nil, fmt.Errorf("parsing mirror DSN: %w", err)
	}

	iam := cfg.GetAwsIamAuth()
	if iam == nil {
		return poolCfg, nil
	}

	region := iam.GetRegion()
	if region == "" {
		return nil, errors.New("aws_iam_auth.region is required when AWS IAM auth is enabled")
	}

	// The committed structured settings must require TLS independently of
	// environment defaults. Retain the actual driver fallback check as well.
	if !connectionconfig.PostgresEnforcesTLS(cfg.GetConnection()) || !poolConfigEnforcesTLS(poolCfg) {
		return nil, errors.New("aws_iam_auth requires explicit TLS on every PostgreSQL endpoint")
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("loading AWS config for IAM auth: %w", err)
	}

	if roleArn := iam.GetAssumeRoleArn(); roleArn != "" {
		// Wrap the ambient credentials with an STS AssumeRole provider so the
		// RDS token is signed with the assumed-role's identity instead of the
		// pod's base identity. NewCredentialsCache memoizes the STS response
		// until ~5 minutes before expiry; each fresh AssumeRole costs one STS
		// call, not one per pgxpool BeforeConnect.
		stsClient := sts.NewFromConfig(awsCfg)
		awsCfg.Credentials = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(stsClient, roleArn))
	}

	poolCfg.BeforeConnect = iamBeforeConnect(awsCfg)

	return poolCfg, nil
}

// iamBeforeConnectTimeout bounds the credential-resolution + SigV4-signing call
// inside iamBeforeConnect. A hung STS/IMDS endpoint would otherwise stall every
// fresh pgxpool connection setup for the full pool dial deadline. 5s matches
// the AWS SDK's default IMDS retry budget.
const iamBeforeConnectTimeout = 5 * time.Second

// iamBeforeConnect returns a pgxpool BeforeConnect hook that mints a fresh
// RDS IAM auth token for each new connection and writes it to ConnConfig.Password.
// The SigV4 token is short-lived (15 min); pgxpool fires BeforeConnect on every
// new connection it opens, so rotation is automatic.
func iamBeforeConnect(awsCfg aws.Config) func(context.Context, *pgx.ConnConfig) error {
	return func(ctx context.Context, cc *pgx.ConnConfig) error {
		ctx, span := mirrorTracer.Start(ctx, "iam.build-auth-token")
		defer span.End()

		ctx, cancel := context.WithTimeout(ctx, iamBeforeConnectTimeout)
		defer cancel()

		endpoint := fmt.Sprintf("%s:%d", cc.Host, cc.Port)

		// pgx's connect tracer records this error on its own span when we
		// return it, so we don't double-attribute via otlp.RecordError here.
		token, err := auth.BuildAuthToken(ctx, endpoint, awsCfg.Region, cc.User, awsCfg.Credentials)
		if err != nil {
			return fmt.Errorf("building aws auth token: %w", err)
		}

		cc.Password = token

		return nil
	}
}

// poolConfigEnforcesTLS reports whether the parsed pgxpool config forces TLS
// on every connection attempt (i.e. matches libpq sslmode in {require,
// verify-ca, verify-full}). It inspects the actual pgx config -- the same
// representation that drives connect attempts at runtime -- so adversarial
// DSN strings (e.g. quoted libpq keyword=value values containing fake
// sslmode= substrings) cannot fool a string-level parser.
//
// pgx encodes sslmode as: primary ConnConfig.TLSConfig + Fallbacks. A non-nil
// TLSConfig on every entry means TLS is the only attempt path. disable/allow
// produce a nil TLSConfig somewhere; prefer puts a nil-TLS fallback after a
// TLS primary.
func poolConfigEnforcesTLS(cfg *pgxpool.Config) bool {
	if cfg == nil || cfg.ConnConfig == nil || cfg.ConnConfig.TLSConfig == nil {
		return false
	}

	for _, fb := range cfg.ConnConfig.Fallbacks {
		if fb == nil || fb.TLSConfig == nil {
			return false
		}
	}

	return true
}
