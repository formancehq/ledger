package v2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

var mirrorTracer = otel.Tracer("mirror.v2.postgres")

// PostgresSource is a Source that reads logs directly from a v2 ledger's
// PostgreSQL database. In v2, the _system.ledgers table maps each ledger
// to a bucket (PostgreSQL schema). The logs table lives in that schema
// and uses a ledger column to distinguish between ledgers sharing a bucket.
// Live ingestion requires a writable primary, SHARE-lock privileges, and the
// upstream CACHE 1 sequence. Concurrent explicit-ID imports are unsupported.
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

	source := &PostgresSource{
		pool:       pool,
		ledgerName: ledgerName,
		bucket:     bucket,
	}
	if err := source.validateSource(ctx); err != nil {
		pool.Close()

		return nil, err
	}

	return source, nil
}

// validateSource checks prerequisites before any source rows can be consumed.
// Permissions are checked without acquiring a lock, so startup does not queue
// behind or interrupt live source writers.
func (s *PostgresSource) validateSource(ctx context.Context) error {
	var primary, canSelect, canLock, sequenceOK bool
	err := s.pool.QueryRow(ctx, `SELECT
NOT pg_is_in_recovery() AND current_setting('transaction_read_only') = 'off',
has_table_privilege($1, 'SELECT'),
has_table_privilege($1, 'UPDATE,DELETE,TRUNCATE'),
EXISTS (SELECT 1 FROM pg_catalog.pg_sequences seq JOIN _system.ledgers l
 ON seq.schemaname = l.bucket AND seq.sequencename = 'log_id_' || l.id::text
 WHERE l.name = $2 AND seq.increment_by = 1 AND seq.cache_size = 1 AND NOT seq.cycle)`,
		pgx.Identifier{s.bucket, "logs"}.Sanitize(), s.ledgerName,
	).Scan(&primary, &canSelect, &canLock, &sequenceOK)
	if err != nil {
		return fmt.Errorf("checking PostgreSQL mirror source requirements: %w", err)
	}
	if !primary {
		return errors.New("PostgreSQL mirror requires a writable PostgreSQL primary")
	}
	if !canSelect || !canLock {
		return errors.New("PostgreSQL mirror requires SELECT and SHARE lock privilege on the source logs table (ownership or UPDATE, DELETE or TRUNCATE)")
	}
	if !sequenceOK {
		return errors.New("PostgreSQL mirror requires the upstream log sequence with increment 1, CACHE 1 and NO CYCLE")
	}

	return nil
}

// FetchLogs reads logs from the v2 PostgreSQL log table.
// afterID is the last known log ID (0 to start from the beginning).
// Returns logs (oldest first), whether there are more, and any error.
func (s *PostgresSource) FetchLogs(ctx context.Context, afterID uint64, pageSize int) ([]V2Log, bool, error) {
	logs, more, err := s.fetchLogs(ctx, s.pool.Query, afterID, pageSize)
	if err != nil {
		return nil, false, err
	}
	previous := afterID
	for _, log := range logs {
		if log.ID <= previous || log.ID-previous != 1 {
			// A visible higher ID does not prove the missing INSERT committed or
			// rolled back. Discard this page before translation can fill its gap.
			return s.fetchStableLogs(ctx, afterID, pageSize)
		}
		previous = log.ID
	}

	return logs, more, nil
}

func (s *PostgresSource) fetchStableLogs(ctx context.Context, afterID uint64, pageSize int) (logs []V2Log, more bool, err error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite})
	if err != nil {
		return nil, false, fmt.Errorf("starting synchronized source read: %w", err)
	}
	defer func() {
		// Cleanup must work even when the caller cancelled its fetch. A failed
		// rollback closes the pgx connection rather than returning a live
		// transaction to the pool.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if rollbackErr := tx.Rollback(cleanupCtx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("rolling back synchronized source read: %w", rollbackErr))
		}
	}()

	// NOWAIT avoids queuing behind active inserts and thereby blocking new
	// writers. On conflict, return no rows and let the worker retry later.
	// READ COMMITTED gives the following SELECT a fresh snapshot after the
	// lock proves there are no outstanding writers on the bucket logs table.
	if _, err := tx.Exec(ctx, "LOCK TABLE "+pgx.Identifier{s.bucket, "logs"}.Sanitize()+" IN SHARE MODE NOWAIT"); err != nil {
		return nil, false, fmt.Errorf("synchronizing source log gap: %w", err)
	}
	logs, more, err = s.fetchLogs(ctx, tx.Query, afterID, pageSize)
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("completing synchronized source read: %w", err)
	}

	// No source lock survives into translation, preloading or Raft apply.
	return logs, more, nil
}

func (s *PostgresSource) fetchLogs(ctx context.Context, queryRows func(context.Context, string, ...any) (pgx.Rows, error), afterID uint64, pageSize int) ([]V2Log, bool, error) {
	// Fetch pageSize+1 rows to determine if there are more.
	// COALESCE on hash: v2 ledgers with LEDGER_FEATURE_HASH_LOGS disabled leave
	// logs.hash NULL, which would fail the scan into V2Log.Hash (string).
	query := postgresFetchLogsQuery(s.bucket)

	rows, err := queryRows(ctx, query, s.ledgerName, afterID, pageSize+1)
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
		`SELECT id, type, date, data, COALESCE(encode(hash, 'hex'), '') FROM %s WHERE ledger = $1 AND id > $2 ORDER BY id ASC LIMIT $3`,
		pgx.Identifier{bucket, "logs"}.Sanitize(),
	)
}

func formatPostgresLogDate(date time.Time) string {
	return date.UTC().Format(time.RFC3339Nano)
}

// GetLatestLogID returns the highest log ID from the v2 PostgreSQL log table.
// Returns 0 if the table is empty.
func (s *PostgresSource) GetLatestLogID(ctx context.Context) (uint64, error) {
	query := fmt.Sprintf(
		`SELECT COALESCE(MAX(id), 0) FROM %s WHERE ledger = $1`,
		pgx.Identifier{s.bucket, "logs"}.Sanitize(),
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
	poolCfg, err := pgxpool.ParseConfig(cfg.GetDsn())
	if err != nil {
		// pgx errors retain the DSN and may expose query credentials or invalid
		// option values. Return only static diagnostics, without the cause.
		if parseErr, ok := errors.AsType[*pgconn.ParseConfigError](err); ok {
			diagnostic := *parseErr
			diagnostic.ConnString = ""
			if strings.HasPrefix(diagnostic.Error(), "cannot parse ``: invalid connect_timeout (") {
				return nil, errors.New("parsing mirror DSN: invalid connect_timeout")
			}
		}

		return nil, errors.New("parsing mirror DSN: invalid PostgreSQL configuration")
	}

	iam := cfg.GetAwsIamAuth()
	if iam == nil {
		return poolCfg, nil
	}

	region := iam.GetRegion()
	if region == "" {
		return nil, errors.New("aws_iam_auth.region is required when AWS IAM auth is enabled")
	}

	// Refuse to install the IAM hook on a non-TLS sslmode: the SigV4 token
	// written to ConnConfig.Password is a 15-minute bearer credential, and
	// sslmode in {disable, allow, prefer} would let it travel in cleartext.
	//
	// Three-layer gate:
	//   1. `dsnIsURIForm` rejects libpq keyword=value DSNs. IAM only accepts
	//      the postgres:// URI form; that lets us parse via net/url and
	//      removes the need for a hand-rolled libpq tokenizer to defend
	//      against tricks like quoted values embedding fake sslmode=
	//      substrings.
	//   2. `dsnHasExplicitSSLMode` requires sslmode= to be present in the
	//      raw DSN. pgxpool.ParseConfig folds ambient libpq env vars
	//      (PGSSLMODE, ...) into its effective config, so a persisted
	//      mirror without sslmode would inherit PGSSLMODE from the pod env
	//      at admission time and later fail (or downgrade to cleartext) on
	//      any pod that lacks that env var. Anchoring the check in the raw
	//      DSN makes the TLS guarantee a property of the stored config
	//      alone, independent of pod environment.
	//   3. `poolConfigEnforcesTLS` inspects the parsed pgx config —
	//      the representation actually used at connect time — so an
	//      explicitly set sslmode of disable/allow/prefer is still
	//      rejected.
	if !dsnIsURIForm(cfg.GetDsn()) {
		return nil, errors.New("aws_iam_auth requires a URI-form DSN (postgres:// or postgresql://)")
	}

	if !dsnHasExplicitSSLMode(cfg.GetDsn()) || !poolConfigEnforcesTLS(poolCfg) {
		return nil, errors.New("aws_iam_auth requires an explicit sslmode in {require, verify-ca, verify-full} in the DSN")
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

// dsnHasExplicitSSLMode reports whether a URI-form DSN carries an explicit
// sslmode query parameter. Returns false for non-URI (libpq keyword=value)
// DSNs — with IAM auth we require URI form up-front (see the guard in
// buildPgxPoolConfig) so this helper never sees anything else.
//
// Reason for looking at the raw DSN rather than the parsed pgxpool config:
// pgxpool.ParseConfig folds ambient libpq env vars (PGSSLMODE, ...) into
// its effective config. A persisted mirror without sslmode= would inherit
// e.g. PGSSLMODE=require from the pod env at admission time, then fail
// (or downgrade to cleartext) on any pod that lacks the env var. Anchoring
// the TLS gate in the raw DSN makes it a property of the stored config
// alone, independent of pod environment.
func dsnHasExplicitSSLMode(dsn string) bool {
	if !dsnIsURIForm(dsn) {
		return false
	}

	u, err := url.Parse(dsn)
	if err != nil {
		return false
	}

	return u.Query().Has("sslmode")
}

// dsnIsURIForm reports whether the DSN uses the libpq URI form
// (postgres:// or postgresql://). Non-URI (keyword=value) DSNs are
// rejected up-front when IAM auth is enabled: enforcing URI form removes
// the need for a hand-rolled libpq tokenizer to defend against tricks
// like quoted values embedding fake sslmode= substrings.
func dsnIsURIForm(dsn string) bool {
	return strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://")
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
