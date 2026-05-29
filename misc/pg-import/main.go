package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	grpcinsecure "google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	var (
		pgDSN        = flag.String("dsn", "", "PostgreSQL DSN to the v2 instance (e.g. postgres://user:pass@host:5432/postgres)")
		v3Server     = flag.String("server", "localhost:8888", "Ledger v3 gRPC server address")
		batchSize    = flag.Uint("batch-size", 0, "Mirror batch size (0 = server default)")
		dryRun       = flag.Bool("dry-run", false, "Only list discovered ledgers, don't create mirrors")
		timeout      = flag.Duration("timeout", 30*time.Second, "Timeout per gRPC call")
		insecureMode = flag.Bool("insecure", false, "Use plaintext gRPC (no TLS)")
		authToken    = flag.String("token", "", "Bearer token for gRPC authentication")
	)

	flag.Parse()

	if *pgDSN == "" {
		fmt.Fprintln(os.Stderr, "error: --dsn is required")
		flag.Usage()

		return errors.New("--dsn is required")
	}

	ctx := context.Background()

	// Step 1: Discover v2 ledgers.
	log.Println("Scanning PostgreSQL instance for v2 databases...")

	ledgers, err := discoverV2Ledgers(ctx, *pgDSN)
	if err != nil {
		return fmt.Errorf("discovery failed: %w", err)
	}

	log.Printf("Found %d ledger(s)\n", len(ledgers))

	if len(ledgers) == 0 {
		return nil
	}

	// Print table.
	fmt.Printf("\n%-30s %-30s %-20s\n", "DATABASE", "LEDGER", "BUCKET")
	fmt.Println(strings.Repeat("-", 80))

	for _, l := range ledgers {
		fmt.Printf("%-30s %-30s %-20s\n", l.database, l.name, l.bucket)
	}

	fmt.Println()

	if *dryRun {
		log.Println("Dry run - exiting.")

		return nil
	}

	// Step 2: Connect to v3.
	var creds credentials.TransportCredentials
	if *insecureMode {
		creds = grpcinsecure.NewCredentials()
	} else {
		creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}

	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	if *authToken != "" {
		dialOpts = append(dialOpts,
			grpc.WithUnaryInterceptor(bearerInterceptor(*authToken)),
			grpc.WithStreamInterceptor(bearerStreamInterceptor(*authToken)),
		)
	}

	conn, err := grpc.NewClient(*v3Server, dialOpts...)
	if err != nil {
		return fmt.Errorf("failed to connect to v3 server: %w", err)
	}

	defer func() { _ = conn.Close() }()

	client := servicepb.NewBucketServiceClient(conn)

	// Fetch existing ledgers to skip duplicates.
	existing := listExistingLedgers(ctx, client, *timeout)

	var created, skipped, failed int

	for _, l := range ledgers {
		mirrorName := buildMirrorName(l.database, l.name)

		if _, ok := existing[mirrorName]; ok {
			log.Printf("  SKIP  %s (already exists)", mirrorName)

			skipped++

			continue
		}

		dbDSN := replaceDSNDatabase(*pgDSN, l.database)

		err := createMirrorLedger(ctx, client, mirrorName, l.name, dbDSN, uint32(*batchSize), *timeout)
		if err != nil {
			log.Printf("  FAIL  %s: %v", mirrorName, err)

			failed++

			continue
		}

		log.Printf("  OK    %s", mirrorName)

		created++
	}

	log.Printf("\nDone: created=%d skipped=%d failed=%d", created, skipped, failed)

	if failed > 0 {
		return fmt.Errorf("%d mirror(s) failed to create", failed)
	}

	return nil
}

// v2Ledger holds info about a ledger discovered from a v2 PostgreSQL database.
type v2Ledger struct {
	database string
	name     string
	bucket   string
}

// discoverV2Ledgers scans all databases on the PG instance and returns all v2 ledgers found.
func discoverV2Ledgers(ctx context.Context, dsn string) ([]v2Ledger, error) {
	conn, err := pgx.Connect(ctx, sanitizeDSN(dsn))
	if err != nil {
		return nil, fmt.Errorf("connecting to instance: %w", err)
	}

	rows, err := conn.Query(ctx,
		`SELECT datname FROM pg_database WHERE datistemplate = false AND datname != 'postgres' ORDER BY datname`,
	)
	if err != nil {
		_ = conn.Close(ctx)

		return nil, fmt.Errorf("listing databases: %w", err)
	}

	var databases []string

	for rows.Next() {
		var name string

		err := rows.Scan(&name)
		if err != nil {
			rows.Close()
			_ = conn.Close(ctx)

			return nil, fmt.Errorf("scanning database: %w", err)
		}

		databases = append(databases, name)
	}

	rows.Close()
	_ = conn.Close(ctx)

	var ledgers []v2Ledger

	for _, db := range databases {
		if !strings.Contains(db, "-") {
			continue
		}

		dbDSN := replaceDSNDatabase(dsn, db)

		found, err := discoverLedgersInDB(ctx, dbDSN, db)
		if err != nil {
			log.Printf("  WARN  %s: %v (skipping)", db, err)

			continue
		}

		ledgers = append(ledgers, found...)
	}

	return ledgers, nil
}

// discoverLedgersInDB connects to a single database and checks for _system.ledgers.
func discoverLedgersInDB(ctx context.Context, dsn, database string) ([]v2Ledger, error) {
	conn, err := pgx.Connect(ctx, sanitizeDSN(dsn))
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	defer func() { _ = conn.Close(ctx) }()

	var exists bool

	err = conn.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = '_system' AND table_name = 'ledgers'
		)`,
	).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("check _system.ledgers: %w", err)
	}

	if !exists {
		return nil, nil
	}

	// Check whether the "name" column exists — older v2 versions don't have it.
	var hasNameCol bool

	_ = conn.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = '_system' AND table_name = 'ledgers' AND column_name = 'name'
		)`,
	).Scan(&hasNameCol)

	if !hasNameCol {
		return nil, nil
	}

	rows, err := conn.Query(ctx, `SELECT name, bucket FROM _system.ledgers ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("query ledgers: %w", err)
	}
	defer rows.Close()

	var ledgers []v2Ledger

	for rows.Next() {
		var l v2Ledger

		err := rows.Scan(&l.name, &l.bucket)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		l.database = database
		ledgers = append(ledgers, l)
	}

	return ledgers, rows.Err()
}

// listExistingLedgers fetches ledgers already present on the v3 instance.
func listExistingLedgers(ctx context.Context, client servicepb.BucketServiceClient, timeout time.Duration) map[string]struct{} {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	stream, err := client.ListLedgers(ctx, &servicepb.ListLedgersRequest{})
	if err != nil {
		log.Printf("  WARN  could not list existing v3 ledgers: %v", err)

		return nil
	}

	existing := make(map[string]struct{})

	for {
		info, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			log.Printf("  WARN  error listing v3 ledgers: %v", err)

			break
		}

		existing[info.GetName()] = struct{}{}
	}

	return existing
}

// createMirrorLedger creates a mirror ledger on v3 pointing to the v2 PG database.
func createMirrorLedger(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	name, sourceLedgerName, dsn string,
	batchSize uint32,
	timeout time.Duration,
) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{
			{
				Type: &servicepb.Request_CreateLedger{
					CreateLedger: &servicepb.CreateLedgerRequest{
						Name: name,
						Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
						MirrorSource: &commonpb.MirrorSourceConfig{
							LedgerName: sourceLedgerName,
							BatchSize:  batchSize,
							Type: &commonpb.MirrorSourceConfig_Postgres{
								Postgres: &commonpb.PostgresMirrorSourceConfig{
									Dsn: dsn,
								},
							},
						},
					},
				},
			},
		},
	})

	return err
}

// buildMirrorName constructs the v3 mirror ledger name as "database-ledger".
func buildMirrorName(database, ledgerName string) string {
	return database + "-" + ledgerName
}

// sanitizeDSN URL-encodes the user and password in a postgres:// DSN so that
// special characters (e.g. ), #, () don't break url.Parse which pgx uses
// internally. Keyword-format DSNs are returned unchanged.
func sanitizeDSN(dsn string) string {
	schemeEnd := strings.Index(dsn, "://")
	if schemeEnd == -1 {
		return dsn // keyword format, no encoding needed
	}

	rest := dsn[schemeEnd+3:]

	atIdx := strings.LastIndex(rest, "@")
	if atIdx == -1 {
		return dsn // no userinfo
	}

	userinfo := rest[:atIdx]

	before, after, ok := strings.Cut(userinfo, ":")
	if !ok {
		// User only, no password.
		return dsn[:schemeEnd+3] + url.PathEscape(userinfo) + rest[atIdx:]
	}

	user := before
	pass := after
	encoded := url.PathEscape(user) + ":" + url.PathEscape(pass)

	return dsn[:schemeEnd+3] + encoded + rest[atIdx:]
}

// replaceDSNDatabase returns a copy of the DSN pointing to a different database.
func replaceDSNDatabase(dsn, database string) string {
	schemeEnd := strings.Index(dsn, "://")
	if schemeEnd == -1 {
		// Keyword format.
		if strings.Contains(dsn, "dbname=") {
			parts := strings.Fields(dsn)
			for i, p := range parts {
				if strings.HasPrefix(p, "dbname=") {
					parts[i] = "dbname=" + database
				}
			}

			return strings.Join(parts, " ")
		}

		return dsn + " dbname=" + database
	}

	// URL format: postgres://user:pass@host:port/database?params
	rest := dsn[schemeEnd+3:]

	atIdx := strings.LastIndex(rest, "@")

	var hostAndPath string
	if atIdx >= 0 {
		hostAndPath = rest[atIdx+1:]
	} else {
		hostAndPath = rest
	}

	slashIdx := strings.Index(hostAndPath, "/")
	if slashIdx == -1 {
		return dsn + "/" + database
	}

	queryIdx := strings.Index(hostAndPath[slashIdx:], "?")

	var query string
	if queryIdx >= 0 {
		query = hostAndPath[slashIdx+queryIdx:]
	}

	prefixLen := schemeEnd + 3
	if atIdx >= 0 {
		prefixLen += atIdx + 1
	}

	prefixLen += slashIdx

	return dsn[:prefixLen] + "/" + database + query
}

// bearerInterceptor returns a unary interceptor that injects an Authorization header.
func bearerInterceptor(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// bearerStreamInterceptor returns a stream interceptor that injects an Authorization header.
func bearerStreamInterceptor(token string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

		return streamer(ctx, desc, cc, method, opts...)
	}
}
