package bucket

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"text/template"
	"time"

	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/go-libs/v5/pkg/storage/migrations"
)

// Compatible corrections must preserve the SQL and data contract of every binary
// supported at the current main schema version. Older binaries ignore this
// additive registry; incompatible changes must use the main migration stream.
// Keep applied corrections immutable and append new ones in execution order.
const compatibleMigrationsTable = "compatible_migrations"

//go:embed compatible_migrations/0-transaction-metadata-history.sql
var transactionMetadataHistorySQL string

func runCompatibleMigrations(ctx context.Context, db bun.IDB, schema string, tracer trace.Tracer) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// Use transaction-scoped advisory locks, including when called outside a
	// transaction. A cancellation rolls back both the repair and its checkpoint
	// and cannot leave a session lock on a pooled connection.
	return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		return applyCompatibleMigrations(ctx, tx, schema, tracer)
	})
}

func applyCompatibleMigrations(ctx context.Context, db bun.Tx, schema string, tracer trace.Tracer) error {
	var sql bytes.Buffer
	if err := template.Must(template.New("transaction-metadata-history").Parse(transactionMetadataHistorySQL)).
		Execute(&sql, map[string]string{"Schema": schema}); err != nil {
		return fmt.Errorf("templating compatible migration: %w", err)
	}

	// The existing runner owns locking, durable progress and retries. Its lock is
	// scoped to the qualified versions table, including inside CreateLedger's tx.
	migrator := migrations.NewMigrator(db,
		migrations.WithSchema(schema),
		migrations.WithTableName(compatibleMigrationsTable),
		migrations.WithTracer(tracer),
	)
	migrator.RegisterMigrations(migrations.Migration{
		Name: "Avoid transaction metadata revisions when metadata is unchanged",
		Up: func(ctx context.Context, db bun.IDB) error {
			_, err := db.ExecContext(ctx, sql.String())
			return err
		},
	})
	return runMigrator(ctx, migrator)
}
