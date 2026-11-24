//go:build it

package driver_test

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"
	stdtime "time"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"golang.org/x/sync/errgroup"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/connect"
	"github.com/formancehq/go-libs/v5/pkg/storage/migrations"
	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/driver"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/formancehq/ledger/pkg/features"
)

func TestTransactionMetadataHistoryCompatibleUpgrade(t *testing.T) {
	t.Parallel()
	ctx := logging.ContextWithLogger(t.Context(), logging.Testing())
	pgDatabase := srv.NewDatabase(t)
	db, err := connect.OpenSQLDB(ctx, pgDatabase.ConnectionOptions())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, systemstore.Migrate(ctx, db))
	const schema = "compatible_history"

	// Reproduce the released version-56 registry, before compatible repairs exist.
	mainMigrator := bucket.GetMigrator(db, schema)
	for range 56 {
		require.NoError(t, mainMigrator.UpByOne(ctx))
	}
	require.ErrorIs(t, mainMigrator.UpByOne(ctx), migrations.ErrAlreadyUpToDate)
	d := driver.New(db, ledgerstore.NewFactory(db), bucket.NewDefaultFactory(), systemstore.NewStoreFactory())
	l, err := ledger.New("legacy", ledger.Configuration{
		Bucket:   schema,
		Features: features.FeatureSet{features.FeatureTransactionMetadataHistory: "SYNC"},
	})
	require.NoError(t, err)
	store, err := d.CreateLedger(ctx, l)
	require.NoError(t, err)

	// The previous AddLedger installed a broad trigger. Keep its original shared
	// function from migration 44, and reproduce that trigger on the legacy ledger.
	installBroadTrigger := func(l *ledger.Ledger) {
		t.Helper()
		trigger := bun.Ident(fmt.Sprintf("update_transaction_metadata_history_%d", l.ID))
		_, err := db.ExecContext(ctx, `drop trigger ? on ?;
			create trigger ? after update on ? for each row when (new.ledger = ?)
			execute function ?()`, trigger, bun.Ident(schema+".transactions"),
			trigger, bun.Ident(schema+".transactions"), l.Name,
			bun.Ident(schema+".update_transaction_metadata_history"))
		require.NoError(t, err)
	}
	installBroadTrigger(l)
	tx := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "alice", "USD", big.NewInt(100))).
		WithMetadata(metadata.Metadata{"order": "123"})
	require.NoError(t, store.InsertTransaction(ctx, &tx))
	_, reverted, err := store.RevertTransaction(ctx, *tx.ID, time.Time{})
	require.NoError(t, err)
	require.True(t, reverted)
	history := func() []metadata.Metadata {
		t.Helper()
		var rows []struct{ Metadata metadata.Metadata }
		err := db.NewSelect().TableExpr(store.GetPrefixedRelationName("transactions_metadata")).
			Column("metadata").Where("ledger = ?", l.Name).Where("transactions_id = ?", *tx.ID).
			OrderExpr("revision asc").Scan(ctx, &rows)
		require.NoError(t, err)
		result := make([]metadata.Metadata, len(rows))
		for i, row := range rows {
			result[i] = row.Metadata
		}
		return result
	}
	before := history()
	require.Equal(t, []metadata.Metadata{{"order": "123"}, {"order": "123"}}, before)

	// An interrupted compatible repair must be retryable without advancing the
	// main version. Roll back the entire first attempt, including its checkpoint.
	errInterrupted := errors.New("interrupt before commit")
	err = db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		require.NoError(t, bucket.NewDefaultFactory().Create(schema).Migrate(ctx, tx))
		return errInterrupted
	})
	require.ErrorIs(t, err, errInterrupted)
	var pending bool
	require.NoError(t, db.NewSelect().ColumnExpr("to_regclass(?) is null", schema+".compatible_migrations").Scan(ctx, &pending))
	require.True(t, pending, "interrupted transaction must roll back the correction registry")

	// A held repair lock must respect the caller's shorter deadline and leave
	// no pooled session lock behind. Releasing it allows the retry below.
	lock, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lock.Rollback() })
	_, err = lock.ExecContext(ctx, "select pg_advisory_xact_lock(hashtext(?))", `"`+schema+`"."compatible_migrations"`)
	require.NoError(t, err)
	if !t.Run("internal deadline without caller deadline", func(t *testing.T) {
		// Exercise the actual production limit. A test watchdog releases the
		// held lock through cleanup if that limit is accidentally removed.
		unbounded := context.WithoutCancel(ctx)
		_, hasDeadline := unbounded.Deadline()
		require.False(t, hasDeadline)
		result := make(chan error, 1)
		started := stdtime.Now()
		go func() { result <- d.UpgradeBucket(unbounded, schema) }()
		select {
		case err := <-result:
			require.Error(t, err)
			require.GreaterOrEqual(t, stdtime.Since(started), 29*stdtime.Second)
			require.Less(t, stdtime.Since(started), 40*stdtime.Second)
		case <-stdtime.After(45 * stdtime.Second):
			t.Fatal("compatible migration exceeded its internal 30-second deadline")
		}
	}) {
		return
	}
	blocked, cancel := context.WithTimeout(ctx, 200*stdtime.Millisecond)
	started := stdtime.Now()
	err = d.UpgradeBucket(blocked, schema)
	require.ErrorIs(t, blocked.Err(), context.DeadlineExceeded)
	cancel()
	require.Error(t, err)
	require.Less(t, stdtime.Since(started), 2*stdtime.Second)

	var lockerPID int
	require.NoError(t, lock.NewSelect().ColumnExpr("pg_backend_pid()").Scan(ctx, &lockerPID))
	var concurrent errgroup.Group
	for range 2 {
		concurrent.Go(func() error { return d.UpgradeBucket(ctx, schema) })
	}
	// Observe both callers queued on the lock held above. Merely launching
	// goroutines could otherwise exercise two sequential, non-overlapping runs.
	require.Eventually(t, func() bool {
		var waiting int
		err := db.NewRaw(`select count(*) from pg_locks blocked
			join pg_locks held using (locktype, database, classid, objid, objsubid)
			where blocked.locktype = 'advisory' and not blocked.granted
			and held.granted and held.pid = ?`, lockerPID).Scan(ctx, &waiting)
		require.NoError(t, err)
		return waiting == 2
	}, 5*stdtime.Second, 10*stdtime.Millisecond)
	require.NoError(t, lock.Rollback())
	require.NoError(t, concurrent.Wait())
	require.NoError(t, d.UpgradeBucket(ctx, schema))
	completed, err := db.NewSelect().TableExpr("?", bun.Ident(schema+".compatible_migrations")).
		Where("is_applied and version_id > 0").Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, completed, "contending upgrades must commit one correction checkpoint")
	require.Equal(t, before, history(), "repair must retain old duplicate revisions")
	version, err := mainMigrator.GetLastVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, 56, version)
	current, err := mainMigrator.IsUpToDate(ctx)
	require.NoError(t, err)
	require.True(t, current, "the released migration registry must still accept the bucket")
	require.NoError(t, d.Initialize(ctx), "the unchanged downgrade check must accept the repaired bucket")

	// A non-metadata UPDATE still invokes the legacy broad trigger.
	_, err = db.ExecContext(ctx, "update ? set updated_at = now() where ledger = ? and id = ?",
		bun.Ident(schema+".transactions"), l.Name, *tx.ID)
	require.NoError(t, err)
	require.Equal(t, before, history())
	_, err = db.ExecContext(ctx, "update ? set metadata = metadata where ledger = ? and id = ?",
		bun.Ident(schema+".transactions"), l.Name, *tx.ID)
	require.NoError(t, err)
	require.Equal(t, before, history(), "identical metadata writes must not append a revision")
	_, changed, err := store.UpdateTransactionMetadata(ctx, *tx.ID, metadata.Metadata{"order": "456"}, time.Time{})
	require.NoError(t, err)
	require.True(t, changed)
	_, changed, err = store.DeleteTransactionMetadata(ctx, *tx.ID, "order", time.Time{})
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, append(before, metadata.Metadata{"order": "456"}, metadata.Metadata{}), history())

	// Old AddLedger is safe too: it installs a broad trigger pointing at the
	// corrected shared function. Its version-56 equality check still succeeds.
	second, err := ledger.New("second", ledger.Configuration{
		Bucket:   schema,
		Features: features.FeatureSet{features.FeatureTransactionMetadataHistory: "SYNC"},
	})
	require.NoError(t, err)
	secondStore, err := d.CreateLedger(ctx, second)
	require.NoError(t, err)
	installBroadTrigger(second)
	secondTx := ledger.NewTransaction().WithPostings(ledger.NewPosting("world", "alice", "USD", big.NewInt(100)))
	require.NoError(t, secondStore.InsertTransaction(ctx, &secondTx))
	_, reverted, err = secondStore.RevertTransaction(ctx, *secondTx.ID, time.Time{})
	require.NoError(t, err)
	require.True(t, reverted)
	count, err := db.NewSelect().TableExpr(secondStore.GetPrefixedRelationName("transactions_metadata")).
		Where("ledger = ? and transactions_id = ?", second.Name, *secondTx.ID).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// This compatible stream must not weaken the main downgrade guard.
	registered, err := bucket.CollectMigrations(bucket.MigrationsFS, schema)
	require.NoError(t, err)
	unknown := migrations.NewMigrator(db, migrations.WithSchema(schema))
	unknown.RegisterMigrations(append(registered, migrations.Migration{
		Name: "unknown future schema",
		Up:   func(context.Context, bun.IDB) error { return nil },
	})...)
	require.NoError(t, unknown.UpByOne(ctx))
	require.Error(t, d.Initialize(ctx), "unknown main versions must still be rejected")
	third, err := ledger.New("third", ledger.Configuration{Bucket: schema})
	require.NoError(t, err)
	_, err = d.CreateLedger(ctx, third)
	require.ErrorIs(t, err, driver.ErrBucketOutdated)
}
