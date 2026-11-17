//go:build it

package restore_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/testing/docker"
	"github.com/formancehq/go-libs/v5/pkg/testing/platform/pgtesting"

	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/driver"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
)

// Every test uses a new database on this suite's disposable PostgreSQL server.
// Use the production migrations and driver, including all feature triggers.
type fixture struct {
	ctx    context.Context
	db     *bun.DB
	driver *driver.Driver
	system *systemcontroller.DefaultController
}

func newFixture(t *testing.T, server *pgtesting.PostgresServer) *fixture {
	t.Helper()
	database := server.NewDatabase(t)
	db, err := sql.Open("pgx", database.ConnString())
	require.NoError(t, err)
	bdb := bun.NewDB(db, pgdialect.New(), bun.WithDiscardUnknownColumns())
	t.Cleanup(func() { require.NoError(t, bdb.Close()) })
	ctx, cancel := context.WithTimeout(logging.ContextWithLogger(t.Context(), logging.Testing()), 20*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, systemstore.Migrate(ctx, bdb))
	d := driver.New(bdb, ledgerstore.NewFactory(bdb), bucket.NewDefaultFactory(), systemstore.NewStoreFactory())
	s := systemcontroller.NewDefaultController(systemcontroller.NewControllerStorageDriverAdapter(d, systemstore.New(bdb)), nil, nil)
	return &fixture{ctx: ctx, db: bdb, driver: d, system: s}
}

func newServer(t *testing.T) *pgtesting.PostgresServer {
	return pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()), pgtesting.WithExtension("pgcrypto"))
}

func (f *fixture) ledger(t *testing.T) (ledgercontroller.Controller, *ledger.Ledger) {
	t.Helper()
	cfg := ledger.NewDefaultConfiguration()
	cfg.Bucket = "_default"
	name := "sec79_" + uuid.NewString()[:8]
	l, err := ledger.New(name, cfg)
	require.NoError(t, err)
	_, err = f.driver.CreateLedger(f.ctx, l)
	require.NoError(t, err)
	c, err := f.system.GetLedgerController(f.ctx, name)
	require.NoError(t, err)
	return c, l
}

func (f *fixture) create(t *testing.T, c ledgercontroller.Controller, script string) *ledger.Log {
	t.Helper()
	log, _, _, err := c.CreateTransaction(f.ctx, transactionParameters(script))
	require.NoError(t, err)
	return log
}

func (f *fixture) export(t *testing.T, c ledgercontroller.Controller) []ledger.Log {
	t.Helper()
	logs := []ledger.Log{}
	require.NoError(t, c.Export(f.ctx, ledgercontroller.ExportWriterFn(func(_ context.Context, l ledger.Log) error { logs = append(logs, l); return nil })))
	return logs
}

func (f *fixture) restore(c ledgercontroller.Controller, logs []ledger.Log) error {
	stream := make(chan ledger.Log, len(logs))
	for _, log := range logs {
		stream <- log
	}
	close(stream)
	return c.Import(f.ctx, stream)
}

func (f *fixture) count(t *testing.T, l *ledger.Ledger, relation string) int {
	t.Helper()
	var count int
	require.NoError(t, f.db.NewSelect().TableExpr("?.?", bun.Ident(l.Bucket), bun.Ident(relation)).ColumnExpr("count(*)").Where("ledger = ?", l.Name).Scan(f.ctx, &count))
	return count
}

func transactionParameters(script string) ledgercontroller.Parameters[ledgercontroller.CreateTransaction] {
	return ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
		Input: ledgercontroller.CreateTransaction{
			RunScript: ledgercontroller.RunScript{Script: ledgercontroller.Script{Plain: script}},
		},
	}
}
