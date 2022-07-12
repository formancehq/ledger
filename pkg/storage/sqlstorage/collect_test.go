package sqlstorage

import (
	"context"
	"database/sql"
	"testing"

	"github.com/psanford/memfs"
	"github.com/stretchr/testify/require"
)

func TestCollectMigrations(t *testing.T) {

	mfs := memfs.New()
	require.NoError(t, mfs.MkdirAll("migrates/0-first-migration", 0666))
	require.NoError(t, mfs.WriteFile("migrates/0-first-migration/postgres.sql", []byte(`
		--statement
		NO SQL;
	`), 0666))
	require.NoError(t, mfs.WriteFile("migrates/0-first-migration/sqlite.go", []byte{}, 0666))
	require.NoError(t, mfs.MkdirAll("migrates/1-second-migration", 0666))
	require.NoError(t, mfs.WriteFile("migrates/1-second-migration/any.sql", []byte(`
		--statement
		NO SQL;
	`), 0666))

	RegisterGoMigrationFromFilename("migrates/0-first-migration/sqlite.go", func(ctx context.Context, schema Schema, tx *sql.Tx) error {
		return nil
	})

	migrations, err := CollectMigrationFiles(mfs)
	require.NoError(t, err)
	require.Len(t, migrations, 2)

	require.Equal(t, "0", migrations[0].Number)
	require.Equal(t, "first-migration", migrations[0].Name)
	require.Len(t, migrations[0].Handlers, 2)
	require.Len(t, migrations[0].Handlers["sqlite"], 1)
	require.Len(t, migrations[0].Handlers["postgres"], 1)

	require.Equal(t, "1", migrations[1].Number)
	require.Equal(t, "second-migration", migrations[1].Name)
	require.Len(t, migrations[1].Handlers, 1)
	require.Len(t, migrations[1].Handlers["any"], 1)
}
