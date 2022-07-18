package sqlstorage

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegister(t *testing.T) {
	fn := func(ctx context.Context, schema Schema, tx *sql.Tx) error {
		return nil
	}
	registeredGoMigrations = make([]Migration, 0)
	defer func() {
		registeredGoMigrations = make([]Migration, 0)
	}()
	RegisterGoMigrationFromFilename(filepath.Join("XXX", "0-init-schema", "any.go"), fn)
	require.Len(t, registeredGoMigrations, 1)
}
