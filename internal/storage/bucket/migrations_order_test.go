package bucket_test

import (
	"io/fs"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/types/pointer"

	"github.com/formancehq/ledger/internal/storage/bucket"
)

// Migrations run by position, so the released order must never change.
func TestWalkMigrationsKeepsReleasedOrder(t *testing.T) {
	t.Parallel()

	names, err := bucket.WalkMigrations(bucket.MigrationsFS, func(entry fs.DirEntry) (*string, error) {
		return pointer.For(entry.Name()), nil
	})
	require.NoError(t, err)

	start := slices.Index(names, "50-add-transactions-id-desc-index")
	require.NotEqual(t, -1, start)
	require.Equal(t, 50, start)
	require.Equal(t, []string{
		"50-add-transactions-id-desc-index",
		"51-add-transactions-sources-array-index",
		"51-fix-logs-blocks-pkey-collision",
		"52-add-transactions-destinations-array-index",
		"53-ensure-logs-blocks-composite-pkey",
	}, names[start:])
}
