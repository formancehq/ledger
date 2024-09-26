//go:build it

package system

import (
	"testing"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/platform/postgres"
	"github.com/stretchr/testify/require"
)

func TestConfiguration(t *testing.T) {
	t.Parallel()

	systemStore := newSystemStore(t)
	ctx := logging.TestingContext()

	require.NoError(t, systemStore.InsertConfiguration(ctx, "foo", "bar"))
	bar, err := systemStore.GetConfiguration(ctx, "foo")
	require.NoError(t, err)
	require.Equal(t, "bar", bar)
}

func TestConfigurationError(t *testing.T) {
	t.Parallel()

	systemStore := newSystemStore(t)
	ctx := logging.TestingContext()

	_, err := systemStore.GetConfiguration(ctx, "not_existing")
	require.Error(t, err)
	require.True(t, postgres.IsNotFoundError(err))
}
