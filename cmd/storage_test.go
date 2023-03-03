package cmd

import (
	"testing"

	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/google/uuid"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func Test_StorageCommands(t *testing.T) {
	db := pgtesting.NewPostgresDatabase(t)

	viper.Set(storagePostgresConnectionStringFlag, db.ConnString())

	require.NoError(t, NewStorageList().Execute())

	viper.Set("name", "")
	require.Error(t, NewStorageInit().Execute())

	name := uuid.NewString()
	viper.Set("name", name)
	require.NoError(t, NewStorageInit().Execute())
	require.NoError(t, NewStorageInit().Execute())

	cmd := NewStorageUpgrade()
	cmd.SetArgs([]string{name})
	require.NoError(t, cmd.Execute())

	cmd = NewStorageDelete()
	cmd.SetArgs([]string{name})
	require.NoError(t, cmd.Execute())

	require.NoError(t, NewStorageScan().Execute())
}
