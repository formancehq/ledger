//go:build it

package system

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/bun/bundebug"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/testing/docker"
	ledger "github.com/formancehq/ledger/internal"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"github.com/google/uuid"
	"github.com/uptrace/bun"
	"golang.org/x/sync/errgroup"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/stretchr/testify/require"
)

func TestLedgersCreate(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	store := newStore(t)

	const count = 30
	grp, ctx := errgroup.WithContext(ctx)
	createdLedgersChan := make(chan ledger.Ledger, count)

	for i := range count {
		grp.Go(func() error {
			l := ledger.MustNewWithDefault(fmt.Sprintf("ledger%d", i))

			ctx, cancel := context.WithDeadline(ctx, time.Now().Add(40*time.Second))
			defer cancel()

			err := store.CreateLedger(ctx, &l)
			if err != nil {
				return err
			}
			createdLedgersChan <- l

			return nil
		})
	}

	require.NoError(t, grp.Wait())

	close(createdLedgersChan)

	createdLedgers := make([]ledger.Ledger, 0)
	for createdLedger := range createdLedgersChan {
		createdLedgers = append(createdLedgers, createdLedger)
	}

	slices.SortStableFunc(createdLedgers, func(a, b ledger.Ledger) int {
		return a.ID - b.ID
	})

	for i, createdLedger := range createdLedgers {
		require.Equal(t, i+1, createdLedger.ID)
		require.NotEmpty(t, createdLedger.AddedAt)
	}
}

func TestLedgersList(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	store := newStore(t)

	ledgers := make([]ledger.Ledger, 0)
	pageSize := uint64(2)
	count := uint64(10)
	for i := uint64(0); i < count; i++ {
		m := metadata.Metadata{}
		if i%2 == 0 {
			m["foo"] = "bar"
		}
		l := ledger.MustNewWithDefault(fmt.Sprintf("ledger%d", i)).WithMetadata(m)
		err := store.CreateLedger(ctx, &l)
		require.NoError(t, err)

		ledgers = append(ledgers, l)
	}

	cursor, err := store.Ledgers().Paginate(ctx, storagecommon.InitialPaginatedQuery[ListLedgersQueryPayload]{
		PageSize: pageSize,
	})
	require.NoError(t, err)
	require.Len(t, cursor.Data, int(pageSize))
	require.Equal(t, ledgers[:pageSize], cursor.Data)

	for i := pageSize; i < count; i += pageSize {
		query := storagecommon.ColumnPaginatedQuery[ListLedgersQueryPayload]{}
		require.NoError(t, bunpaginate.UnmarshalCursor(cursor.Next, &query))

		cursor, err = store.Ledgers().Paginate(ctx, query)
		require.NoError(t, err)
		require.Len(t, cursor.Data, 2)
		require.Equal(t, ledgers[i:i+pageSize], cursor.Data)
	}
}

func TestLedgerUpdateMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	store := newStore(t)

	l := ledger.MustNewWithDefault(uuid.NewString())
	err := store.CreateLedger(ctx, &l)
	require.NoError(t, err)

	addedMetadata := metadata.Metadata{
		"foo": "bar",
	}
	err = store.UpdateLedgerMetadata(ctx, l.Name, addedMetadata)
	require.NoError(t, err)

	ledgerFromDB, err := store.GetLedger(ctx, l.Name)
	require.NoError(t, err)
	require.Equal(t, addedMetadata, ledgerFromDB.Metadata)
}

func TestLedgerDeleteMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	store := newStore(t)

	l := ledger.MustNewWithDefault(uuid.NewString()).WithMetadata(metadata.Metadata{
		"foo": "bar",
	})

	err := store.CreateLedger(ctx, &l)
	require.NoError(t, err)

	err = store.DeleteLedgerMetadata(ctx, l.Name, "foo")
	require.NoError(t, err)

	ledgerFromDB, err := store.GetLedger(ctx, l.Name)
	require.NoError(t, err)
	require.Equal(t, metadata.Metadata{}, ledgerFromDB.Metadata)
}

func TestListEnabledPipelines(t *testing.T) {
	ctx := logging.TestingContext()

	store := newStore(t)

	// Create a connector
	connector := ledger.NewConnector(
		ledger.NewConnectorConfiguration("connector1", json.RawMessage("")),
	)
	require.NoError(t, store.CreateConnector(ctx, connector))

	// Creating a pair which will be marked as ready
	alivePipeline := ledger.NewPipeline(
		ledger.NewPipelineConfiguration("module1", connector.ID),
	)

	// Save a state
	require.NoError(t, store.CreatePipeline(ctx, alivePipeline))

	// Creating a pair which will be marked as stopped
	stoppedPipeline := ledger.NewPipeline(
		ledger.NewPipelineConfiguration("module2", connector.ID),
	)
	stoppedPipeline.Enabled = false

	// Save a state
	require.NoError(t, store.CreatePipeline(ctx, stoppedPipeline))

	// Read all states
	states, err := store.ListEnabledPipelines(ctx)
	require.NoError(t, err)
	require.Len(t, states, 1)
	require.Equal(t, alivePipeline, states[0])
}

func TestCreatePipeline(t *testing.T) {

	ctx := logging.TestingContext()

	store := newStore(t)

	// Create a connector
	connector := ledger.NewConnector(
		ledger.NewConnectorConfiguration("connector1", json.RawMessage("")),
	)
	require.NoError(t, store.CreateConnector(ctx, connector))

	// Creating a pipeline which will be marked as ready
	alivePipeline := ledger.NewPipeline(
		ledger.NewPipelineConfiguration("module1", connector.ID),
	)

	// Save a state
	require.NoError(t, store.CreatePipeline(ctx, alivePipeline))

	// Try to create the same pipeline again
	require.IsType(t, ledger.ErrPipelineAlreadyExists{}, store.CreatePipeline(ctx, alivePipeline))

	// Try to create another pipeline with the same configuration
	newPipeline := ledger.NewPipeline(
		ledger.NewPipelineConfiguration("module1", connector.ID),
	)
	require.IsType(t, ledger.ErrPipelineAlreadyExists{}, store.CreatePipeline(ctx, newPipeline))
}

func TestDeletePipeline(t *testing.T) {

	ctx := logging.TestingContext()

	// Create the store
	store := newStore(t)

	// Create a connector
	connector := ledger.NewConnector(
		ledger.NewConnectorConfiguration("connector1", json.RawMessage("")),
	)
	require.NoError(t, store.CreateConnector(ctx, connector))

	// Creating a pair which will be marked as ready
	alivePipeline := ledger.NewPipeline(
		ledger.NewPipelineConfiguration("module1", connector.ID),
	)

	// Save a state
	require.NoError(t, store.CreatePipeline(ctx, alivePipeline))

	// Try to create the same pipeline again
	require.NoError(t, store.DeletePipeline(ctx, alivePipeline.ID))
}

func TestUpdatePipeline(t *testing.T) {

	ctx := logging.TestingContext()

	// Create the store
	store := newStore(t)

	// Create a connector
	connector := ledger.NewConnector(
		ledger.NewConnectorConfiguration("connector1", json.RawMessage("")),
	)
	require.NoError(t, store.CreateConnector(ctx, connector))

	// Creating a pair which will be marked as ready
	alivePipeline := ledger.NewPipeline(
		ledger.NewPipelineConfiguration("module1", connector.ID),
	)

	// Save a state
	require.NoError(t, store.CreatePipeline(ctx, alivePipeline))

	// Try to create the same pipeline again
	_, err := store.UpdatePipeline(ctx, alivePipeline.ID, map[string]any{
		"enabled": false,
	})
	require.NoError(t, err)

	pipelineFromDB, err := store.GetPipeline(ctx, alivePipeline.ID)
	require.NoError(t, err)
	require.False(t, pipelineFromDB.Enabled)

	pipelineFromDB.Enabled = true
	pipelineFromDB.Version -= 1
	require.Equal(t, alivePipeline, *pipelineFromDB)
}

func TestDeleteConnector(t *testing.T) {
	ctx := logging.TestingContext()

	// Create the store
	store := newStore(t)

	// Create a connector
	connector := ledger.NewConnector(
		ledger.NewConnectorConfiguration("connector1", json.RawMessage("")),
	)
	require.NoError(t, store.CreateConnector(ctx, connector))

	// Creating a pipeline which will be marked as ready
	pipeline := ledger.NewPipeline(
		ledger.NewPipelineConfiguration("module1", connector.ID),
	)

	// Save a state
	require.NoError(t, store.CreatePipeline(ctx, pipeline))

	// Pipelines should be deleted in cascade
	err := store.DeleteConnector(ctx, pipeline.ConnectorID)
	require.NoError(t, err)
}

func newStore(t docker.T) *DefaultStore {
	t.Helper()

	ctx := logging.TestingContext()
	pgDatabase := srv.NewDatabase(t)

	hooks := make([]bun.QueryHook, 0)
	debugHook := bundebug.NewQueryHook()
	debugHook.Debug = os.Getenv("DEBUG") == "true"
	hooks = append(hooks, debugHook)
	db, err := bunconnect.OpenSQLDB(ctx, pgDatabase.ConnectionOptions(), hooks...)
	require.NoError(t, err)

	ret := New(db)
	require.NoError(t, ret.Migrate(ctx))

	return ret
}
