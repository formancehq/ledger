//go:build it

package storage

//)
//
//var (
//	srv *pgtesting.PostgresServer
//)
//
//func TestMain(m *testing.M) {
//	utils.WithTestMain(func(t *utils.TestingTForMain) int {
//		srv = pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()))
//
//		return m.Run()
//	})
//}
//
//func TestListEnabledPipelines(t *testing.T) {
//	ctx := logging.TestingContext()
//
//	db, err := bunconnect.OpenSQLDB(ctx, bunconnect.ConnectionOptions{
//		DatabaseSourceName: srv.NewDatabase(t).ConnString(),
//	})
//	require.NoError(t, err)
//
//	// Create tables
//	require.NoError(t, NewMigrator(db).Up(ctx))
//
//	// Create the store
//	store := NewPostgresStore(db)
//
//	// Create a connector
//	connector := ingester.NewConnector(
//		ingester.NewConnectorConfiguration("connector1", json.RawMessage("")),
//	)
//	require.NoError(t, store.CreateConnector(ctx, connector))
//
//	// Creating a pair which will be marked as ready
//	alivePipeline := ingester.NewPipeline(
//		ingester.NewPipelineConfiguration("module1", connector.ID),
//		ingester.NewReadyState(),
//	)
//
//	// Save a state
//	require.NoError(t, store.CreatePipeline(ctx, alivePipeline))
//
//	// Creating a pair which will be marked as stopped
//	stoppedPipeline := ingester.NewPipeline(
//		ingester.NewPipelineConfiguration("module2", connector.ID),
//		ingester.NewStopState(ingester.NewReadyState()),
//	)
//
//	// Save a state
//	require.NoError(t, store.CreatePipeline(ctx, stoppedPipeline))
//
//	// Read all states
//	states, err := store.ListEnabledPipelines(ctx)
//	require.NoError(t, err)
//	require.Len(t, states, 1)
//	require.Equal(t, alivePipeline, states[0])
//}
//
//func TestCreatePipeline(t *testing.T) {
//
//	ctx := logging.TestingContext()
//
//	db, err := bunconnect.OpenSQLDB(ctx, bunconnect.ConnectionOptions{
//		DatabaseSourceName: srv.NewDatabase(t).ConnString(),
//	})
//	require.NoError(t, err)
//
//	// Create tables
//	require.NoError(t, NewMigrator(db).Up(ctx))
//
//	// Create the store
//	store := NewPostgresStore(db)
//
//	// Create a connector
//	connector := ingester.NewConnector(
//		ingester.NewConnectorConfiguration("connector1", json.RawMessage("")),
//	)
//	require.NoError(t, store.CreateConnector(ctx, connector))
//
//	// Creating a pipeline which will be marked as ready
//	alivePipeline := ingester.NewPipeline(
//		ingester.NewPipelineConfiguration("module1", connector.ID),
//		ingester.NewReadyState(),
//	)
//
//	// Save a state
//	require.NoError(t, store.CreatePipeline(ctx, alivePipeline))
//
//	// Try to create the same pipeline again
//	require.IsType(t, controller.ErrPipelineAlreadyExists{}, store.CreatePipeline(ctx, alivePipeline))
//
//	// Try to create another pipeline with the same configuration
//	newPipeline := ingester.NewPipeline(
//		ingester.NewPipelineConfiguration("module1", connector.ID),
//		ingester.NewReadyState(),
//	)
//	require.IsType(t, controller.ErrPipelineAlreadyExists{}, store.CreatePipeline(ctx, newPipeline))
//}
//
//func TestDeletePipeline(t *testing.T) {
//
//	ctx := logging.TestingContext()
//
//	db, err := bunconnect.OpenSQLDB(ctx, bunconnect.ConnectionOptions{
//		DatabaseSourceName: srv.NewDatabase(t).ConnString(),
//	})
//	require.NoError(t, err)
//
//	// Create tables
//	require.NoError(t, NewMigrator(db).Up(ctx))
//
//	// Create the store
//	store := NewPostgresStore(db)
//
//	// Create a connector
//	connector := ingester.NewConnector(
//		ingester.NewConnectorConfiguration("connector1", json.RawMessage("")),
//	)
//	require.NoError(t, store.CreateConnector(ctx, connector))
//
//	// Creating a pair which will be marked as ready
//	alivePipeline := ingester.NewPipeline(
//		ingester.NewPipelineConfiguration("module1", connector.ID),
//		ingester.NewReadyState(),
//	)
//
//	// Save a state
//	require.NoError(t, store.CreatePipeline(ctx, alivePipeline))
//
//	// Try to create the same pipeline again
//	require.NoError(t, store.DeletePipeline(ctx, alivePipeline.ID))
//}
//
//func TestDeleteConnector(t *testing.T) {
//	ctx := logging.TestingContext()
//
//	db, err := bunconnect.OpenSQLDB(ctx, bunconnect.ConnectionOptions{
//		DatabaseSourceName: srv.NewDatabase(t).ConnString(),
//	})
//	require.NoError(t, err)
//
//	// Create tables
//	require.NoError(t, NewMigrator(db).Up(ctx))
//
//	// Create the store
//	store := NewPostgresStore(db)
//
//	// Create a connector
//	connector := ingester.NewConnector(
//		ingester.NewConnectorConfiguration("connector1", json.RawMessage("")),
//	)
//	require.NoError(t, store.CreateConnector(ctx, connector))
//
//	// Creating a pipeline which will be marked as ready
//	pipeline := ingester.NewPipeline(
//		ingester.NewPipelineConfiguration("module1", connector.ID),
//		ingester.NewReadyState(),
//	)
//
//	// Save a state
//	require.NoError(t, store.CreatePipeline(ctx, pipeline))
//
//	err = store.DeleteConnector(ctx, pipeline.ConnectorID)
//	require.Error(t, err)
//}
