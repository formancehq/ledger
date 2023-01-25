package api

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/formancehq/go-libs/pgtesting"
	"github.com/formancehq/orchestration/internal/storage"
	"github.com/formancehq/orchestration/internal/workflow"
	"github.com/go-chi/chi/v5"
	flag "github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"go.temporal.io/sdk/client"
)

type mockedRun struct {
	client.WorkflowRun
}

func (m mockedRun) GetRunID() string {
	return "foo"
}

type mockedClient struct {
	client.Client
}

func (c mockedClient) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error) {
	return mockedRun{}, nil
}

func test(t *testing.T, fn func(router *chi.Mux, m *workflow.Manager, db *bun.DB)) {
	t.Parallel()

	database := pgtesting.NewPostgresDatabase(t)
	db := storage.LoadDB(database.ConnString(), testing.Verbose())
	require.NoError(t, storage.Migrate(db, testing.Verbose()))
	manager := workflow.NewManager(db, mockedClient{})
	router := newRouter(manager)
	fn(router, manager, db)
}

func TestMain(m *testing.M) {
	flag.Parse()

	if err := pgtesting.CreatePostgresServer(); err != nil {
		log.Fatal(err)
	}
	code := m.Run()
	if err := pgtesting.DestroyPostgresServer(); err != nil {
		log.Println("unable to stop postgres server", err)
	}
	os.Exit(code)
}
