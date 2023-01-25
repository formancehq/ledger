package workflow

import (
	"encoding/json"
	"testing"

	"github.com/formancehq/go-libs/pgtesting"
	"github.com/formancehq/orchestration/internal/storage"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func TestConfig(t *testing.T) {
	t.Parallel()

	database := pgtesting.NewPostgresDatabase(t)
	db := storage.LoadDB(database.ConnString(), testing.Verbose())
	require.NoError(t, storage.Migrate(db, testing.Verbose()))
	runner := NewRunner(db)

	rawConfig, err := json.Marshal(map[string]any{
		"stages": []map[string]any{
			{
				"noop": map[string]any{},
			},
		},
	})
	require.NoError(t, err)

	config := Config{}
	require.NoError(t, json.Unmarshal(rawConfig, &config))

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(runner.Run, Input{
		Config: config,
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}
