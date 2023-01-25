package processor

import (
	"testing"

	"github.com/formancehq/orchestration/internal/spec"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func TestProcessorEcho(t *testing.T) {

	// Set up the test suite and testing execution environment
	testSuite := &testsuite.WorkflowTestSuite{}

	env := testSuite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(Echo, Input{
		Specification: spec.Echo,
		Parameters: map[string]any{
			"message": "Hello ${username}",
		},
		Variables: map[string]string{
			"username": "John",
		},
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var ret string
	require.NoError(t, env.GetWorkflowResult(&ret))
	require.Equal(t, "Hello John", ret)
}
