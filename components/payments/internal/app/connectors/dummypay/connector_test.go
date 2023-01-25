package dummypay

import (
	"context"
	"reflect"
	"testing"

	"github.com/formancehq/payments/internal/app/models"

	"github.com/formancehq/payments/internal/app/task"

	"github.com/formancehq/go-libs/logging"
	"github.com/stretchr/testify/assert"
)

// Create a minimal mock for connector installation.
type (
	mockConnectorContext struct {
		ctx context.Context
	}
	mockScheduler struct{}
)

func (mcc *mockConnectorContext) Context() context.Context {
	return mcc.ctx
}

func (mcc mockScheduler) Schedule(p models.TaskDescriptor, restart bool) error {
	return nil
}

func (mcc *mockConnectorContext) Scheduler() task.Scheduler {
	return mockScheduler{}
}

func TestConnector(t *testing.T) {
	t.Parallel()

	config := Config{}
	logger := logging.GetLogger(context.Background())

	fileSystem := newTestFS()

	connector := newConnector(logger, config, fileSystem)

	err := connector.Install(new(mockConnectorContext))
	assert.NoErrorf(t, err, "Install() failed")

	testCases := []struct {
		key  taskKey
		task task.Task
	}{
		{taskKeyReadFiles, taskReadFiles(config, fileSystem)},
		{taskKeyGenerateFiles, taskGenerateFiles(config, fileSystem)},
		{taskKeyIngest, taskIngest(config, TaskDescriptor{}, fileSystem)},
	}

	for _, testCase := range testCases {
		var taskDescriptor models.TaskDescriptor

		taskDescriptor, err = models.EncodeTaskDescriptor(TaskDescriptor{Key: testCase.key})
		assert.NoErrorf(t, err, "EncodeTaskDescriptor() failed")

		assert.EqualValues(t,
			reflect.ValueOf(testCase.task).String(),
			reflect.ValueOf(connector.Resolve(taskDescriptor)).String(),
		)
	}

	taskDescriptor, err := models.EncodeTaskDescriptor(TaskDescriptor{Key: "test"})
	assert.NoErrorf(t, err, "EncodeTaskDescriptor() failed")

	assert.EqualValues(t,
		reflect.ValueOf(func() error { return nil }).String(),
		reflect.ValueOf(connector.Resolve(taskDescriptor)).String(),
	)

	assert.NoError(t, connector.Uninstall(context.Background()))
}
