package controller

import (
	"context"
	"database/sql"
	ingester "github.com/formancehq/ledger/internal"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/ledger/internal/replication/runner"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGetPipeline(t *testing.T) {
	t.Parallel()
	t.Run("with existing pipeline", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockStore := NewMockStore(ctrl)
		mockRunner := NewMockRunner(ctrl)
		mockConfigValidator := NewMockConfigValidator(ctrl)

		controller := New(mockRunner, mockStore, mockConfigValidator, logging.Testing())

		pipelineConfiguration := ingester.NewPipelineConfiguration("module1", "connector1")
		pipeline := ingester.NewPipeline(pipelineConfiguration, ingester.NewReadyState())

		mockStore.EXPECT().
			GetPipeline(gomock.Any(), pipeline.ID).
			Return(&pipeline, nil)

		pipelineFromController, err := controller.GetPipeline(logging.TestingContext(), pipeline.ID)
		require.NoError(t, err)
		require.Equal(t, pipelineFromController, pipelineFromController)
	})
	t.Run("with not existing pipeline", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockStore := NewMockStore(ctrl)
		mockRunner := NewMockRunner(ctrl)
		mockConfigValidator := NewMockConfigValidator(ctrl)
		controller := New(mockRunner, mockStore, mockConfigValidator, logging.Testing())

		id := uuid.NewString()
		mockStore.EXPECT().
			GetPipeline(gomock.Any(), id).
			Return(nil, sql.ErrNoRows)

		_, err := controller.GetPipeline(logging.TestingContext(), id)
		require.IsType(t, ErrPipelineNotFound(""), err)
	})
}

func TestDeletePipeline(t *testing.T) {
	t.Parallel()
	t.Run("with existing pipeline (not running)", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockStore := NewMockStore(ctrl)
		mockRunner := NewMockRunner(ctrl)
		mockConfigValidator := NewMockConfigValidator(ctrl)
		controller := New(mockRunner, mockStore, mockConfigValidator, logging.Testing())

		id := uuid.NewString()

		mockRunner.EXPECT().
			GetPipeline(id).
			Return(nil, false)

		mockStore.EXPECT().
			DeletePipeline(gomock.Any(), id).
			Return(nil)

		err := controller.DeletePipeline(logging.TestingContext(), id)
		require.NoError(t, err)
	})
	t.Run("with not existing pipeline (not running)", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockStore := NewMockStore(ctrl)
		mockRunner := NewMockRunner(ctrl)
		mockConfigValidator := NewMockConfigValidator(ctrl)
		controller := New(mockRunner, mockStore, mockConfigValidator, logging.Testing())

		id := uuid.NewString()

		mockRunner.EXPECT().
			GetPipeline(id).
			Return(nil, false)

		mockStore.EXPECT().
			DeletePipeline(gomock.Any(), id).
			Return(sql.ErrNoRows)

		err := controller.DeletePipeline(logging.TestingContext(), id)
		require.IsType(t, ErrPipelineNotFound(""), err)
	})
	t.Run("with existing pipeline (running)", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockStore := NewMockStore(ctrl)
		mockRunner := NewMockRunner(ctrl)
		mockConfigValidator := NewMockConfigValidator(ctrl)
		controller := New(mockRunner, mockStore, mockConfigValidator, logging.Testing())

		id := uuid.NewString()

		mockPipeline := NewMockPipeline(ctrl)
		mockRunner.EXPECT().
			GetPipeline(id).
			Return(mockPipeline, true)

		mockPipeline.EXPECT().
			Stop().
			Return(nil)

		mockStore.EXPECT().
			DeletePipeline(gomock.Any(), id).
			Return(nil)

		err := controller.DeletePipeline(logging.TestingContext(), id)
		require.NoError(t, err)
	})
}

func TestStartPipeline(t *testing.T) {
	t.Parallel()
	t.Run("with existing pipeline", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockStore := NewMockStore(ctrl)
		mockRunner := NewMockRunner(ctrl)
		mockConfigValidator := NewMockConfigValidator(ctrl)
		controller := New(mockRunner, mockStore, mockConfigValidator, logging.Testing())

		pipelineConfiguration := ingester.NewPipelineConfiguration("module1", "connector1")
		pipeline := ingester.NewPipeline(pipelineConfiguration, ingester.NewReadyState())

		mockStore.EXPECT().
			GetPipeline(gomock.Any(), pipeline.ID).
			Return(&pipeline, nil)

		mockPipeline := NewMockPipeline(ctrl)
		mockRunner.EXPECT().
			StartPipeline(gomock.Any(), pipeline).
			Return(mockPipeline, nil)

		err := controller.StartPipeline(logging.TestingContext(), pipeline.ID)
		require.NoError(t, err)
	})
	t.Run("with not existing pipeline", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockStore := NewMockStore(ctrl)
		mockRunner := NewMockRunner(ctrl)
		mockConfigValidator := NewMockConfigValidator(ctrl)
		controller := New(mockRunner, mockStore, mockConfigValidator, logging.Testing())

		id := uuid.NewString()

		mockStore.EXPECT().
			GetPipeline(gomock.Any(), id).
			Return(nil, sql.ErrNoRows)

		err := controller.StartPipeline(logging.TestingContext(), id)
		require.IsType(t, ErrPipelineNotFound(""), err)
	})
	t.Run("with already started", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockStore := NewMockStore(ctrl)
		mockRunner := NewMockRunner(ctrl)
		mockConfigValidator := NewMockConfigValidator(ctrl)
		controller := New(mockRunner, mockStore, mockConfigValidator, logging.Testing())

		pipelineConfiguration := ingester.NewPipelineConfiguration("module1", "connector1")
		pipeline := ingester.NewPipeline(pipelineConfiguration, ingester.NewReadyState())

		mockStore.EXPECT().
			GetPipeline(gomock.Any(), pipeline.ID).
			Return(&pipeline, nil)

		mockRunner.EXPECT().
			StartPipeline(gomock.Any(), pipeline).
			Return(nil, runner.ErrAlreadyStarted(""))

		err := controller.StartPipeline(logging.TestingContext(), pipeline.ID)
		require.IsType(t, ErrAlreadyStarted(""), err)
	})
}

func TestCreatePipeline(t *testing.T) {
	t.Parallel()

	t.Run("with no error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockStore := NewMockStore(ctrl)
		mockRunner := NewMockRunner(ctrl)
		mockConfigValidator := NewMockConfigValidator(ctrl)
		controller := New(mockRunner, mockStore, mockConfigValidator, logging.Testing())

		pipelineConfiguration := ingester.NewPipelineConfiguration("module1", "connector1")

		mockStore.EXPECT().
			CreatePipeline(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, pipeline ingester.Pipeline) error {
				mockRunner.EXPECT().
					StartPipeline(gomock.Any(), pipeline).
					Return(nil, nil)
				return nil
			})

		_, err := controller.CreatePipeline(logging.TestingContext(), pipelineConfiguration)
		require.NoError(t, err)
	})
	t.Run("with connector not found", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		mockStore := NewMockStore(ctrl)
		mockRunner := NewMockRunner(ctrl)
		mockConfigValidator := NewMockConfigValidator(ctrl)
		controller := New(mockRunner, mockStore, mockConfigValidator, logging.Testing())

		pipelineConfiguration := ingester.NewPipelineConfiguration("module1", "connector1")
		mockStore.EXPECT().
			CreatePipeline(gomock.Any(), gomock.Any()).
			Return(NewErrConnectorNotFound("connector1"))

		_, err := controller.CreatePipeline(logging.TestingContext(), pipelineConfiguration)
		require.IsType(t, ErrConnectorNotFound(""), err)
	})
}

func TestSwitchStateOnPipeline(t *testing.T) {
	t.Parallel()

	type subTestCase struct {
		name                string
		initialState        ingester.State
		expectState         ingester.State
		expectError         error
		returnPipelineError error
	}

	type testCase struct {
		name         string
		controllerFn func(*Controller, context.Context, string) error
		pipelineFn   func(pipeline *MockPipelineMockRecorder) *gomock.Call
		subTests     []subTestCase
	}
	for _, testCase := range []testCase{
		{
			name:         "reset",
			controllerFn: (*Controller).ResetPipeline,
			pipelineFn:   (*MockPipelineMockRecorder).Reset,
			subTests: []subTestCase{{
				expectState:  ingester.NewInitState(),
				initialState: ingester.NewReadyState(),
			}},
		},
		{
			name:         "stop",
			controllerFn: (*Controller).StopPipeline,
			pipelineFn:   (*MockPipelineMockRecorder).Stop,
			subTests: []subTestCase{{
				name:         "with no error",
				expectState:  ingester.NewStopState(ingester.NewReadyState()),
				initialState: ingester.NewReadyState(),
			}},
		},
		{
			name:         "pause",
			controllerFn: (*Controller).PausePipeline,
			pipelineFn:   (*MockPipelineMockRecorder).Pause,
			subTests: []subTestCase{
				{
					name:         "with no error",
					expectState:  ingester.NewPauseState(ingester.NewInitState()),
					initialState: ingester.NewInitState(),
				},
				{
					name:                "with pipeline already paused",
					initialState:        ingester.NewPauseState(ingester.NewInitState()),
					expectError:         ErrInvalidStateSwitch{},
					returnPipelineError: runner.ErrInvalidStateSwitch{},
				},
			},
		},
		{
			name:         "resume",
			controllerFn: (*Controller).ResumePipeline,
			pipelineFn:   (*MockPipelineMockRecorder).Resume,
			subTests: []subTestCase{
				{
					name:         "with no error",
					expectState:  ingester.NewInitState(),
					initialState: ingester.NewPauseState(ingester.NewInitState()),
				},
				{
					name:                "with invalid state switch",
					initialState:        ingester.NewReadyState(),
					expectError:         ErrInvalidStateSwitch{},
					returnPipelineError: runner.ErrInvalidStateSwitch{},
				},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			for _, subTest := range testCase.subTests {
				t.Run(subTest.name, func(t *testing.T) {
					t.Parallel()

					ctrl := gomock.NewController(t)
					mockStore := NewMockStore(ctrl)
					mockRunner := NewMockRunner(ctrl)
					mockConfigValidator := NewMockConfigValidator(ctrl)
					controller := New(mockRunner, mockStore, mockConfigValidator, logging.Testing())

					id := uuid.NewString()

					mockPipeline := NewMockPipeline(ctrl)
					mockRunner.EXPECT().
						GetPipeline(id).
						Return(mockPipeline, true)

					signal := runner.NewSignal(pointer.For(subTest.initialState))
					mockPipeline.EXPECT().
						GetActiveState().
						Return(signal)

					expectation := mockPipeline.EXPECT()
					testCase.pipelineFn(expectation).Return(subTest.returnPipelineError)

					errCh := make(chan error, 1)
					go func() {
						errCh <- testCase.controllerFn(controller, logging.TestingContext(), id)
					}()

					if subTest.expectError != nil {
						select {
						case err := <-errCh:
							require.IsType(t, subTest.expectError, err)
						case <-time.After(time.Second):
							require.Fail(t, "should be ok")
						}
					} else {
						require.Eventually(t, func() bool {
							return signal.CountListeners() == 1
						}, time.Second, 10*time.Millisecond)

						signal.Signal(subTest.expectState)

						select {
						case err := <-errCh:
							require.NoError(t, err)
						case <-time.After(time.Second):
							require.Fail(t, "should be ok")
						}
					}
				})
			}

			t.Run("with pipeline not found", func(t *testing.T) {
				t.Parallel()

				ctrl := gomock.NewController(t)
				mockStore := NewMockStore(ctrl)
				mockRunner := NewMockRunner(ctrl)
				mockConfigValidator := NewMockConfigValidator(ctrl)
				controller := New(mockRunner, mockStore, mockConfigValidator, logging.Testing())

				id := uuid.NewString()

				mockRunner.EXPECT().
					GetPipeline(id).
					Return(nil, false)

				// When stopping, the controller make an additional call
				// to the database if the runner return a not found.
				// This way, the runner check if the pipeline really not exists,
				// or if it is just already stopped
				mockStore.EXPECT().
					GetPipeline(gomock.Any(), id).
					AnyTimes().
					Return(nil, sql.ErrNoRows)

				err := testCase.controllerFn(controller, logging.TestingContext(), id)
				require.IsType(t, ErrPipelineNotFound(""), err)
			})
		})
	}
}
