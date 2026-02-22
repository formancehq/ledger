package drivers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v4/logging"

	ledger "github.com/formancehq/ledger/internal"
)

func TestBatchingConfiguration(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name          string
		configuration Batching
		expectError   string
	}

	for _, testCase := range []testCase{
		{
			name: "nominal",
			configuration: Batching{
				FlushInterval: time.Second,
			},
		},
		{
			name:          "no configuration",
			configuration: Batching{},
			expectError:   "while configuring the batcher with unlimited size, you must configure the flush interval",
		},
		{
			name: "negative max item",
			configuration: Batching{
				MaxItems: -1,
			},
			expectError: "flushBytes must be greater than 0",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.configuration.Validate()
			if testCase.expectError != "" {
				require.EqualError(t, err, testCase.expectError)
			} else {
				require.NoError(t, err)
			}
		})
	}

}

func TestBatcher(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	driver := NewMockDriver(ctrl)
	logger := logging.Testing()
	ctx := t.Context()

	log := NewLogWithLedger("module1", ledger.Log{})

	driver.EXPECT().Start(gomock.Any()).Return(nil)
	driver.EXPECT().Stop(gomock.Any()).Return(nil)
	driver.EXPECT().
		Accept(gomock.Any(), log).
		Return([]error{nil}, nil)

	batcher := newBatcher(driver, Batching{
		MaxItems:      5,
		FlushInterval: 50 * time.Millisecond,
	}, logger)
	require.NoError(t, batcher.Start(ctx))
	t.Cleanup(func() {
		require.NoError(t, batcher.Stop(ctx))
	})

	itemsErrors, err := batcher.Accept(ctx, log)
	require.NoError(t, err)
	require.Len(t, itemsErrors, 1)
	require.Nil(t, itemsErrors[0])
}
