package drivers

import (
	"context"
	ledger "github.com/formancehq/ledger/internal"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
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
	connector := NewMockDriver(ctrl)
	logger := logging.Testing()
	ctx := context.TODO()

	log := NewLogWithLedger("module1", ledger.Log{})

	connector.EXPECT().Start(gomock.Any()).Return(nil)
	connector.EXPECT().Stop(gomock.Any()).Return(nil)
	connector.EXPECT().
		Accept(gomock.Any(), log).
		Return([]error{nil}, nil)

	batcher := newBatcher(connector, Batching{
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
