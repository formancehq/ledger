package drivers

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v4/logging"
)

func TestDriverFactoryWithBatching(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name        string
		config      map[string]any
		expectError string
	}{
		{
			name: "nominal",
		},
		{
			name: "with only maxItems defined for batching",
			config: map[string]any{
				"batching": map[string]any{
					"maxItems": 10,
				},
			},
		},
		{
			name: "with only flushInterval defined for batching",
			config: map[string]any{
				"batching": map[string]any{
					"flushInterval": "10ms",
				},
			},
		},
		{
			name: "with maxItems and flushInterval defined for batching",
			config: map[string]any{
				"batching": map[string]any{
					"maxItems":      10,
					"flushInterval": "10ms",
				},
			},
		},
		{
			name: "with invalid maxItems defined for batching",
			config: map[string]any{
				"batching": map[string]any{
					"maxItems": -1,
				},
			},
			expectError: "validating batching config: flushBytes must be greater than 0",
		},
		{
			name: "with invalid flushInterval defined for batching",
			config: map[string]any{
				"batching": map[string]any{
					"flushInterval": "-1",
				},
			},
			expectError: "extracting batching config: time: missing unit in duration \"-1\"",
		},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)

			rawConfig, _ := json.Marshal(testCase.config)

			underlyingExporterFactory := NewMockFactory(ctrl)
			underlyingExporterFactory.EXPECT().
				Create(gomock.Any(), "test").
				Return(&MockDriver{}, json.RawMessage(rawConfig), nil)

			logger := logging.Testing()
			f := NewWithBatchingDriverFactory(underlyingExporterFactory, logger)
			exporter, _, err := f.Create(logging.TestingContext(), "test")
			if testCase.expectError == "" {
				require.NoError(t, err)
				require.NotNil(t, exporter)
			} else {
				require.Equal(t, testCase.expectError, err.Error())
			}
		})
	}
}
