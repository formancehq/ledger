package elasticsearch

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		config      Config
		expectError string
	}

	for _, testCase := range []testCase{
		{
			name: "minimal valid",
			config: Config{
				Endpoint: "http://localhost:9200",
				Index:    "index",
			},
		},
		{
			name: "minimal index",
			config: Config{
				Endpoint: "http://localhost:9200",
			},
			expectError: "missing index",
		},
		{
			name:        "missing endpoint",
			config:      Config{},
			expectError: "elasticsearch endpoint is required",
		},
		{
			name: "with authentication (username/password)",
			config: Config{
				Endpoint: "http://localhost:9200",
				Authentication: &Authentication{
					Username: "root",
					Password: "password",
				},
				Index: "index",
			},
		},
		{
			name: "with authentication (aws)",
			config: Config{
				Endpoint: "http://localhost:9200",
				Authentication: &Authentication{
					AWSEnabled: true,
				},
				Index: "index",
			},
		},
		{
			name: "with username and no password",
			config: Config{
				Endpoint: "http://localhost:9200",
				Authentication: &Authentication{
					Username: "root",
				},
				Index: "index",
			},
			expectError: "authentication configuration is invalid: username and password must be defined together",
		},
		{
			name: "with username defined and aws enabled",
			config: Config{
				Endpoint: "http://localhost:9200",
				Authentication: &Authentication{
					Username:   "root",
					Password:   "password",
					AWSEnabled: true,
				},
				Index: "index",
			},
			expectError: "authentication configuration is invalid: username and password defined while aws is enabled",
		},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.config.Validate()
			if testCase.expectError != "" {
				require.NotNil(t, err)
				require.Equal(t, testCase.expectError, err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
