package main

import (
	"encoding/json"
	"github.com/google/uuid"
	"github.com/pulumi/pulumi/pkg/v3/testing/integration"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestProgram(t *testing.T) {

	type testCase struct {
		name   string
		config map[string]any
	}
	for _, tc := range []testCase{
		{
			name: "nominal",
			config: map[string]any{
				"timeout": 30,
				"storage": map[string]any{
					"postgres": map[string]any{
						"install": true,
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := map[string]string{}
			for k, v := range tc.config {
				data, err := json.Marshal(v)
				require.NoError(t, err)

				config[k] = string(data)
			}
			config["namespace"] = "ledger-tests-pulumi-" + uuid.NewString()[:8]

			integration.ProgramTest(t, &integration.ProgramTestOptions{
				Quick:       true,
				SkipRefresh: true,
				Dir:         ".",
				Config:      config,
				Stdout:      os.Stdout,
				Stderr:      os.Stderr,
				Verbose:     testing.Verbose(),
			})
		})
	}
}
