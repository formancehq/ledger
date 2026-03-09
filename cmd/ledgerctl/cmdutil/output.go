package cmdutil

import (
	"encoding/json"
	"os"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// AddOutputFlags registers --json and --yaml flags on the command.
// The two flags are mutually exclusive.
func AddOutputFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Bool("yaml", false, "Output as YAML")
	cmd.MarkFlagsMutuallyExclusive("json", "yaml")
}

// EncodeStructured checks whether --json or --yaml is set. When one is active
// it encodes data to os.Stdout and returns (true, nil) on success or
// (true, err) on failure. When neither flag is set it returns (false, nil) so
// the caller can fall through to its pterm rendering.
func EncodeStructured(cmd *cobra.Command, data any) (bool, error) {
	if jsonOutput, _ := cmd.Flags().GetBool("json"); jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")

		return true, encoder.Encode(data)
	}

	if yamlOutput, _ := cmd.Flags().GetBool("yaml"); yamlOutput {
		encoder := yaml.NewEncoder(os.Stdout)
		encoder.SetIndent(2)

		err := encoder.Encode(data)
		if closeErr := encoder.Close(); err == nil {
			err = closeErr
		}

		return true, err
	}

	return false, nil
}

// IsStructuredOutput returns true when --json or --yaml is active.
// Use this for paginated commands that need to skip interactive prompts.
func IsStructuredOutput(cmd *cobra.Command) bool {
	jsonOutput, _ := cmd.Flags().GetBool("json")
	yamlOutput, _ := cmd.Flags().GetBool("yaml")

	return jsonOutput || yamlOutput
}
