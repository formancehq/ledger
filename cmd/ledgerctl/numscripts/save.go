package numscripts

import (
	"fmt"
	"os"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewSaveCommand creates the numscripts save command.
func NewSaveCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "save <name>",
		Short: "Save a numscript to the library",
		Long: `Save a numscript to the global library.

If a script with the same name already exists, a new version is created.
The script content is read from a file (--file) or stdin.

Examples:
  ledgerctl numscripts save transfer --file transfer.num
  cat transfer.num | ledgerctl numscripts save transfer`,
		Args: cobra.ExactArgs(1),
		RunE: runSave,
	}

	cmd.Flags().String("file", "", "Path to the numscript file (reads stdin if omitted)")
	cmd.Flags().String("version", "", "Semver version (e.g. 1.0.0) or empty for latest")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runSave(cmd *cobra.Command, args []string) error {
	name := args[0]

	// Read content from file or stdin
	var content []byte
	filePath, _ := cmd.Flags().GetString("file")
	if filePath != "" {
		var err error
		content, err = os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("reading file %q: %w", filePath, err)
		}
	} else {
		var err error
		content, err = os.ReadFile("/dev/stdin")
		if err != nil {
			return fmt.Errorf("reading stdin: %w", err)
		}
	}

	if len(content) == 0 {
		return fmt.Errorf("numscript content is empty")
	}

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	version, _ := cmd.Flags().GetString("version")

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Saving numscript %s...", name))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_SaveNumscript{
				SaveNumscript: &servicepb.SaveNumscriptRequest{
					Name:    name,
					Content: string(content),
					Version: version,
				},
			},
		},
	}

	if err := cmdutil.SignRequests(cmd, requests); err != nil {
		spinner.Fail("Failed to sign request")
		return cmdutil.Displayed(err)
	}

	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{Requests: requests})
	if err != nil {
		_ = spinner.Stop()
		return cmdutil.FormatGRPCError("failed to save numscript", err)
	}

	if len(resp.Logs) > 0 {
		if saved := resp.Logs[0].Payload.GetSavedNumscript(); saved != nil {
			spinner.Success("Saved")
			pterm.Println()
			pterm.Printf("Name:    %s\n", pterm.Cyan(saved.Info.Name))
			pterm.Printf("Version: %s\n", saved.Info.Version)
			return nil
		}
	}

	spinner.Success("Saved")
	return nil
}
