package upgrade

import (
	"fmt"
	"os"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

// NewCommand creates the upgrade command.
func NewCommand(currentVersion string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade ledgerctl to the latest version",
		Long:  "Download and install the latest version of ledgerctl from GitHub releases",
		RunE: func(cmd *cobra.Command, _ []string) error {
			channel, _ := cmd.Flags().GetString("channel")
			force, _ := cmd.Flags().GetBool("force")
			dryRun, _ := cmd.Flags().GetBool("dry-run")

			return runUpgrade(currentVersion, channel, force, dryRun)
		},
	}

	cmd.Flags().String("channel", "nightly", "Release channel: \"nightly\" or \"stable\"")
	cmd.Flags().Bool("force", false, "Upgrade even if already on the latest version")
	cmd.Flags().Bool("dry-run", false, "Check for updates without installing")

	return cmd
}

func runUpgrade(currentVersion, channel string, force, dryRun bool) error {
	// Warn if version is "dev" (built without ldflags).
	if currentVersion == "dev" && !force {
		pterm.Warning.Println("Cannot determine current version (dev build). Use --force to upgrade anyway.")
		return nil
	}

	spinner, _ := pterm.DefaultSpinner.Start(
		fmt.Sprintf("Checking for updates (channel: %s)...", channel))

	release, err := fetchRelease(channel)
	if err != nil {
		spinner.Fail("Failed to check for updates")
		return cmdutil.Displayed(fmt.Errorf("failed to fetch release info: %w", err))
	}

	latestVersion := releaseVersion(release)
	_ = spinner.Stop()

	pterm.Info.Printfln("Current version: %s", pterm.Cyan(currentVersion))
	pterm.Info.Printfln("Latest version:  %s", pterm.Cyan(latestVersion))

	if isUpToDate(currentVersion, release) && !force {
		pterm.Success.Println("Already up to date!")
		return nil
	}

	if dryRun {
		pterm.Info.Println("Update available (dry-run mode, not installing)")
		return nil
	}

	// Find the archive and checksums assets.
	archiveAsset, err := findAsset(release)
	if err != nil {
		return err
	}

	checksumsAsset, err := findChecksumsAsset(release)
	if err != nil {
		return err
	}

	// Download, verify, and extract.
	spinner, _ = pterm.DefaultSpinner.Start(
		fmt.Sprintf("Downloading %s...", archiveAsset.Name))

	extractedPath, err := downloadAndVerify(archiveAsset, checksumsAsset, spinner)
	if err != nil {
		spinner.Fail("Failed to download update")
		return cmdutil.Displayed(err)
	}
	defer func() { _ = os.Remove(extractedPath) }()

	spinner.Success("Download complete, checksum verified")

	// Replace the binary.
	spinner, _ = pterm.DefaultSpinner.Start("Installing update...")

	binaryPath, err := replaceBinary(extractedPath)
	if err != nil {
		spinner.Fail("Failed to install update")
		return cmdutil.Displayed(err)
	}

	spinner.Success(fmt.Sprintf("ledgerctl upgraded to %s", pterm.Cyan(latestVersion)))
	pterm.Info.Printfln("Binary: %s", pterm.Cyan(binaryPath))

	return nil
}
