// fctl-plugin-publish uploads GoReleaser binaries and emits catalogue schema 1.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/formancehq/ledger/misc/fctl-plugin/internal/catalogue"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, output, diagnostics io.Writer) error {
	flags := flag.NewFlagSet("fctl-plugin-publish", flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	var options catalogue.PublishOptions
	flags.StringVar(&options.ServiceVersion, "service-version", "", "Exact co-released Ledger service version (without v prefix)")
	flags.StringVar(&options.ManifestPath, "manifest", "", "SDK manifest exported by the native plugin")
	flags.StringVar(&options.ArtifactsPath, "artifacts", "", "GoReleaser artifacts.json")
	flags.StringVar(&options.SourceRoot, "source-root", "", "Root containing raw GoReleaser binaries")
	flags.StringVar(&options.Registry, "registry", "https://ghcr.io", "Public OCI registry HTTPS origin")
	flags.StringVar(&options.Repository, "repository", "formancehq/fctl-plugin-ledger", "Public OCI repository")
	flags.StringVar(&options.Layout, "layout", "", "Write a local OCI image layout instead of pushing to the registry")
	flags.IntVar(&options.Revision, "revision", 1, "Positive plugin revision for the exact Ledger version")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}

		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}

	return catalogue.Publish(context.Background(), options, output, diagnostics)
}
