// fctl-plugin-catalogue prepares a single immutable platform release entry.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/formancehq/ledger/fctl-plugin/internal/catalogue"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, output, diagnostics io.Writer) error {
	flags := flag.NewFlagSet("fctl-plugin-catalogue", flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	manifestPath := flags.String("manifest", "", "SDK manifest exported by the native plugin")
	binary := flags.String("binary", "", "Raw GoReleaser platform executable to checksum")
	registry := flags.String("registry", "", "Registry HTTPS origin, or loopback HTTP origin")
	repository := flags.String("repository", "", "OCI repository")
	digest := flags.String("digest", "", "Immutable OCI manifest sha256 digest")
	goos := flags.String("os", "", "Executable operating system")
	arch := flags.String("arch", "", "Executable architecture")
	revision := flags.Int("revision", 1, "Positive plugin revision for this service version")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}

		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	manifest, err := catalogue.ReadManifest(*manifestPath)
	if err != nil {
		return err
	}
	release, err := catalogue.NewRelease(manifest, *binary,
		catalogue.Artifact{Registry: *registry, Repository: *repository, Digest: *digest},
		catalogue.Platform{OS: *goos, Arch: *arch}, *revision)
	if err != nil {
		return err
	}

	return json.NewEncoder(output).Encode(catalogue.Catalogue{SchemaVersion: 1, Releases: []catalogue.Release{release}})
}
