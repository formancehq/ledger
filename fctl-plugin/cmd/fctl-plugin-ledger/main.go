// fctl-plugin-ledger serves Ledger commands through the public fctl protocol.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"

	"github.com/formancehq/fctl/pkg/pluginsdk"
	"github.com/formancehq/fctl/pkg/pluginsdk/transport"

	ledger "github.com/formancehq/ledger/fctl-plugin"
)

// GoReleaser supplies both values for each immutable product release artifact.
var (
	serviceVersion = "dev"
	revision       = "1"
)

type versionMetadata struct {
	Name            string `json:"name"`
	ServiceVersion  string `json:"serviceVersion"`
	Revision        int    `json:"revision"`
	ProtocolVersion int    `json:"protocolVersion"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, output, diagnostics io.Writer) error {
	flags := flag.NewFlagSet("fctl-plugin-ledger", flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	manifest := flags.Bool("manifest", false, "Print the SDK command manifest as JSON without starting the plugin")
	version := flags.Bool("version", false, "Print the service version and plugin revision as JSON")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}

		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if *manifest && *version {
		return errors.New("use either --manifest or --version")
	}
	pluginRevision, err := strconv.Atoi(revision)
	if err != nil || pluginRevision < 1 {
		return errors.New("plugin revision must be a positive integer")
	}
	if serviceVersion == "" {
		return errors.New("ledger service version must not be empty")
	}
	factory := func(client *http.Client) pluginsdk.Plugin {
		return ledger.NewVersion(client, serviceVersion)
	}
	if *manifest {
		metadata, err := factory(nil).GetManifest(context.Background())
		if err != nil {
			return err
		}

		return json.NewEncoder(output).Encode(metadata)
	}
	if *version {
		return json.NewEncoder(output).Encode(versionMetadata{
			Name: "ledger", ServiceVersion: serviceVersion,
			Revision: pluginRevision, ProtocolVersion: pluginsdk.ProtocolVersion,
		})
	}
	transport.Serve(factory)

	return nil
}
