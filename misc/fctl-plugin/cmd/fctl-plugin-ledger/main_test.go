package main

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/formancehq/fctl/pkg/pluginsdk"
)

func TestMetadataFlags(t *testing.T) {
	t.Parallel()
	for _, flag := range []string{"--manifest", "--version"} {
		t.Run(flag, func(t *testing.T) {
			t.Parallel()
			var output bytes.Buffer
			if err := run([]string{flag}, &output, io.Discard); err != nil {
				t.Fatal(err)
			}
			if flag == "--manifest" {
				var manifest pluginsdk.Manifest
				if err := json.Unmarshal(output.Bytes(), &manifest); err != nil {
					t.Fatal(err)
				}
				if manifest.Name != "ledger" || manifest.Version != serviceVersion || manifest.ProtocolVersion != pluginsdk.ProtocolVersion {
					t.Fatalf("unexpected manifest: %#v", manifest)
				}
				if _, err := pluginsdk.FindCommand(manifest, []string{"ledger", "transactions", "create"}); err != nil {
					t.Fatal(err)
				}

				return
			}
			var version versionMetadata
			if err := json.Unmarshal(output.Bytes(), &version); err != nil {
				t.Fatal(err)
			}
			if version.Name != "ledger" || version.ServiceVersion != serviceVersion || version.Revision != 1 || version.ProtocolVersion != pluginsdk.ProtocolVersion {
				t.Fatalf("unexpected version metadata: %#v", version)
			}
		})
	}
}

func TestInvalidMetadataArguments(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{{"--manifest", "--version"}, {"--manifest", "extra"}, {"--unknown"}} {
		if err := run(args, io.Discard, io.Discard); err == nil {
			t.Fatalf("accepted invalid arguments: %v", args)
		}
	}
	if err := run([]string{"--help"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
}
