package main

import (
	"encoding/json"
	"fmt"
	"github.com/formancehq/ledger/internal/bus"
	"github.com/invopop/jsonschema"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"reflect"
)

func newDocEventsCommand() *cobra.Command {
	const (
		writeDirFlag = "write-dir"
	)
	cmd := &cobra.Command{
		RunE: func(cmd *cobra.Command, _ []string) error {

			writeDir, err := cmd.Flags().GetString(writeDirFlag)
			if err != nil {
				return fmt.Errorf("failed to get write-dir flag: %w", err)
			}

			err = os.MkdirAll(writeDir, 0755)
			if err != nil {
				return fmt.Errorf("failed to create write-dir: %w", err)
			}

			for _, o := range []any{
				bus.CommittedTransactions{},
				bus.DeletedMetadata{},
				bus.SavedMetadata{},
				bus.RevertedTransaction{},
			} {
				schema := jsonschema.Reflect(o)
				data, err := json.MarshalIndent(schema, "", "  ")
				if err != nil {
					return fmt.Errorf("failed to marshal schema: %w", err)
				}
				err = os.WriteFile(filepath.Join(writeDir, reflect.TypeOf(o).Name()+".json"), data, 0600)
				if err != nil {
					return fmt.Errorf("failed to write schema: %w", err)
				}
			}

			return nil
		},
	}
	cmd.Flags().String(writeDirFlag, "", "directory to write events to")

	return cmd
}

func main() {
	if err := newDocEventsCommand().Execute(); err != nil {
		os.Exit(1)
	}
}
