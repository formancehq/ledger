package cmd

import (
	"encoding/json"
	"github.com/formancehq/ledger/internal/bus"
	"github.com/invopop/jsonschema"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"reflect"
)

func NewDocEventsCommand() *cobra.Command {
	const (
		writeDirFlag = "write-dir"
	)
	cmd := &cobra.Command{
		Use: "events",
		RunE: func(cmd *cobra.Command, args []string) error {

			writeDir, err := cmd.Flags().GetString(writeDirFlag)
			if err != nil {
				return errors.Wrap(err, "failed to get write-dir flag")
			}

			err = os.MkdirAll(writeDir, 0755)
			if err != nil {
				return errors.Wrap(err, "failed to create write-dir")
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
					return errors.Wrap(err, "failed to marshal schema")
				}
				err = os.WriteFile(filepath.Join(writeDir, reflect.TypeOf(o).Name()+".json"), data, 0600)
				if err != nil {
					return errors.Wrap(err, "failed to write schema")
				}
			}

			return nil
		},
	}
	cmd.Flags().String(writeDirFlag, "", "directory to write events to")
	return cmd
}
