package transactions

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/formancehq/fctl/cmd/ledger/internal"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	const (
		amountVarFlag  = "amount-var"
		portionVarFlag = "portion-var"
		accountVarFlag = "account-var"
		metadataFlag   = "metadata"
		referenceFlag  = "reference"
	)
	return fctl.NewCommand("num -|<filename>",
		fctl.WithShortDescription("Execute a numscript script on a ledger"),
		fctl.WithDescription(`More help on variables can be found here: https://docs.formance.com/oss/ledger/reference/numscript/variables`),
		fctl.WithArgs(cobra.ExactArgs(1)),
		fctl.WithConfirmFlag(),
		fctl.WithStringSliceFlag(amountVarFlag, []string{""}, "Pass a variable of type 'amount'"),
		fctl.WithStringSliceFlag(portionVarFlag, []string{""}, "Pass a variable of type 'portion'"),
		fctl.WithStringSliceFlag(accountVarFlag, []string{""}, "Pass a variable of type 'account'"),
		fctl.WithStringSliceFlag(metadataFlag, []string{""}, "Metadata to use"),
		fctl.WithStringFlag(referenceFlag, "", "Reference to add to the generated transaction"),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {
			cfg, err := fctl.GetConfig(cmd)
			if err != nil {
				return err
			}

			organizationID, err := fctl.ResolveOrganizationID(cmd, cfg)
			if err != nil {
				return err
			}

			stack, err := fctl.ResolveStack(cmd, cfg, organizationID)
			if err != nil {
				return err
			}

			script, err := fctl.ReadFile(cmd, stack, args[0])
			if err != nil {
				return err
			}

			if !fctl.CheckStackApprobation(cmd, stack, "You are about to apply a numscript") {
				return fctl.ErrMissingApproval
			}

			client, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return err
			}

			vars := map[string]interface{}{}
			for _, v := range fctl.GetStringSlice(cmd, accountVarFlag) {
				parts := strings.SplitN(v, "=", 2)
				if len(parts) == 1 {
					return fmt.Errorf("malformed var: %s", v)
				}
				vars[parts[0]] = parts[1]
			}
			for _, v := range fctl.GetStringSlice(cmd, portionVarFlag) {
				parts := strings.SplitN(v, "=", 2)
				if len(parts) == 1 {
					return fmt.Errorf("malformed var: %s", v)
				}
				vars[parts[0]] = parts[1]
			}
			for _, v := range fctl.GetStringSlice(cmd, amountVarFlag) {
				parts := strings.SplitN(v, "=", 2)
				if len(parts) == 1 {
					return fmt.Errorf("malformed var: %s", v)
				}

				amountParts := strings.SplitN(parts[1], "/", 2)
				if len(amountParts) != 2 {
					return fmt.Errorf("malformed var: %s", v)
				}

				amount, err := strconv.ParseInt(amountParts[0], 10, 64)
				if err != nil {
					return fmt.Errorf("malformed var: %s", v)
				}

				vars[parts[0]] = map[string]any{
					"amount": amount,
					"asset":  amountParts[1],
				}
			}

			reference := fctl.GetString(cmd, referenceFlag)

			metadata, err := fctl.ParseMetadata(fctl.GetStringSlice(cmd, metadataFlag))
			if err != nil {
				return err
			}

			ledger := fctl.GetString(cmd, internal.LedgerFlag)
			response, _, err := client.ScriptApi.
				RunScript(cmd.Context(), ledger).
				Script(formance.Script{
					Plain:     script,
					Metadata:  metadata,
					Vars:      vars,
					Reference: &reference,
				}).
				Execute()
			if err != nil {
				return err
			}
			if err != nil {
				return errors.Wrapf(err, "executing numscript")
			}
			if response.ErrorCode != nil && *response.ErrorCode != "" {
				if response.ErrorMessage != nil {
					return errors.New(*response.ErrorMessage)
				}
				return errors.New(string(*response.ErrorCode))
			}

			return internal.PrintTransaction(cmd.OutOrStdout(), *response.Transaction)
		}),
	)
}
