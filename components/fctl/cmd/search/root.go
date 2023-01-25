package search

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	const (
		sizeFlag = "size"
	)
	return fctl.NewStackCommand("search <object-type> <terms>...",
		fctl.WithAliases("se"),
		fctl.WithArgs(cobra.MinimumNArgs(1)),
		fctl.WithIntFlag(sizeFlag, 5, "Number of items to fetch"),
		fctl.WithValidArgs("ANY", "ACCOUNT", "TRANSACTION", "ASSET", "PAYMENT"),
		fctl.WithShortDescription("Search in all services"),
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

			searchClient, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return err
			}

			target := strings.ToUpper(args[0])
			if target == "ANY" {
				target = ""
			}
			terms := make([]string, 0)
			if len(args) > 1 {
				terms = args[1:]
			}
			size := int64(fctl.GetInt(cmd, sizeFlag))

			response, _, err := searchClient.SearchApi.Search(cmd.Context()).Query(formance.Query{
				PageSize: &size,
				Terms:    terms,
				Target:   &target,
			}).Execute()
			if err != nil {
				return err
			}

			if target == "" {
				tableData := make([][]string, 0)
				for kind, values := range response.Data {
					for _, value := range values.([]any) {
						dataAsJson, err := json.Marshal(value)
						if err != nil {
							return err
						}

						dataAsJsonString := string(dataAsJson)
						if len(dataAsJsonString) > 100 {
							dataAsJsonString = dataAsJsonString[:100] + "..."
						}

						tableData = append(tableData, []string{
							kind, dataAsJsonString,
						})
					}
				}
				tableData = fctl.Prepend(tableData, []string{"Kind", "Object"})

				return pterm.DefaultTable.
					WithHasHeader().
					WithWriter(cmd.OutOrStdout()).
					WithData(tableData).
					Render()
			}

			switch target {
			case "TRANSACTION":
				fctl.Section.WithWriter(cmd.OutOrStdout()).Println("Transactions")
				err = displayTransactions(cmd.OutOrStdout(), response.Cursor.Data)
			case "ACCOUNT":
				fctl.Section.WithWriter(cmd.OutOrStdout()).Println("Accounts")
				err = displayAccounts(cmd.OutOrStdout(), response.Cursor.Data)
			case "ASSET":
				fctl.Section.WithWriter(cmd.OutOrStdout()).Println("Assets")
				err = displayAssets(cmd.OutOrStdout(), response.Cursor.Data)
			case "PAYMENT":
				fctl.Section.WithWriter(cmd.OutOrStdout()).Println("Payments")
				err = displayPayments(cmd.OutOrStdout(), response.Cursor.Data)
			}
			return err
		}),
	)
}

func displayPayments(out io.Writer, payments []interface{}) error {
	tableData := make([][]string, 0)
	for _, payment := range payments {
		payment := payment.(map[string]any)
		tableData = append(tableData, []string{
			payment["provider"].(string),
			payment["reference"].(string),
			fmt.Sprint(payment["amount"].(float64)),
			payment["asset"].(string),
			payment["createdAt"].(string),
			payment["scheme"].(string),
			payment["status"].(string),
			payment["type"].(string),
		})
	}
	tableData = fctl.Prepend(tableData, []string{"Provider", "Reference", "Account",
		"Asset", "Created at", "Scheme", "Status", "Type"})

	return pterm.DefaultTable.
		WithHasHeader().
		WithWriter(out).
		WithData(tableData).
		Render()
}

func displayAssets(out io.Writer, assets []interface{}) error {
	tableData := make([][]string, 0)
	for _, asset := range assets {
		asset := asset.(map[string]any)
		tableData = append(tableData, []string{
			asset["ledger"].(string),
			asset["name"].(string),
			asset["account"].(string),
			fmt.Sprint(asset["input"].(float64)),
			fmt.Sprint(asset["output"].(float64)),
		})
	}
	tableData = fctl.Prepend(tableData, []string{"Ledger", "Asset", "Account", "Input", "Output"})

	return pterm.DefaultTable.
		WithHasHeader().
		WithWriter(out).
		WithData(tableData).
		Render()
}

func displayAccounts(out io.Writer, accounts []interface{}) error {
	tableData := make([][]string, 0)
	for _, account := range accounts {
		account := account.(map[string]any)
		tableData = append(tableData, []string{
			// TODO: Missing property 'ledger' on api response
			//account["ledger"].(string),
			account["address"].(string),
		})
	}
	tableData = fctl.Prepend(tableData, []string{ /*"Ledger",*/ "Address"})

	return pterm.DefaultTable.
		WithHasHeader().
		WithWriter(out).
		WithData(tableData).
		Render()
}

func displayTransactions(out io.Writer, txs []interface{}) error {
	tableData := make([][]string, 0)
	for _, tx := range txs {
		tx := tx.(map[string]any)
		tableData = append(tableData, []string{
			tx["ledger"].(string),
			fmt.Sprint(tx["txid"].(float64)),
			tx["reference"].(string),
			tx["timestamp"].(string),
		})
	}
	tableData = fctl.Prepend(tableData, []string{"Ledger", "ID", "Reference", "Date"})

	return pterm.DefaultTable.
		WithHasHeader().
		WithWriter(out).
		WithData(tableData).
		Render()
}
