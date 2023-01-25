package internal

import (
	"fmt"
	"io"

	"github.com/formancehq/fctl/membershipclient"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pterm/pterm"
)

func PrintStackInformation(out io.Writer, profile *fctl.Profile, stack *membershipclient.Stack) error {
	baseUrlStr := profile.ServicesBaseUrl(stack).String()

	fctl.Section.WithWriter(out).Println("Information")
	tableData := pterm.TableData{}
	tableData = append(tableData, []string{pterm.LightCyan("ID"), stack.Id})
	tableData = append(tableData, []string{pterm.LightCyan("Name"), stack.Name})
	tableData = append(tableData, []string{pterm.LightCyan("Bound region"), func() string {
		if stack.BoundRegion == nil {
			return ""
		}
		return stack.BoundRegion.Id
	}()})
	tableData = append(tableData, []string{pterm.LightCyan("Ledger URI"), fmt.Sprintf("%s/api/ledger", baseUrlStr)})
	tableData = append(tableData, []string{pterm.LightCyan("Payments URI"), fmt.Sprintf("%s/api/payments", baseUrlStr)})
	tableData = append(tableData, []string{pterm.LightCyan("Search URI"), fmt.Sprintf("%s/api/search", baseUrlStr)})
	tableData = append(tableData, []string{pterm.LightCyan("Auth URI"), fmt.Sprintf("%s/api/auth", baseUrlStr)})
	tableData = append(tableData, []string{pterm.LightCyan("Wallets URI"), fmt.Sprintf("%s/api/wallets", baseUrlStr)})
	if err := pterm.DefaultTable.
		WithWriter(out).
		WithData(tableData).
		Render(); err != nil {
		return err
	}

	fctl.Println()
	fctl.Section.WithWriter(out).Println("Metadata")
	tableData = pterm.TableData{}
	for k, v := range stack.Metadata {
		tableData = append(tableData, []string{pterm.LightCyan(k), v})
	}

	return pterm.DefaultTable.
		WithWriter(out).
		WithData(tableData).
		Render()
}
