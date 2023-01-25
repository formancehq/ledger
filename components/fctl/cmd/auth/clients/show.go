package clients

import (
	"fmt"

	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewShowCommand() *cobra.Command {
	return fctl.NewCommand("show <client-id>",
		fctl.WithArgs(cobra.ExactArgs(1)),
		fctl.WithAliases("s"),
		fctl.WithShortDescription("Show client"),
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

			authClient, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return err
			}

			response, _, err := authClient.ClientsApi.ReadClient(cmd.Context(), args[0]).Execute()
			if err != nil {
				return err
			}

			tableData := pterm.TableData{}
			tableData = append(tableData, []string{pterm.LightCyan("ID"), response.Data.Id})
			tableData = append(tableData, []string{pterm.LightCyan("Name"), response.Data.Name})
			tableData = append(tableData, []string{pterm.LightCyan("Description"), fctl.StringPointerToString(response.Data.Description)})
			tableData = append(tableData, []string{pterm.LightCyan("Public"), fctl.BoolPointerToString(response.Data.Public)})

			fctl.Section.WithWriter(cmd.OutOrStdout()).Println("Information :")
			if err := pterm.DefaultTable.
				WithWriter(cmd.OutOrStdout()).
				WithData(tableData).
				Render(); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), "")

			if len(response.Data.RedirectUris) > 0 {
				fctl.Highlightln(cmd.OutOrStdout(), "Redirect URIs :")
				if err := pterm.DefaultBulletList.WithWriter(cmd.OutOrStdout()).WithItems(fctl.Map(response.Data.RedirectUris, func(redirectURI string) pterm.BulletListItem {
					return pterm.BulletListItem{
						Text:        redirectURI,
						TextStyle:   pterm.NewStyle(pterm.FgDefault),
						BulletStyle: pterm.NewStyle(pterm.FgLightCyan),
					}
				})).Render(); err != nil {
					return err
				}
			}

			if len(response.Data.PostLogoutRedirectUris) > 0 {
				fctl.Highlightln(cmd.OutOrStdout(), "Post logout redirect URIs :")
				if err := pterm.DefaultBulletList.WithWriter(cmd.OutOrStdout()).WithItems(fctl.Map(response.Data.PostLogoutRedirectUris, func(redirectURI string) pterm.BulletListItem {
					return pterm.BulletListItem{
						Text:        redirectURI,
						TextStyle:   pterm.NewStyle(pterm.FgDefault),
						BulletStyle: pterm.NewStyle(pterm.FgLightCyan),
					}
				})).Render(); err != nil {
					return err
				}
			}

			if len(response.Data.Secrets) > 0 {
				fctl.Section.WithWriter(cmd.OutOrStdout()).Println("Secrets :")

				if err := pterm.DefaultTable.
					WithWriter(cmd.OutOrStdout()).
					WithHasHeader(true).
					WithData(fctl.Prepend(
						fctl.Map(response.Data.Secrets, func(secret formance.ClientSecret) []string {
							return []string{
								secret.Id, secret.Name, secret.LastDigits,
							}
						}),
						[]string{"ID", "Name", "Last digits"},
					)).
					Render(); err != nil {
					return err
				}
			}

			return nil
		}),
	)
}
