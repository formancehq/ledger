package clients

import (
	"strings"

	fctl "github.com/formancehq/fctl/pkg"
	"github.com/formancehq/formance-sdk-go"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewCreateCommand() *cobra.Command {
	const (
		publicFlag                = "public"
		trustedFlag               = "trusted"
		descriptionFlag           = "description"
		redirectUriFlag           = "redirect-uri"
		postLogoutRedirectUriFlag = "post-logout-redirect-uri"
	)
	return fctl.NewCommand("create <name>",
		fctl.WithAliases("c"),
		fctl.WithArgs(cobra.ExactArgs(1)),
		fctl.WithConfirmFlag(),
		fctl.WithBoolFlag(publicFlag, false, "Is client public"),
		fctl.WithBoolFlag(trustedFlag, false, "Is the client trusted"),
		fctl.WithStringFlag(descriptionFlag, "", "Client description"),
		fctl.WithStringSliceFlag(redirectUriFlag, []string{}, "Redirect URIS"),
		fctl.WithStringSliceFlag(postLogoutRedirectUriFlag, []string{}, "Post logout redirect uris"),
		fctl.WithShortDescription("Create client"),
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

			if !fctl.CheckStackApprobation(cmd, stack, "You are about to create a new OAuth2 client") {
				return fctl.ErrMissingApproval
			}

			authClient, err := fctl.NewStackClient(cmd, cfg, stack)
			if err != nil {
				return err
			}

			public := fctl.GetBool(cmd, publicFlag)
			trusted := fctl.GetBool(cmd, trustedFlag)
			description := fctl.GetString(cmd, descriptionFlag)

			response, _, err := authClient.ClientsApi.CreateClient(cmd.Context()).Body(formance.ClientOptions{
				Public:                 &public,
				RedirectUris:           fctl.GetStringSlice(cmd, redirectUriFlag),
				Description:            &description,
				Name:                   args[0],
				Trusted:                &trusted,
				PostLogoutRedirectUris: fctl.GetStringSlice(cmd, postLogoutRedirectUriFlag),
			}).Execute()
			if err != nil {
				return err
			}

			tableData := pterm.TableData{}
			tableData = append(tableData, []string{pterm.LightCyan("ID"), response.Data.Id})
			tableData = append(tableData, []string{pterm.LightCyan("Name"), response.Data.Name})
			tableData = append(tableData, []string{pterm.LightCyan("Description"), fctl.StringPointerToString(response.Data.Description)})
			tableData = append(tableData, []string{pterm.LightCyan("Public"), fctl.BoolPointerToString(response.Data.Public)})
			tableData = append(tableData, []string{pterm.LightCyan("Redirect URIs"), strings.Join(response.Data.RedirectUris, ",")})
			tableData = append(tableData, []string{pterm.LightCyan("Post logout redirect URIs"), strings.Join(response.Data.PostLogoutRedirectUris, ",")})
			return pterm.DefaultTable.
				WithWriter(cmd.OutOrStdout()).
				WithData(tableData).
				Render()
		}),
	)
}
