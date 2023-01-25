package stack

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/formancehq/fctl/cmd/stack/internal"
	"github.com/formancehq/fctl/membershipclient"
	fctl "github.com/formancehq/fctl/pkg"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func NewCreateCommand() *cobra.Command {
	const (
		productionFlag = "production"
		unprotectFlag  = "unprotect"
		tagFlag        = "tag"
		nowaitFlag     = "no-wait"
	)
	return fctl.NewMembershipCommand("create <name>",
		fctl.WithShortDescription("Create a new stack"),
		fctl.WithAliases("c", "cr"),
		fctl.WithArgs(cobra.ExactArgs(1)),
		fctl.WithBoolFlag(productionFlag, false, "Create a production stack"),
		fctl.WithBoolFlag(unprotectFlag, false, "Unprotect stacks (no confirmation on write commands)"),
		fctl.WithStringSliceFlag(tagFlag, []string{}, "Tags to use to find matching region"),
		fctl.WithBoolFlag(nowaitFlag, false, "Not wait stack availability"),
		fctl.WithRunE(func(cmd *cobra.Command, args []string) error {

			cfg, err := fctl.GetConfig(cmd)
			if err != nil {
				return err
			}

			organization, err := fctl.ResolveOrganizationID(cmd, cfg)
			if err != nil {
				return err
			}

			apiClient, err := fctl.NewMembershipClient(cmd, cfg)
			if err != nil {
				return err
			}

			production := fctl.GetBool(cmd, productionFlag)
			protected := !fctl.GetBool(cmd, unprotectFlag)
			metadata := map[string]string{
				fctl.ProtectedStackMetadata: fctl.BoolPointerToString(&protected),
			}
			tags := make(map[string]string)
			for _, tagFlagValue := range fctl.GetStringSlice(cmd, tagFlag) {
				parts := strings.SplitN(tagFlagValue, "=", 2)
				if len(parts) < 2 {
					return errors.New("malformed flag --tag")
				}
				tags[parts[0]] = parts[1]
			}
			stack, _, err := apiClient.DefaultApi.CreateStack(cmd.Context(), organization).Body(membershipclient.StackData{
				Name:       args[0],
				Production: production,
				Metadata:   metadata,
				Tags:       tags,
			}).Execute()
			if err != nil {
				return fctl.WrapError(err, "creating stack")
			}

			profile := fctl.GetCurrentProfile(cmd, cfg)

			if !fctl.GetBool(cmd, nowaitFlag) {
				spinner, err := pterm.DefaultSpinner.Start("Waiting services availability")
				if err != nil {
					return err
				}

				if err := waitStackReady(cmd, profile, stack.Data); err != nil {
					return err
				}

				if err := spinner.Stop(); err != nil {
					return err
				}
			}

			fctl.Highlightln(cmd.OutOrStdout(), "Your dashboard will be reachable on: %s",
				profile.ServicesBaseUrl(stack.Data).String())

			return internal.PrintStackInformation(cmd.OutOrStdout(), profile, stack.Data)
		}),
	)
}

func waitStackReady(cmd *cobra.Command, profile *fctl.Profile, stack *membershipclient.Stack) error {
	baseUrlStr := profile.ServicesBaseUrl(stack).String()
	authServerUrl := fmt.Sprintf("%s/api/auth", baseUrlStr)
	for {
		req, err := http.NewRequestWithContext(cmd.Context(), http.MethodGet,
			fmt.Sprintf(authServerUrl+"/.well-known/openid-configuration"), nil)
		if err != nil {
			return err
		}
		rsp, err := fctl.GetHttpClient(cmd).Do(req)
		if err == nil && rsp.StatusCode == http.StatusOK {
			break
		}
		select {
		case <-cmd.Context().Done():
			return cmd.Context().Err()
		case <-time.After(time.Second):
		}
	}
	return nil
}
