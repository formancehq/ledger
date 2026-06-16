package auth

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
)

// NewStatusCommand returns the "auth status" command.
func NewStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "status",
		Aliases: []string{"whoami"},
		Short:   "Show current authentication status",
		Long: `Display the current authentication status including token source
(flag, environment, keychain, or none) and decoded JWT claims.`,
		Args:              cobra.ExactArgs(0),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE:              runStatus,
	}
}

func runStatus(cmd *cobra.Command, _ []string) error {
	server, _ := cmd.Flags().GetString("server")
	source, token := cmdutil.ResolveTokenSource(cmd)

	pterm.DefaultSection.Println("Authentication Status")

	rows := [][]string{
		{"Server", server},
		{"Token Source", source},
	}

	if token == "" {
		rows = append(rows, []string{"Status", pterm.Yellow("not authenticated")})
		data := append([][]string{{"Field", "Value"}}, rows...)

		return pterm.DefaultTable.WithHasHeader().WithData(data).Render()
	}

	parser := jwt.NewParser()
	claims := jwt.MapClaims{}

	jwtToken, _, err := parser.ParseUnverified(token, claims)
	if err != nil {
		rows = append(rows, []string{"Status", pterm.Red("invalid token: " + err.Error())})
		data := append([][]string{{"Field", "Value"}}, rows...)

		return pterm.DefaultTable.WithHasHeader().WithData(data).Render()
	}

	if sub, _ := claims.GetSubject(); sub != "" {
		rows = append(rows, []string{"Subject", sub})
	}

	if iss, _ := claims.GetIssuer(); iss != "" {
		rows = append(rows, []string{"Issuer", iss})
	}

	// Extract key ID from JWT header.
	if kid, ok := jwtToken.Header["kid"].(string); ok && kid != "" {
		rows = append(rows, []string{"Key ID", kid})
	}

	if scopes, ok := claims["scope"].(string); ok && scopes != "" {
		rows = append(rows, []string{"Scopes", scopes})
	}

	if iat, _ := claims.GetIssuedAt(); iat != nil {
		rows = append(rows, []string{"Issued At", iat.Format(time.RFC3339)})
	}

	if exp, _ := claims.GetExpirationTime(); exp != nil {
		remaining := time.Until(exp.Time)

		var status string
		if remaining <= 0 {
			status = fmt.Sprintf("%s (%s)", exp.Format(time.RFC3339), pterm.Red("EXPIRED"))
		} else {
			status = fmt.Sprintf("%s (%s, %s remaining)", exp.Format(time.RFC3339), pterm.Green("valid"), remaining.Truncate(time.Second))
		}

		rows = append(rows, []string{"Expires", status})
	}

	data := append([][]string{{"Field", "Value"}}, rows...)

	return pterm.DefaultTable.WithHasHeader().WithData(data).Render()
}
