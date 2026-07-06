package credentials

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
	"github.com/formance/ledger/operator/cmd/kubectl-ledger/cmdutil"
)

func newGetCommand(opts *cmdutil.Options) *cobra.Command {
	return &cobra.Command{
		Use:   "get [name]",
		Short: "Show details of a Credentials resource",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGet(cmd, opts, args)
		},
	}
}

func runGet(cmd *cobra.Command, opts *cmdutil.Options, args []string) error {
	ctx := cmd.Context()

	name, err := cmdutil.ResolveCredentialsName(ctx, opts, args)
	if err != nil {
		return err
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	credentials, err := cmdutil.GetCredentials(ctx, crdClient, name)
	if err != nil {
		return fmt.Errorf("getting credentials %q: %w", name, err)
	}

	switch opts.OutputFormat() {
	case "json":
		return cmdutil.OutputJSON(credentials)
	case "yaml":
		return cmdutil.OutputYAML(credentials)
	default:
		return renderAgentDetails(credentials)
	}
}

func renderAgentDetails(credentials *ledgerv1alpha1.Credentials) error {
	pterm.Println()

	// Overview section
	phase := cmdutil.PhaseColor(credentialsPhaseColor(credentials.Status.Phase))
	keyID := credentials.Status.KeyID
	if keyID == "" {
		keyID = pterm.Gray("<pending>")
	}
	secretRef := pterm.Gray("<none>")
	if n := len(credentials.Status.DistributedSecretRefs); n > 0 {
		first := credentials.Status.DistributedSecretRefs[0]
		if n == 1 {
			secretRef = fmt.Sprintf("%s/%s", first.Namespace, first.Name)
		} else {
			secretRef = fmt.Sprintf("%s/%s (+%d more)", first.Namespace, first.Name, n-1)
		}
	}
	scopes := strings.Join(credentials.Spec.Scopes, ", ")
	if scopes == "" {
		scopes = pterm.Gray("<none>")
	}

	pterm.DefaultSection.Println("Overview")
	cmdutil.RenderTable(
		[]string{"FIELD", "VALUE"},
		[][]string{
			{"Name", pterm.Cyan(credentials.Name)},
			{"Phase", phase},
			{"Key ID", keyID},
			{"Secret", secretRef},
			{"Scopes", scopes},
			{"Matched Clusters", strconv.Itoa(len(credentials.Status.MatchedClusters))},
			{"Age", cmdutil.FormatAge(time.Since(credentials.CreationTimestamp.Time))},
		},
	)

	// Matched clusters section
	if len(credentials.Status.MatchedClusters) > 0 {
		pterm.DefaultSection.Println("Matched Clusters")
		svcRows := make([][]string, 0, len(credentials.Status.MatchedClusters))
		for i := range credentials.Status.MatchedClusters {
			ms := &credentials.Status.MatchedClusters[i]
			svcRows = append(svcRows, []string{ms.Namespace, ms.Name})
		}
		cmdutil.RenderTable([]string{"NAMESPACE", "NAME"}, svcRows)
	}

	// Conditions section
	if len(credentials.Status.Conditions) > 0 {
		pterm.DefaultSection.Println("Conditions")
		condRows := make([][]string, 0, len(credentials.Status.Conditions))
		for i := range credentials.Status.Conditions {
			c := &credentials.Status.Conditions[i]
			condRows = append(condRows, []string{
				c.Type,
				string(c.Status),
				c.Reason,
				c.Message,
				cmdutil.FormatAge(time.Since(c.LastTransitionTime.Time)),
			})
		}
		cmdutil.RenderTable([]string{"TYPE", "STATUS", "REASON", "MESSAGE", "AGE"}, condRows)
	}

	return nil
}
