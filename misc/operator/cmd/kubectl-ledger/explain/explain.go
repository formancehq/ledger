package explain

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"k8s.io/client-go/rest"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

// RESTConfigFunc is a function that returns a Kubernetes REST config.
// Used to avoid import cycles with cmdutil.
type RESTConfigFunc func() (*rest.Config, error)

// Field describes a single CRD field.
type Field struct {
	Name        string
	Type        string
	Required    bool
	Default     string
	Description string
	Enum        []string // Valid values for enum-constrained fields.
	Immutable   bool     // True if the field has an XValidation immutability rule.
	Children    []Field
}

var (
	specFieldsOnce   sync.Once
	cachedSpecFields []Field
)

// SpecFields returns the schema fields for the Cluster spec,
// built by reflecting on the Go types. No cluster access required.
// Used for flag registration at init time.
func SpecFields() []Field {
	specFieldsOnce.Do(func() {
		cachedSpecFields = fieldsFromType(reflect.TypeFor[ledgerv1alpha1.ClusterSpec]())
	})

	return cachedSpecFields
}

// LedgerClusterAgentSpecFields returns the schema fields for the LedgerClusterAgent spec.
func LedgerClusterAgentSpecFields() []Field {
	return fieldsFromType(reflect.TypeFor[ledgerv1alpha1.LedgerClusterAgentSpec]())
}

// Lookup finds a field by dotted path (e.g. "raft.electionTick").
func Lookup(fields []Field, path string) (Field, bool) {
	parts := strings.SplitN(path, ".", 2)
	for _, f := range fields {
		if f.Name == parts[0] {
			if len(parts) == 1 {
				return f, true
			}

			return Lookup(f.Children, parts[1])
		}
	}

	return Field{}, false
}

// NewCommand returns the "explain" command. It fetches the CRD schema
// from the cluster for full descriptions and defaults.
func NewCommand(restConfigFn RESTConfigFunc) *cobra.Command {
	return &cobra.Command{
		Use:     "explain [field.path]",
		Aliases: []string{"schema", "fields"},
		Short:   "Describe the Cluster CRD schema and fields",
		Long:    "Displays the Cluster CRD field hierarchy with types, defaults, and descriptions.\nFetches the schema from the cluster for full documentation.\nOptionally pass a dotted field path to show only that subtree (e.g. spec.raft.electionTick).",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runExplain(cmd, restConfigFn, args)
		},
	}
}

func runExplain(cmd *cobra.Command, restConfigFn RESTConfigFunc, args []string) error {
	var specFields, statusFields []Field

	restCfg, err := restConfigFn()
	if err == nil {
		specFields, err = FetchSpecFields(cmd.Context(), restCfg)
	}
	if err != nil {
		pterm.Warning.Printfln("Could not fetch CRD from cluster: %v", err)
		pterm.Warning.Println("Falling back to compiled schema (no descriptions).")
		pterm.Println()
		specFields = SpecFields()
	}

	if restCfg != nil {
		statusFields, _ = FetchStatusFields(cmd.Context(), restCfg)
	}
	if statusFields == nil {
		statusFields = fieldsFromType(reflect.TypeFor[ledgerv1alpha1.ClusterStatus]())
	}

	root := []Field{
		{Name: "spec", Type: "object", Description: "Desired state of the Cluster deployment.", Children: specFields},
		{Name: "status", Type: "object", Description: "Observed state (read-only, set by the operator).", Children: statusFields},
	}

	if len(args) > 0 {
		node, ok := Lookup(root, args[0])
		if !ok {
			return fmt.Errorf("unknown field path %q", args[0])
		}
		pterm.Println()
		pterm.Printf("%s %s\n", pterm.Bold.Sprint(pterm.Cyan(args[0])), pterm.Gray(node.Type))
		if node.Description != "" {
			pterm.Printf("  %s\n", node.Description)
		}
		if node.Default != "" {
			pterm.Printf("  Default: %s\n", pterm.Green(node.Default))
		}
		pterm.Println()
		printFields(node.Children, 0)

		return nil
	}

	pterm.Println()
	pterm.DefaultSection.Println("Cluster CRD — ledger.formance.com/v1alpha1")
	printFields(root, 0)

	return nil
}

func printFields(fields []Field, depth int) {
	indent := strings.Repeat("  ", depth)
	for _, f := range fields {
		req := ""
		if f.Required {
			req = pterm.Red(" *")
		}
		def := ""
		if f.Default != "" {
			def = pterm.Gray(fmt.Sprintf(" [%s]", f.Default))
		}

		pterm.Printf("%s%s%s  %s%s\n", indent, pterm.Cyan(f.Name), req, pterm.Gray(f.Type), def)
		if f.Description != "" {
			pterm.Printf("%s  %s\n", indent, f.Description)
		}

		if len(f.Children) > 0 {
			printFields(f.Children, depth+1)
		}
	}
}
