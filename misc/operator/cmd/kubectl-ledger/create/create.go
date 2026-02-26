package create

import (
	"context"
	"fmt"
	"sort"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

type createFlags struct {
	replicas    int32
	image       string
	tag         string
	walSize     string
	dataSize    string
	storageClass string
	clusterID   string
	cpu         string
	memory      string
	defaultsRef string
	dryRun      bool
}

// NewCommand returns the "create" command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	var f createFlags

	cmd := &cobra.Command{
		Use:   "create [name]",
		Short: "Create a new LedgerService deployment",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCreate(cmd, opts, &f, args)
		},
	}

	cmd.Flags().Int32Var(&f.replicas, "replicas", 3, "Number of Raft replicas (must be odd)")
	cmd.Flags().StringVar(&f.image, "image", "ghcr.io/formancehq/ledger-v3-poc", "Container image repository")
	cmd.Flags().StringVar(&f.tag, "tag", "latest", "Container image tag")
	cmd.Flags().StringVar(&f.walSize, "wal-size", "5Gi", "WAL volume size")
	cmd.Flags().StringVar(&f.dataSize, "data-size", "10Gi", "Data volume size")
	cmd.Flags().StringVar(&f.storageClass, "storage-class", "", "Storage class for PVCs")
	cmd.Flags().StringVar(&f.clusterID, "cluster-id", "default", "Cluster ID")
	cmd.Flags().StringVar(&f.cpu, "cpu", "", "CPU resource request (e.g. 500m)")
	cmd.Flags().StringVar(&f.memory, "memory", "", "Memory resource request (e.g. 512Mi)")
	cmd.Flags().StringVar(&f.defaultsRef, "defaults-ref", "", "Reference a cluster-scoped LedgerDefaults resource")
	cmd.Flags().BoolVar(&f.dryRun, "dry-run", false, "Print YAML without applying")

	return cmd
}

func runCreate(cmd *cobra.Command, opts *cmdutil.Options, f *createFlags, args []string) error {
	// Resolve name: from arg or interactive prompt
	name, err := resolveName(args)
	if err != nil {
		return err
	}

	ns, err := resolveNamespace(cmd, opts)
	if err != nil {
		return fmt.Errorf("resolving namespace: %w", err)
	}

	// Offer defaults-ref selection if not set via flag and defaults exist.
	if !cmd.Flags().Changed("defaults-ref") {
		crdClient, clientErr := opts.CRDClient()
		if clientErr == nil {
			defaults, listErr := cmdutil.ListLedgerDefaults(cmd.Context(), crdClient)
			if listErr == nil && len(defaults.Items) > 0 {
				names := []string{"(none)"}
				for i := range defaults.Items {
					names = append(names, defaults.Items[i].Name)
				}
				{
					selected, selErr := pterm.DefaultInteractiveSelect.
						WithOptions(names).
						WithDefaultText("Use a LedgerDefaults resource?").
						Show()
					if selErr == nil && selected != "(none)" {
						f.defaultsRef = selected
					}
				}
			}
		}
	}

	// Prompt for replicas if not explicitly set
	if !cmd.Flags().Changed("replicas") {
		replicas, err := cmdutil.PromptReplicas(f.replicas)
		if err != nil {
			return err
		}
		f.replicas = replicas
	}

	if err := cmdutil.ValidateReplicas(f.replicas); err != nil {
		return err
	}

	// Prompt for cluster ID if not explicitly set
	if !cmd.Flags().Changed("cluster-id") {
		clusterID, err := cmdutil.PromptText("Cluster ID", f.clusterID)
		if err != nil {
			return err
		}
		if clusterID != "" {
			f.clusterID = clusterID
		}
	}

	ledger, err := buildLedgerService(cmd, name, ns, f)
	if err != nil {
		return err
	}

	// Show preview
	pterm.Println()
	pterm.DefaultSection.Println("Create Preview")
	previewRows := [][]string{
		{"Name", pterm.Cyan(name)},
		{"Namespace", ns},
	}
	if f.defaultsRef != "" {
		previewRows = append(previewRows, []string{"Defaults", pterm.Cyan(f.defaultsRef)})
	}
	previewRows = append(previewRows, []string{"Replicas", fmt.Sprintf("%d", f.replicas)})
	if ledger.Spec.Image.Repository != "" || ledger.Spec.Image.Tag != "" {
		previewRows = append(previewRows, []string{"Image", cmdutil.FormatImage(ledger.Spec.Image)})
	} else {
		previewRows = append(previewRows, []string{"Image", pterm.Gray("<from defaults>")})
	}
	previewRows = append(previewRows,
		[]string{"Cluster ID", f.clusterID},
		[]string{"WAL Size", f.walSize},
		[]string{"Data Size", f.dataSize},
	)
	cmdutil.RenderBoxedTable(previewRows)
	pterm.Println()

	if f.dryRun {
		b, err := yaml.Marshal(ledger)
		if err != nil {
			return fmt.Errorf("marshaling YAML: %w", err)
		}
		pterm.Info.Println("Dry run - YAML output:")
		pterm.Println()
		fmt.Print(string(b))
		return nil
	}

	confirm, err := cmdutil.PromptConfirm("Create this LedgerService?", true)
	if err != nil {
		return err
	}
	if !confirm {
		pterm.Warning.Println("Aborted.")
		return nil
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	spinner, _ := pterm.DefaultSpinner.Start("Creating LedgerService...")

	if err := crdClient.Create(cmd.Context(), ledger); err != nil {
		spinner.Fail("Failed to create LedgerService")
		return fmt.Errorf("creating ledger %q: %w", name, err)
	}

	spinner.Success(fmt.Sprintf("LedgerService %s created in namespace %s", pterm.Cyan(name), pterm.Cyan(ns)))
	return nil
}

func resolveName(args []string) (string, error) {
	if len(args) > 0 {
		return args[0], nil
	}

	name, err := cmdutil.PromptText("LedgerService name", "")
	if err != nil {
		return "", err
	}
	if name == "" {
		return "", fmt.Errorf("ledger name is required")
	}
	return name, nil
}

// resolveNamespace returns the target namespace. If the user passed -n, use
// that directly. Otherwise, list cluster namespaces and let the user pick,
// with the current kubeconfig namespace pre-selected.
func resolveNamespace(cmd *cobra.Command, opts *cmdutil.Options) (string, error) {
	// Explicit -n flag: use it as-is, no prompt.
	if cmd.Flags().Changed("namespace") {
		return opts.ResolvedNamespace()
	}

	currentNS, err := opts.ResolvedNamespace()
	if err != nil {
		return "", err
	}

	namespaces, err := listNamespaces(cmd.Context(), opts)
	if err != nil {
		// Fall back to text prompt if we can't list namespaces.
		return cmdutil.PromptText("Namespace", currentNS)
	}

	selected, err := cmdutil.SelectPrompt("Namespace", namespaces)
	if err != nil {
		return "", fmt.Errorf("namespace selection failed: %w", err)
	}
	return selected, nil
}

func listNamespaces(ctx context.Context, opts *cmdutil.Options) ([]string, error) {
	clientset, err := opts.Clientset()
	if err != nil {
		return nil, err
	}

	nsList, err := clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(nsList.Items))
	for i := range nsList.Items {
		names = append(names, nsList.Items[i].Name)
	}
	sort.Strings(names)
	return names, nil
}

func buildLedgerService(cmd *cobra.Command, name, namespace string, f *createFlags) (*ledgerv1alpha1.LedgerService, error) {
	walSize, err := resource.ParseQuantity(f.walSize)
	if err != nil {
		return nil, fmt.Errorf("invalid wal-size %q: %w", f.walSize, err)
	}

	dataSize, err := resource.ParseQuantity(f.dataSize)
	if err != nil {
		return nil, fmt.Errorf("invalid data-size %q: %w", f.dataSize, err)
	}

	ledger := &ledgerv1alpha1.LedgerService{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "ledger.formance.com/v1alpha1",
			Kind:       "LedgerService",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: ledgerv1alpha1.LedgerServiceSpec{
			DefaultsRef: f.defaultsRef,
			Replicas:    &f.replicas,
			Config: ledgerv1alpha1.LedgerServiceConfig{
				ClusterID: f.clusterID,
			},
			Persistence: ledgerv1alpha1.PersistenceSpec{
				WAL:  ledgerv1alpha1.VolumeSpec{Size: walSize},
				Data: ledgerv1alpha1.VolumeSpec{Size: dataSize},
			},
		},
	}

	// Only set image fields if the user explicitly provided them via flags.
	// When defaultsRef is set, leaving these empty lets the controller
	// inherit them from LedgerDefaults.
	if cmd.Flags().Changed("image") || f.defaultsRef == "" {
		ledger.Spec.Image.Repository = f.image
	}
	if cmd.Flags().Changed("tag") || f.defaultsRef == "" {
		ledger.Spec.Image.Tag = f.tag
	}

	if f.storageClass != "" {
		ledger.Spec.Persistence.WAL.StorageClass = f.storageClass
		ledger.Spec.Persistence.Data.StorageClass = f.storageClass
	}

	if f.cpu != "" || f.memory != "" {
		resources := corev1.ResourceRequirements{
			Requests: corev1.ResourceList{},
		}
		if f.cpu != "" {
			q, err := resource.ParseQuantity(f.cpu)
			if err != nil {
				return nil, fmt.Errorf("invalid cpu %q: %w", f.cpu, err)
			}
			resources.Requests[corev1.ResourceCPU] = q
		}
		if f.memory != "" {
			q, err := resource.ParseQuantity(f.memory)
			if err != nil {
				return nil, fmt.Errorf("invalid memory %q: %w", f.memory, err)
			}
			resources.Requests[corev1.ResourceMemory] = q
		}
		ledger.Spec.Resources = resources
	}

	return ledger, nil
}
