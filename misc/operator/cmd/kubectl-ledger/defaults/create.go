package defaults

import (
	"fmt"

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
	image               string
	tag                 string
	cpu                 string
	memory              string
	tls                 bool
	monitoring          bool
	monitoringEndpoint  string
	pebbleMemTableSize  string
	pebbleCacheSize     string
	pdbEnabled          bool
	podAntiAffinity     bool
	podAntiAffinityType string
	serviceMonitor      bool
	dryRun              bool
}

func newCreateCommand(opts *cmdutil.Options) *cobra.Command {
	var f createFlags

	cmd := &cobra.Command{
		Use:   "create [name]",
		Short: "Create a new LedgerDefaults resource",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCreate(cmd, opts, &f, args)
		},
	}

	cmd.Flags().StringVar(&f.image, "image", "", "Container image repository")
	cmd.Flags().StringVar(&f.tag, "tag", "", "Container image tag")
	cmd.Flags().StringVar(&f.cpu, "cpu", "", "CPU resource request (e.g. 2000m)")
	cmd.Flags().StringVar(&f.memory, "memory", "", "Memory resource request (e.g. 4Gi)")
	cmd.Flags().BoolVar(&f.tls, "tls", false, "Enable TLS in defaults")
	cmd.Flags().BoolVar(&f.monitoring, "monitoring", false, "Enable OpenTelemetry monitoring")
	cmd.Flags().StringVar(&f.monitoringEndpoint, "monitoring-endpoint", "", "OTLP endpoint for traces and metrics")
	cmd.Flags().StringVar(&f.pebbleMemTableSize, "pebble-memtable-size", "", "Pebble MemTable size in bytes (e.g. 268435456)")
	cmd.Flags().StringVar(&f.pebbleCacheSize, "pebble-cache-size", "", "Pebble block cache size in bytes (e.g. 1073741824)")
	cmd.Flags().BoolVar(&f.pdbEnabled, "pdb", false, "Enable PodDisruptionBudget")
	cmd.Flags().BoolVar(&f.podAntiAffinity, "pod-anti-affinity", false, "Enable pod anti-affinity")
	cmd.Flags().StringVar(&f.podAntiAffinityType, "pod-anti-affinity-type", "soft", "Pod anti-affinity type: soft or hard")
	cmd.Flags().BoolVar(&f.serviceMonitor, "service-monitor", false, "Enable Prometheus ServiceMonitor")
	cmd.Flags().BoolVar(&f.dryRun, "dry-run", false, "Print YAML without applying")

	return cmd
}

func runCreate(cmd *cobra.Command, opts *cmdutil.Options, f *createFlags, args []string) error {
	name, err := resolveDefaultsName(args)
	if err != nil {
		return err
	}

	// Interactive wizard for fields not set via flags.
	if err := runCreateWizard(cmd, f); err != nil {
		return err
	}

	defaults, err := buildDefaults(name, f)
	if err != nil {
		return err
	}

	// Show preview.
	pterm.Println()
	pterm.DefaultSection.Println("Create LedgerDefaults Preview")
	previewRows := buildPreviewRows(name, f)
	cmdutil.RenderBoxedTable(previewRows)
	pterm.Println()

	if f.dryRun {
		b, err := yaml.Marshal(defaults)
		if err != nil {
			return fmt.Errorf("marshaling YAML: %w", err)
		}
		pterm.Info.Println("Dry run - YAML output:")
		pterm.Println()
		fmt.Print(string(b))
		return nil
	}

	confirm, err := cmdutil.PromptConfirm("Create this LedgerDefaults?", true)
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

	spinner, _ := pterm.DefaultSpinner.Start("Creating LedgerDefaults...")

	if err := crdClient.Create(cmd.Context(), defaults); err != nil {
		spinner.Fail("Failed to create LedgerDefaults")
		return fmt.Errorf("creating ledger defaults %q: %w", name, err)
	}

	spinner.Success(fmt.Sprintf("LedgerDefaults %s created", pterm.Cyan(name)))
	return nil
}

func runCreateWizard(cmd *cobra.Command, f *createFlags) error {
	// --- Image ---
	if !cmd.Flags().Changed("image") {
		image, err := cmdutil.PromptText("Image repository (leave empty to skip)", "")
		if err != nil {
			return err
		}
		f.image = image
	}

	if f.image != "" && !cmd.Flags().Changed("tag") {
		tag, err := cmdutil.PromptText("Image tag", "latest")
		if err != nil {
			return err
		}
		f.tag = tag
	}

	// --- Resources ---
	if !cmd.Flags().Changed("cpu") {
		cpu, err := cmdutil.PromptText("CPU request (leave empty to skip)", "")
		if err != nil {
			return err
		}
		f.cpu = cpu
	}

	if !cmd.Flags().Changed("memory") {
		memory, err := cmdutil.PromptText("Memory request (leave empty to skip)", "")
		if err != nil {
			return err
		}
		f.memory = memory
	}

	// --- TLS ---
	if !cmd.Flags().Changed("tls") {
		tls, err := cmdutil.PromptConfirm("Enable TLS?", false)
		if err != nil {
			return err
		}
		f.tls = tls
	}

	// --- Monitoring ---
	if !cmd.Flags().Changed("monitoring") {
		monitoring, err := cmdutil.PromptConfirm("Enable OpenTelemetry monitoring?", false)
		if err != nil {
			return err
		}
		f.monitoring = monitoring
	}

	if f.monitoring && !cmd.Flags().Changed("monitoring-endpoint") {
		endpoint, err := cmdutil.PromptText("OTLP endpoint (e.g. otel-collector:4317)", "")
		if err != nil {
			return err
		}
		f.monitoringEndpoint = endpoint
	}

	// --- Pebble ---
	if !cmd.Flags().Changed("pebble-memtable-size") {
		memTable, err := cmdutil.PromptText("Pebble MemTable size in bytes (leave empty to skip)", "")
		if err != nil {
			return err
		}
		f.pebbleMemTableSize = memTable
	}

	if !cmd.Flags().Changed("pebble-cache-size") {
		cache, err := cmdutil.PromptText("Pebble cache size in bytes (leave empty to skip)", "")
		if err != nil {
			return err
		}
		f.pebbleCacheSize = cache
	}

	// --- PDB ---
	if !cmd.Flags().Changed("pdb") {
		pdb, err := cmdutil.PromptConfirm("Enable PodDisruptionBudget?", false)
		if err != nil {
			return err
		}
		f.pdbEnabled = pdb
	}

	// --- Pod Anti-Affinity ---
	if !cmd.Flags().Changed("pod-anti-affinity") {
		antiAffinity, err := cmdutil.PromptConfirm("Enable pod anti-affinity?", true)
		if err != nil {
			return err
		}
		f.podAntiAffinity = antiAffinity
	}

	if f.podAntiAffinity && !cmd.Flags().Changed("pod-anti-affinity-type") {
		selected, err := pterm.DefaultInteractiveSelect.
			WithOptions([]string{"soft", "hard"}).
			WithDefaultText("Pod anti-affinity type").
			WithDefaultOption("soft").
			Show()
		if err != nil {
			return fmt.Errorf("failed to select anti-affinity type: %w", err)
		}
		f.podAntiAffinityType = selected
	}

	// --- Service Monitor ---
	if !cmd.Flags().Changed("service-monitor") {
		sm, err := cmdutil.PromptConfirm("Enable Prometheus ServiceMonitor?", false)
		if err != nil {
			return err
		}
		f.serviceMonitor = sm
	}

	return nil
}

func buildPreviewRows(name string, f *createFlags) [][]string {
	rows := [][]string{
		{"Name", pterm.Cyan(name)},
		{"Scope", "Cluster"},
	}
	if f.image != "" {
		tag := f.tag
		if tag == "" {
			tag = "latest"
		}
		rows = append(rows, []string{"Image", f.image + ":" + tag})
	}
	if f.cpu != "" || f.memory != "" {
		rows = append(rows, []string{"Resources", fmt.Sprintf("cpu=%s memory=%s", f.cpu, f.memory)})
	}
	if f.tls {
		rows = append(rows, []string{"TLS", pterm.Green("enabled")})
	}
	if f.monitoring {
		detail := "enabled"
		if f.monitoringEndpoint != "" {
			detail += " (endpoint: " + f.monitoringEndpoint + ")"
		}
		rows = append(rows, []string{"Monitoring", pterm.Green(detail)})
	}
	if f.pebbleMemTableSize != "" || f.pebbleCacheSize != "" {
		detail := ""
		if f.pebbleMemTableSize != "" {
			detail += "memTable=" + f.pebbleMemTableSize
		}
		if f.pebbleCacheSize != "" {
			if detail != "" {
				detail += " "
			}
			detail += "cache=" + f.pebbleCacheSize
		}
		rows = append(rows, []string{"Pebble", detail})
	}
	if f.pdbEnabled {
		rows = append(rows, []string{"PDB", pterm.Green("enabled")})
	}
	if f.podAntiAffinity {
		rows = append(rows, []string{"Pod Anti-Affinity", pterm.Green(f.podAntiAffinityType)})
	}
	if f.serviceMonitor {
		rows = append(rows, []string{"Service Monitor", pterm.Green("enabled")})
	}
	return rows
}

func resolveDefaultsName(args []string) (string, error) {
	if len(args) > 0 {
		return args[0], nil
	}

	name, err := cmdutil.PromptText("LedgerDefaults name", "")
	if err != nil {
		return "", err
	}
	if name == "" {
		return "", fmt.Errorf("name is required")
	}
	return name, nil
}

func buildDefaults(name string, f *createFlags) (*ledgerv1alpha1.LedgerDefaults, error) {
	defaults := &ledgerv1alpha1.LedgerDefaults{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "ledger.formance.com/v1alpha1",
			Kind:       "LedgerDefaults",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}

	if f.image != "" {
		defaults.Spec.Image.Repository = f.image
		if f.tag != "" {
			defaults.Spec.Image.Tag = f.tag
		}
	}

	if f.cpu != "" || f.memory != "" {
		defaults.Spec.Resources.Requests = corev1.ResourceList{}
		defaults.Spec.Resources.Limits = corev1.ResourceList{}
		if f.cpu != "" {
			q, err := resource.ParseQuantity(f.cpu)
			if err != nil {
				return nil, fmt.Errorf("invalid cpu %q: %w", f.cpu, err)
			}
			defaults.Spec.Resources.Requests[corev1.ResourceCPU] = q
			defaults.Spec.Resources.Limits[corev1.ResourceCPU] = q
		}
		if f.memory != "" {
			q, err := resource.ParseQuantity(f.memory)
			if err != nil {
				return nil, fmt.Errorf("invalid memory %q: %w", f.memory, err)
			}
			defaults.Spec.Resources.Requests[corev1.ResourceMemory] = q
			defaults.Spec.Resources.Limits[corev1.ResourceMemory] = q
		}
	}

	if f.tls {
		defaults.Spec.Config.TLS = &ledgerv1alpha1.TLSConfig{
			Enabled: true,
		}
	}

	if f.monitoring {
		trueVal := true
		defaults.Spec.Config.Monitoring = &ledgerv1alpha1.MonitoringConfig{
			Traces: &ledgerv1alpha1.TracesConfig{
				Enabled: &trueVal,
			},
			Metrics: &ledgerv1alpha1.MetricsConfig{
				Enabled: &trueVal,
			},
		}
		if f.monitoringEndpoint != "" {
			defaults.Spec.Config.Monitoring.Traces.Endpoint = f.monitoringEndpoint
			defaults.Spec.Config.Monitoring.Traces.Exporter = "otlp"
			defaults.Spec.Config.Monitoring.Traces.Mode = "grpc"
			defaults.Spec.Config.Monitoring.Metrics.Endpoint = f.monitoringEndpoint
			defaults.Spec.Config.Monitoring.Metrics.Exporter = "otlp"
			defaults.Spec.Config.Monitoring.Metrics.Mode = "grpc"
		}
	}

	if f.pebbleMemTableSize != "" || f.pebbleCacheSize != "" {
		defaults.Spec.Config.Pebble = &ledgerv1alpha1.PebbleConfig{}
		if f.pebbleMemTableSize != "" {
			val, err := parseByteSize(f.pebbleMemTableSize)
			if err != nil {
				return nil, fmt.Errorf("invalid pebble-memtable-size %q: %w", f.pebbleMemTableSize, err)
			}
			defaults.Spec.Config.Pebble.MemTableSize = &val
		}
		if f.pebbleCacheSize != "" {
			val, err := parseByteSize(f.pebbleCacheSize)
			if err != nil {
				return nil, fmt.Errorf("invalid pebble-cache-size %q: %w", f.pebbleCacheSize, err)
			}
			defaults.Spec.Config.Pebble.CacheSize = &val
		}
	}

	if f.pdbEnabled {
		one := int32(1)
		defaults.Spec.PodDisruptionBudget = &ledgerv1alpha1.PodDisruptionBudgetSpec{
			Enabled:        true,
			MaxUnavailable: &one,
		}
	}

	if f.podAntiAffinity {
		defaults.Spec.PodAntiAffinity = &ledgerv1alpha1.PodAntiAffinitySpec{
			Enabled: true,
			Type:    f.podAntiAffinityType,
		}
	}

	if f.serviceMonitor {
		defaults.Spec.ServiceMonitor = &ledgerv1alpha1.ServiceMonitorSpec{
			Enabled: true,
		}
	}

	return defaults, nil
}

func parseByteSize(s string) (int64, error) {
	// Accept raw integer byte counts.
	var n int64
	_, err := fmt.Sscanf(s, "%d", &n)
	if err != nil {
		return 0, fmt.Errorf("expected integer byte count: %w", err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("must be a positive integer, got %d", n)
	}
	return n, nil
}
