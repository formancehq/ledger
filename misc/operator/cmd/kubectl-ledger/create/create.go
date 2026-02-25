package create

import (
	"fmt"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

type createFlags struct {
	replicas     int32
	image        string
	tag          string
	walSize      string
	dataSize     string
	storageClass string
	clusterID    string
	cpu          string
	memory       string
	dryRun       bool
}

// NewCommand returns the "create" command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	var f createFlags

	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a new Ledger deployment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCreate(cmd, opts, &f, args[0])
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
	cmd.Flags().BoolVar(&f.dryRun, "dry-run", false, "Print YAML without applying")

	return cmd
}

func runCreate(cmd *cobra.Command, opts *cmdutil.Options, f *createFlags, name string) error {
	if f.replicas%2 == 0 {
		return fmt.Errorf("replicas must be odd for Raft consensus, got %d", f.replicas)
	}

	ns, err := opts.ResolvedNamespace()
	if err != nil {
		return fmt.Errorf("resolving namespace: %w", err)
	}

	ledger, err := buildLedger(name, ns, f)
	if err != nil {
		return err
	}

	if f.dryRun {
		b, err := yaml.Marshal(ledger)
		if err != nil {
			return fmt.Errorf("marshaling YAML: %w", err)
		}
		fmt.Print(string(b))
		return nil
	}

	crdClient, err := opts.CRDClient()
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	if err := crdClient.Create(cmd.Context(), ledger); err != nil {
		return fmt.Errorf("creating ledger %q: %w", name, err)
	}

	fmt.Printf("Ledger %q created in namespace %q\n", name, ns)
	return nil
}

func buildLedger(name, namespace string, f *createFlags) (*ledgerv1alpha1.Ledger, error) {
	walSize, err := resource.ParseQuantity(f.walSize)
	if err != nil {
		return nil, fmt.Errorf("invalid wal-size %q: %w", f.walSize, err)
	}

	dataSize, err := resource.ParseQuantity(f.dataSize)
	if err != nil {
		return nil, fmt.Errorf("invalid data-size %q: %w", f.dataSize, err)
	}

	ledger := &ledgerv1alpha1.Ledger{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "ledger.formance.com/v1alpha1",
			Kind:       "Ledger",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: ledgerv1alpha1.LedgerSpec{
			Replicas: &f.replicas,
			Image: ledgerv1alpha1.ImageSpec{
				Repository: f.image,
				Tag:        f.tag,
			},
			Config: ledgerv1alpha1.LedgerConfig{
				ClusterID: f.clusterID,
			},
			Persistence: ledgerv1alpha1.PersistenceSpec{
				WAL:  ledgerv1alpha1.VolumeSpec{Size: walSize},
				Data: ledgerv1alpha1.VolumeSpec{Size: dataSize},
			},
		},
	}

	if f.storageClass != "" {
		ledger.Spec.Persistence.WAL.StorageClass = f.storageClass
		ledger.Spec.Persistence.Data.StorageClass = f.storageClass
	}

	resources := corev1.ResourceRequirements{}
	if f.cpu != "" || f.memory != "" {
		resources.Requests = corev1.ResourceList{}
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
