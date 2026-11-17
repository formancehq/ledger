package cmdutil

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

const (
	// resourcePrefix mirrors internal/controller.resourcePrefix (names.go), the
	// source of truth. The operator prefixes every object it creates with it, so
	// a Cluster's StatefulSet is named "ledger-<cr>", not "<cr>" (EN-1319).
	// Duplicated here because that const is unexported and importing the
	// controller package would pull controller-runtime into this CLI.
	resourcePrefix = "ledger-"
)

// GetCluster fetches a single Cluster CR.
func GetCluster(ctx context.Context, c client.Client, namespace, name string) (*ledgerv1alpha1.Cluster, error) {
	var ledger ledgerv1alpha1.Cluster
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &ledger); err != nil {
		return nil, err
	}

	return &ledger, nil
}

// ListClusters lists Cluster CRs. Pass empty namespace for all namespaces.
func ListClusters(ctx context.Context, c client.Client, namespace string) (*ledgerv1alpha1.ClusterList, error) {
	var list ledgerv1alpha1.ClusterList
	opts := []client.ListOption{}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if err := c.List(ctx, &list, opts...); err != nil {
		return nil, err
	}

	return &list, nil
}

// ClusterPods lists selected pods controlled by the Cluster's live StatefulSet.
func ClusterPods(ctx context.Context, cs kubernetes.Interface, ledger *ledgerv1alpha1.Cluster) (*corev1.PodList, error) {
	sts, err := clusterOwnedStatefulSet(ctx, cs, ledger)
	if err != nil {
		return nil, err
	}
	if sts == nil {
		return &corev1.PodList{}, nil
	}
	selector, err := metav1.LabelSelectorAsSelector(sts.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("reading StatefulSet %q selector: %w", sts.Name, err)
	}
	pods, err := cs.CoreV1().Pods(ledger.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return nil, err
	}
	pods.Items = slices.DeleteFunc(pods.Items, func(pod corev1.Pod) bool {
		return !metav1.IsControlledBy(&pod, sts)
	})

	return pods, nil
}

// ClusterPVCs lists associated claims for display only, not as deletion authority.
func ClusterPVCs(ctx context.Context, cs kubernetes.Interface, ledger *ledgerv1alpha1.Cluster) (*corev1.PersistentVolumeClaimList, error) {
	sts, err := clusterOwnedStatefulSet(ctx, cs, ledger)
	if err != nil {
		return nil, err
	}
	stsName := resourcePrefix + ledger.Name
	volumeNames := []string{"wal", "data"}
	if sts != nil {
		stsName = sts.Name
		volumeNames = make([]string, 0, len(sts.Spec.VolumeClaimTemplates))
		for _, template := range sts.Spec.VolumeClaimTemplates {
			volumeNames = append(volumeNames, template.Name)
		}
	}
	pvcs, err := cs.CoreV1().PersistentVolumeClaims(ledger.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	pvcs.Items = slices.DeleteFunc(pvcs.Items, func(pvc corev1.PersistentVolumeClaim) bool {
		return !slices.ContainsFunc(volumeNames, func(volumeName string) bool {
			ordinal, found := strings.CutPrefix(pvc.Name, volumeName+"-"+stsName+"-")
			if !found {
				return false
			}
			value, err := strconv.ParseUint(ordinal, 10, 32)

			return err == nil && strconv.FormatUint(value, 10) == ordinal
		})
	})

	return pvcs, nil
}

func clusterOwnedStatefulSet(ctx context.Context, cs kubernetes.Interface, ledger *ledgerv1alpha1.Cluster) (*appsv1.StatefulSet, error) {
	sts, err := ClusterStatefulSet(ctx, cs, ledger.Namespace, ledger.Name)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting StatefulSet for Cluster %q: %w", ledger.Name, err)
	}
	if !metav1.IsControlledBy(sts, ledger) {
		return nil, fmt.Errorf("StatefulSet %q is not controlled by Cluster %q", sts.Name, ledger.Name)
	}

	return sts, nil
}

// ClusterStatefulSet fetches the StatefulSet for a Cluster. The
// operator names it "ledger-<cr>" (resourcePrefix), not the bare CR name.
func ClusterStatefulSet(ctx context.Context, cs kubernetes.Interface, namespace, name string) (*appsv1.StatefulSet, error) {
	return cs.AppsV1().StatefulSets(namespace).Get(ctx, resourcePrefix+name, metav1.GetOptions{})
}

// ClusterPodName returns the name of the ordinal-th StatefulSet pod for a
// Cluster. The operator names pods "ledger-<cr>-<ordinal>" (resourcePrefix),
// not "<cr>-<ordinal>".
func ClusterPodName(name string, ordinal int) string {
	return fmt.Sprintf("%s%s-%d", resourcePrefix, name, ordinal)
}

// ClusterServices lists services controlled by the Cluster, independent of labels.
func ClusterServices(ctx context.Context, cs kubernetes.Interface, ledger *ledgerv1alpha1.Cluster) (*corev1.ServiceList, error) {
	services, err := cs.CoreV1().Services(ledger.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	services.Items = slices.DeleteFunc(services.Items, func(service corev1.Service) bool {
		return !metav1.IsControlledBy(&service, ledger)
	})

	return services, nil
}

// GetBackup fetches a single Backup CR.
func GetBackup(ctx context.Context, c client.Client, namespace, name string) (*ledgerv1alpha1.Backup, error) {
	var backup ledgerv1alpha1.Backup
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &backup); err != nil {
		return nil, err
	}

	return &backup, nil
}

// ListBackups lists Backup CRs in a namespace.
func ListBackups(ctx context.Context, c client.Client, namespace string) (*ledgerv1alpha1.BackupList, error) {
	var list ledgerv1alpha1.BackupList
	opts := []client.ListOption{}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if err := c.List(ctx, &list, opts...); err != nil {
		return nil, err
	}

	return &list, nil
}

// ListBackupRuns lists BackupRun CRs in a namespace, optionally filtered
// by parent Backup name (matches the LabelBackup label). Pass an empty
// backupName to list all runs in the namespace.
func ListBackupRuns(ctx context.Context, c client.Client, namespace, backupName string) (*ledgerv1alpha1.BackupRunList, error) {
	var list ledgerv1alpha1.BackupRunList
	opts := []client.ListOption{}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if backupName != "" {
		opts = append(opts, client.MatchingLabels{ledgerv1alpha1.LabelBackup: backupName})
	}
	if err := c.List(ctx, &list, opts...); err != nil {
		return nil, err
	}

	return &list, nil
}

// GetCredentials fetches a single cluster-scoped Credentials CR.
func GetCredentials(ctx context.Context, c client.Client, name string) (*ledgerv1alpha1.Credentials, error) {
	var credentials ledgerv1alpha1.Credentials
	if err := c.Get(ctx, types.NamespacedName{Name: name}, &credentials); err != nil {
		return nil, err
	}

	return &credentials, nil
}

// ListCredentials lists all cluster-scoped Credentials CRs.
func ListCredentials(ctx context.Context, c client.Client) (*ledgerv1alpha1.CredentialsList, error) {
	var list ledgerv1alpha1.CredentialsList
	if err := c.List(ctx, &list); err != nil {
		return nil, err
	}

	return &list, nil
}

// PodReadyCount returns a "ready/total" string for a pod's containers.
func PodReadyCount(p *corev1.Pod) string {
	ready := 0
	total := len(p.Spec.Containers)
	for _, cs := range p.Status.ContainerStatuses {
		if cs.Ready {
			ready++
		}
	}

	return fmt.Sprintf("%d/%d", ready, total)
}

// PodRestarts returns the total restart count across all containers.
func PodRestarts(p *corev1.Pod) int32 {
	var restarts int32
	for _, cs := range p.Status.ContainerStatuses {
		restarts += cs.RestartCount
	}

	return restarts
}
