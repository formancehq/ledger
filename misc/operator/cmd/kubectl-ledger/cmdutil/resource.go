package cmdutil

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

const (
	LabelName     = "app.kubernetes.io/name"
	LabelInstance = "app.kubernetes.io/instance"
	LabelValue    = "ledger"

	// resourcePrefix mirrors internal/controller.resourcePrefix (names.go), the
	// source of truth. Unless spec.resourceName overrides the base name, the
	// operator prefixes every object it creates with it, so a Cluster's
	// StatefulSet is named "ledger-<cr>", not "<cr>" (EN-1319). Duplicated
	// here because that const is unexported and importing the controller
	// package would pull controller-runtime into this CLI.
	resourcePrefix = "ledger-"
)

// LabelSelector returns a comma-separated label selector for the given Cluster name.
func LabelSelector(name string) string {
	return fmt.Sprintf("%s=%s,%s=%s", LabelName, LabelValue, LabelInstance, name)
}

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

// ClusterPVCs lists the PVCs named after a Cluster's volume claim templates:
// "<wal|data>-<base>-<ordinal>" (base = ClusterBaseName), for any ordinal,
// including claims kept from removed replicas. It selects by derived name, not
// by the app.kubernetes.io/instance label, which spec.additionalLabels can
// override. A derived name is not proof of ownership either: a claim retained
// from another Cluster under the same base name is listed too, as it is the
// one this Cluster's StatefulSet mounts for that ordinal.
func ClusterPVCs(ctx context.Context, cs kubernetes.Interface, ledger *ledgerv1alpha1.Cluster) (*corev1.PersistentVolumeClaimList, error) {
	list, err := cs.CoreV1().PersistentVolumeClaims(ledger.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	derived := regexp.MustCompile(`^(wal|data)-` + regexp.QuoteMeta(ClusterBaseName(ledger)) + `-(0|[1-9][0-9]*)$`)
	items := list.Items[:0]
	for _, pvc := range list.Items {
		if derived.MatchString(pvc.Name) {
			items = append(items, pvc)
		}
	}
	list.Items = items

	return list, nil
}

// ClusterBaseName returns the base name of every object the operator derives
// from a Cluster: spec.resourceName when set, otherwise "ledger-<cr>". It
// mirrors internal/controller.clusterBaseName (names.go), the source of truth.
func ClusterBaseName(ledger *ledgerv1alpha1.Cluster) string {
	if ledger.Spec.ResourceName != "" {
		return ledger.Spec.ResourceName
	}

	return resourcePrefix + ledger.Name
}

// ErrNotOwned signals that an object found under a name derived from a
// Cluster is not controlled by that Cluster: the StatefulSet "<base>" is not
// controlled by the Cluster's UID, or a pod is not controlled by that
// StatefulSet's UID. Two Clusters can share a base name through
// spec.resourceName (the second one reports ResourceNamesAvailable=False), and
// a pod "<base>-<ordinal>" can still belong to a deleted Cluster's runtime, so
// a name match never proves ownership. It mirrors the operator's guard
// (internal/controller fetchClusterStatefulSet and requirePodControlledBy).
var ErrNotOwned = errors.New("not controlled by the Cluster")

// ClusterStatefulSet fetches the StatefulSet named after the Cluster base name
// (see ClusterBaseName) and returns it only when ledger, as read from the API
// server (with its UID), controls it; otherwise the error wraps ErrNotOwned.
// A missing StatefulSet is returned as the API NotFound error.
func ClusterStatefulSet(ctx context.Context, cs kubernetes.Interface, ledger *ledgerv1alpha1.Cluster) (*appsv1.StatefulSet, error) {
	name := ClusterBaseName(ledger)
	sts, err := cs.AppsV1().StatefulSets(ledger.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("getting StatefulSet %s/%s of Cluster %q: %w", ledger.Namespace, name, ledger.Name, err)
	}
	if !metav1.IsControlledBy(sts, ledger) {
		return nil, fmt.Errorf("%w: StatefulSet %s/%s %s, not by Cluster %q (uid %s); "+
			"its base name %q conflicts with another owner (see the Cluster's ResourceNamesAvailable condition), refusing to access its pods",
			ErrNotOwned, sts.Namespace, sts.Name, describeController(sts), ledger.Name, ledger.UID, name)
	}

	return sts, nil
}

// ClusterPod returns the pod "<base>-<ordinal>" only when it is controlled by
// the StatefulSet ledger controls (ClusterStatefulSet); otherwise the error
// wraps ErrNotOwned. Read the pod right before using it: a pod replaced after
// this check (port-forward and logs have no UID precondition) is not detected.
func ClusterPod(ctx context.Context, cs kubernetes.Interface, ledger *ledgerv1alpha1.Cluster, ordinal int) (*corev1.Pod, error) {
	sts, err := ClusterStatefulSet(ctx, cs, ledger)
	if err != nil {
		return nil, err
	}
	name := ClusterPodName(ledger, ordinal)
	pod, err := cs.CoreV1().Pods(ledger.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("getting pod %s/%s of Cluster %q: %w", ledger.Namespace, name, ledger.Name, err)
	}
	if !metav1.IsControlledBy(pod, sts) {
		return nil, fmt.Errorf("%w: pod %s/%s %s, not by StatefulSet %s (uid %s) of Cluster %q; refusing to access it",
			ErrNotOwned, pod.Namespace, pod.Name, describeController(pod), sts.Name, sts.UID, ledger.Name)
	}

	return pod, nil
}

// ClusterPods lists the pods controlled by the StatefulSet ledger controls
// (ClusterStatefulSet), selected by that StatefulSet's selector: a pod that
// matches the labels but has another controller is left out.
func ClusterPods(ctx context.Context, cs kubernetes.Interface, ledger *ledgerv1alpha1.Cluster) ([]corev1.Pod, error) {
	sts, err := ClusterStatefulSet(ctx, cs, ledger)
	if err != nil {
		return nil, err
	}
	selector, err := metav1.LabelSelectorAsSelector(sts.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("parsing selector of StatefulSet %s/%s: %w", sts.Namespace, sts.Name, err)
	}
	if selector.Empty() {
		return nil, fmt.Errorf("StatefulSet %s/%s has an empty selector", sts.Namespace, sts.Name)
	}
	list, err := cs.CoreV1().Pods(ledger.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return nil, fmt.Errorf("listing pods of StatefulSet %s/%s: %w", sts.Namespace, sts.Name, err)
	}
	var pods []corev1.Pod
	for i := range list.Items {
		if metav1.IsControlledBy(&list.Items[i], sts) {
			pods = append(pods, list.Items[i])
		}
	}

	return pods, nil
}

// describeController renders who controls obj, for ErrNotOwned messages.
func describeController(obj metav1.Object) string {
	ref := metav1.GetControllerOf(obj)
	if ref == nil {
		return "has no controller"
	}

	return fmt.Sprintf("is controlled by %s/%s (uid %s)", ref.Kind, ref.Name, ref.UID)
}

// ClusterPodName returns the name of the ordinal-th StatefulSet pod for a
// Cluster: "<base>-<ordinal>" with base = ClusterBaseName, not "<cr>-<ordinal>".
// The name alone proves nothing; access the pod through ClusterPod.
func ClusterPodName(ledger *ledgerv1alpha1.Cluster, ordinal int) string {
	return fmt.Sprintf("%s-%d", ClusterBaseName(ledger), ordinal)
}

// Clusters lists services matching the selector labels for a Cluster.
func Clusters(ctx context.Context, cs kubernetes.Interface, namespace, name string) (*corev1.ServiceList, error) {
	return cs.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: LabelSelector(name),
	})
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

// GetBackupRun fetches a single BackupRun CR.
func GetBackupRun(ctx context.Context, c client.Client, namespace, name string) (*ledgerv1alpha1.BackupRun, error) {
	var run ledgerv1alpha1.BackupRun
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &run); err != nil {
		return nil, err
	}

	return &run, nil
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
