package cmdutil

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

const (
	LabelName     = "app.kubernetes.io/name"
	LabelInstance = "app.kubernetes.io/instance"
	LabelValue    = "ledger"

	// resourcePrefix mirrors internal/controller.resourcePrefix (names.go), the
	// source of truth. The operator prefixes every object it creates with it, so
	// a LedgerService's StatefulSet is named "ledger-<cr>", not "<cr>" (EN-1319).
	// Duplicated here because that const is unexported and importing the
	// controller package would pull controller-runtime into this CLI.
	resourcePrefix = "ledger-"
)

// LabelSelector returns a comma-separated label selector for the given LedgerService name.
func LabelSelector(name string) string {
	return fmt.Sprintf("%s=%s,%s=%s", LabelName, LabelValue, LabelInstance, name)
}

// GetLedgerService fetches a single LedgerService CR.
func GetLedgerService(ctx context.Context, c client.Client, namespace, name string) (*ledgerv1alpha1.LedgerService, error) {
	var ledger ledgerv1alpha1.LedgerService
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &ledger); err != nil {
		return nil, err
	}

	return &ledger, nil
}

// ListLedgerServices lists LedgerService CRs. Pass empty namespace for all namespaces.
func ListLedgerServices(ctx context.Context, c client.Client, namespace string) (*ledgerv1alpha1.LedgerServiceList, error) {
	var list ledgerv1alpha1.LedgerServiceList
	opts := []client.ListOption{}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if err := c.List(ctx, &list, opts...); err != nil {
		return nil, err
	}

	return &list, nil
}

// LedgerServicePods lists pods matching the selector labels for a LedgerService.
func LedgerServicePods(ctx context.Context, cs kubernetes.Interface, namespace, name string) (*corev1.PodList, error) {
	return cs.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: LabelSelector(name),
	})
}

// LedgerServicePVCs lists PVCs matching the selector labels for a LedgerService.
func LedgerServicePVCs(ctx context.Context, cs kubernetes.Interface, namespace, name string) (*corev1.PersistentVolumeClaimList, error) {
	return cs.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: LabelSelector(name),
	})
}

// LedgerServiceStatefulSet fetches the StatefulSet for a LedgerService. The
// operator names it "ledger-<cr>" (resourcePrefix), not the bare CR name.
func LedgerServiceStatefulSet(ctx context.Context, cs kubernetes.Interface, namespace, name string) (*appsv1.StatefulSet, error) {
	return cs.AppsV1().StatefulSets(namespace).Get(ctx, resourcePrefix+name, metav1.GetOptions{})
}

// LedgerServicePodName returns the name of the ordinal-th StatefulSet pod for a
// LedgerService. The operator names pods "ledger-<cr>-<ordinal>" (resourcePrefix),
// not "<cr>-<ordinal>".
func LedgerServicePodName(name string, ordinal int) string {
	return fmt.Sprintf("%s%s-%d", resourcePrefix, name, ordinal)
}

// LedgerServices lists services matching the selector labels for a LedgerService.
func LedgerServices(ctx context.Context, cs kubernetes.Interface, namespace, name string) (*corev1.ServiceList, error) {
	return cs.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: LabelSelector(name),
	})
}

// GetLedgerBackup fetches a single LedgerBackup CR.
func GetLedgerBackup(ctx context.Context, c client.Client, namespace, name string) (*ledgerv1alpha1.LedgerBackup, error) {
	var backup ledgerv1alpha1.LedgerBackup
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &backup); err != nil {
		return nil, err
	}

	return &backup, nil
}

// ListLedgerBackups lists LedgerBackup CRs in a namespace.
func ListLedgerBackups(ctx context.Context, c client.Client, namespace string) (*ledgerv1alpha1.LedgerBackupList, error) {
	var list ledgerv1alpha1.LedgerBackupList
	opts := []client.ListOption{}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if err := c.List(ctx, &list, opts...); err != nil {
		return nil, err
	}

	return &list, nil
}

// GetLedgerBackupRun fetches a single LedgerBackupRun CR.
func GetLedgerBackupRun(ctx context.Context, c client.Client, namespace, name string) (*ledgerv1alpha1.LedgerBackupRun, error) {
	var run ledgerv1alpha1.LedgerBackupRun
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &run); err != nil {
		return nil, err
	}

	return &run, nil
}

// ListLedgerBackupRuns lists LedgerBackupRun CRs in a namespace, optionally filtered
// by parent LedgerBackup name (matches the LabelLedgerBackup label). Pass an empty
// backupName to list all runs in the namespace.
func ListLedgerBackupRuns(ctx context.Context, c client.Client, namespace, backupName string) (*ledgerv1alpha1.LedgerBackupRunList, error) {
	var list ledgerv1alpha1.LedgerBackupRunList
	opts := []client.ListOption{}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if backupName != "" {
		opts = append(opts, client.MatchingLabels{ledgerv1alpha1.LabelLedgerBackup: backupName})
	}
	if err := c.List(ctx, &list, opts...); err != nil {
		return nil, err
	}

	return &list, nil
}

// GetLedgerClusterAgent fetches a single cluster-scoped LedgerClusterAgent CR.
func GetLedgerClusterAgent(ctx context.Context, c client.Client, name string) (*ledgerv1alpha1.LedgerClusterAgent, error) {
	var agent ledgerv1alpha1.LedgerClusterAgent
	if err := c.Get(ctx, types.NamespacedName{Name: name}, &agent); err != nil {
		return nil, err
	}

	return &agent, nil
}

// ListLedgerClusterAgents lists all cluster-scoped LedgerClusterAgent CRs.
func ListLedgerClusterAgents(ctx context.Context, c client.Client) (*ledgerv1alpha1.LedgerClusterAgentList, error) {
	var list ledgerv1alpha1.LedgerClusterAgentList
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
