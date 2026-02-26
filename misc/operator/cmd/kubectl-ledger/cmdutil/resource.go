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

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

const (
	LabelName     = "app.kubernetes.io/name"
	LabelInstance = "app.kubernetes.io/instance"
	LabelValue    = "ledger"
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

// LedgerServiceStatefulSet fetches the StatefulSet for a LedgerService (same name as CR).
func LedgerServiceStatefulSet(ctx context.Context, cs kubernetes.Interface, namespace, name string) (*appsv1.StatefulSet, error) {
	return cs.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
}

// LedgerServices lists services matching the selector labels for a LedgerService.
func LedgerServices(ctx context.Context, cs kubernetes.Interface, namespace, name string) (*corev1.ServiceList, error) {
	return cs.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: LabelSelector(name),
	})
}

// GetLedgerDefaults fetches a single cluster-scoped LedgerDefaults CR.
func GetLedgerDefaults(ctx context.Context, c client.Client, name string) (*ledgerv1alpha1.LedgerDefaults, error) {
	var defaults ledgerv1alpha1.LedgerDefaults
	if err := c.Get(ctx, types.NamespacedName{Name: name}, &defaults); err != nil {
		return nil, err
	}
	return &defaults, nil
}

// ListLedgerDefaults lists all cluster-scoped LedgerDefaults CRs.
func ListLedgerDefaults(ctx context.Context, c client.Client) (*ledgerv1alpha1.LedgerDefaultsList, error) {
	var list ledgerv1alpha1.LedgerDefaultsList
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
