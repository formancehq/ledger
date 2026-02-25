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

// LabelSelector returns a comma-separated label selector for the given Ledger name.
func LabelSelector(name string) string {
	return fmt.Sprintf("%s=%s,%s=%s", LabelName, LabelValue, LabelInstance, name)
}

// GetLedger fetches a single Ledger CR.
func GetLedger(ctx context.Context, c client.Client, namespace, name string) (*ledgerv1alpha1.Ledger, error) {
	var ledger ledgerv1alpha1.Ledger
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &ledger); err != nil {
		return nil, err
	}
	return &ledger, nil
}

// ListLedgers lists Ledger CRs. Pass empty namespace for all namespaces.
func ListLedgers(ctx context.Context, c client.Client, namespace string) (*ledgerv1alpha1.LedgerList, error) {
	var list ledgerv1alpha1.LedgerList
	opts := []client.ListOption{}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if err := c.List(ctx, &list, opts...); err != nil {
		return nil, err
	}
	return &list, nil
}

// LedgerPods lists pods matching the selector labels for a Ledger.
func LedgerPods(ctx context.Context, cs kubernetes.Interface, namespace, name string) (*corev1.PodList, error) {
	return cs.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: LabelSelector(name),
	})
}

// LedgerPVCs lists PVCs matching the selector labels for a Ledger.
func LedgerPVCs(ctx context.Context, cs kubernetes.Interface, namespace, name string) (*corev1.PersistentVolumeClaimList, error) {
	return cs.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: LabelSelector(name),
	})
}

// LedgerStatefulSet fetches the StatefulSet for a Ledger (same name as CR).
func LedgerStatefulSet(ctx context.Context, cs kubernetes.Interface, namespace, name string) (*appsv1.StatefulSet, error) {
	return cs.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
}

// LedgerServices lists services matching the selector labels for a Ledger.
func LedgerServices(ctx context.Context, cs kubernetes.Interface, namespace, name string) (*corev1.ServiceList, error) {
	return cs.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: LabelSelector(name),
	})
}
