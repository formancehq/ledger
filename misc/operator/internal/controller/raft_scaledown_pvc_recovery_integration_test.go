//go:build integration

package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

func TestScaleDownPVCDeletionRecoversAfterReplicaPersistence(t *testing.T) {
	t.Parallel()
	namespace := createTestNamespace(t)
	ledger := newCluster("pvc-recovery", namespace)
	ledger.UID = types.UID(namespace + "-cluster")
	applyDefaults(ledger)
	ledger.Spec.Persistence.DeletionProtection = new(false)
	clientset, err := kubernetes.NewForConfig(testEnv.Config)
	require.NoError(t, err)
	intercept := &failFirstScaleDownPVCDelete{Interface: clientset, t: t, namespace: namespace, name: resourceName(ledger.Name)}
	reconciler := &ClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Config: testEnv.Config, Clientset: intercept}
	fixture := &scaleDownReconcileMembership{
		t: t, namespace: namespace, pod0: podName(ledger.Name, 0),
		members: map[int]bool{1: true, 2: true, 3: true},
	}
	_, err = reconciler.reconcileStatefulSetWithExec(ctx, ledger, "initial", nil, fixture.exec)
	require.NoError(t, err)
	// Pods are controlled by the Cluster's StatefulSet, as the StatefulSet
	// controller creates them: scale-down execs only into pods it controls.
	owner := &appsv1.StatefulSet{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: resourceName(ledger.Name)}, owner))
	for ordinal := range 3 {
		createScaleDownReconcilePod(t, clientset, owner, podName(ledger.Name, ordinal), corev1.PodRunning)
		for _, volume := range []string{"wal", "data"} {
			_, err := clientset.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-%s-%d", volume, resourceName(ledger.Name), ordinal)},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
				},
			}, metav1.CreateOptions{})
			require.NoError(t, err)
		}
	}
	ledger.Spec.Replicas = new(int32(1))
	intercept.fail = true
	_, err = reconciler.reconcileStatefulSetWithExec(ctx, ledger, "scaled", nil, fixture.exec)
	require.ErrorContains(t, err, "injected PVC deletion failure")
	require.Equal(t, 1, intercept.attempts)
	sts := &appsv1.StatefulSet{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: resourceName(ledger.Name)}, sts))
	require.EqualValues(t, 1, *sts.Spec.Replicas, "replica reduction must commit before the injected failure")
	require.Equal(t, "3", sts.Annotations[annotationScaleDownPVCFrom])

	// A new reconciler has no memory of the failed attempt. It must use the
	// persisted StatefulSet marker and delete both removed ordinals.
	reconciler = &ClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Config: testEnv.Config, Clientset: intercept}
	_, err = reconciler.reconcileStatefulSetWithExec(ctx, ledger, "scaled", nil, fixture.exec)
	require.NoError(t, err)
	requireScaleDownReconcileState(t, clientset, namespace, resourceName(ledger.Name), 1)
	require.Equal(t, 5, intercept.attempts)
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: resourceName(ledger.Name)}, sts))
	require.NotContains(t, sts.Annotations, annotationScaleDownPVCFrom)
	_, err = reconciler.reconcileStatefulSetWithExec(ctx, ledger, "scaled", nil, fixture.exec)
	require.NoError(t, err)
	require.Equal(t, 5, intercept.attempts, "completed cleanup must not run again")
}

type failFirstScaleDownPVCDelete struct {
	kubernetes.Interface
	t         *testing.T
	namespace string
	name      string
	fail      bool
	attempts  int
}

func (c *failFirstScaleDownPVCDelete) CoreV1() coreclient.CoreV1Interface {
	return &interceptScaleDownCore{CoreV1Interface: c.Interface.CoreV1(), parent: c}
}

type interceptScaleDownCore struct {
	coreclient.CoreV1Interface
	parent *failFirstScaleDownPVCDelete
}

func (c *interceptScaleDownCore) PersistentVolumeClaims(namespace string) coreclient.PersistentVolumeClaimInterface {
	return &interceptScaleDownPVCs{PersistentVolumeClaimInterface: c.CoreV1Interface.PersistentVolumeClaims(namespace), parent: c.parent}
}

type interceptScaleDownPVCs struct {
	coreclient.PersistentVolumeClaimInterface
	parent *failFirstScaleDownPVCDelete
}

func (c *interceptScaleDownPVCs) Delete(ctx context.Context, name string, options metav1.DeleteOptions) error {
	c.parent.t.Helper()
	c.parent.attempts++
	sts := &appsv1.StatefulSet{}
	require.NoError(c.parent.t, k8sClient.Get(ctx, types.NamespacedName{Namespace: c.parent.namespace, Name: c.parent.name}, sts))
	require.EqualValues(c.parent.t, 1, *sts.Spec.Replicas)
	if c.parent.fail {
		c.parent.fail = false
		return errors.New("injected PVC deletion failure")
	}
	return c.PersistentVolumeClaimInterface.Delete(ctx, name, options)
}
