//go:build integration

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Kubernetes state is real envtest state; only ledgerctl's Raft boundary is
// modeled. Establish membership before replacing the Pod so the replacement's
// Pending phase cannot be mistaken for evidence that its ordinal never joined.
func TestReconcileScaleDownReplacementMembership(t *testing.T) {
	t.Parallel()

	for _, replacement := range []string{"pending", "missing"} {
		for _, outcome := range []string{"success", "failure_then_retry", "later_failure_then_retry", "committed_response_lost"} {
			t.Run(replacement+"/"+outcome, func(t *testing.T) {
				t.Parallel()

				namespace := createTestNamespace(t)
				ledger := newCluster("membership", namespace)
				// Do not create this CR: the background manager must not race the
				// direct reconciler or try to run a real ledgerctl process.
				ledger.UID = types.UID(namespace + "-cluster")
				applyDefaults(ledger)
				ledger.Spec.Persistence.DeletionProtection = new(false)
				clientset, err := kubernetes.NewForConfig(testEnv.Config)
				require.NoError(t, err)
				reconciler := &ClusterReconciler{
					Client: k8sClient, Scheme: k8sClient.Scheme(),
					Config: testEnv.Config, Clientset: clientset,
				}
				fixture := &scaleDownReconcileMembership{
					t: t, namespace: namespace, pod0: podName(ledger, 0),
					members: map[int]bool{1: true, 2: true, 3: true},
					outcome: outcome, forceThird: replacement == "missing",
				}
				_, err = reconciler.reconcileStatefulSetWithExec(ctx, ledger, "initial", nil, fixture.exec)
				require.NoError(t, err)
				require.Empty(t, fixture.commands, "initial creation must not remove established members")
				// envtest runs no StatefulSet controller: the fixture creates the
				// pods and claims it would, controlled by / labelled for the
				// Cluster's StatefulSet, which the exec and PVC-deletion paths
				// require before acting on them.
				sts := &appsv1.StatefulSet{}
				require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: clusterBaseName(ledger)}, sts))

				for ordinal := range 3 {
					createScaleDownReconcilePod(t, clientset, sts, podName(ledger, ordinal), corev1.PodRunning)
					createScaleDownReconcileClaims(t, clientset, sts, podName(ledger, ordinal))
				}
				// The pods becoming Ready update the StatefulSet status, which
				// triggers a reconcile; it stamps the claims the Ready pods
				// mount with the Cluster UID, which scale-down requires.
				_, err = reconciler.reconcileStatefulSetWithExec(ctx, ledger, "initial", nil, fixture.exec)
				require.NoError(t, err)
				require.Empty(t, fixture.commands, "a steady-state pass must not remove established members")
				for ordinal := range 3 {
					for _, volume := range []string{"wal", "data"} {
						pvc, err := clientset.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName(ledger, volume, int32(ordinal)), metav1.GetOptions{})
						require.NoError(t, err)
						require.Equal(t, string(ledger.UID), pvc.Annotations[annotationClusterUID], "the claims of Ready owned pods must be stamped")
					}
				}
				oldPod, err := clientset.CoreV1().Pods(namespace).Get(ctx, podName(ledger, 2), metav1.GetOptions{})
				require.NoError(t, err)
				require.NoError(t, clientset.CoreV1().Pods(namespace).Delete(ctx, oldPod.Name, metav1.DeleteOptions{GracePeriodSeconds: new(int64(0))}))
				if replacement == "pending" {
					createScaleDownReconcilePod(t, clientset, sts, oldPod.Name, corev1.PodPending)
					newPod, err := clientset.CoreV1().Pods(namespace).Get(ctx, oldPod.Name, metav1.GetOptions{})
					require.NoError(t, err)
					require.NotEqual(t, oldPod.UID, newPod.UID, "same ordinal must have a new Pod incarnation")
				}
				require.Equal(t, map[int]bool{1: true, 2: true, 3: true}, fixture.members)
				ledger.Spec.Replicas = new(int32(1))
				_, err = reconciler.reconcileStatefulSetWithExec(ctx, ledger, "scaled", nil, fixture.exec)
				if outcome == "failure_then_retry" || outcome == "later_failure_then_retry" {
					require.ErrorContains(t, err, "injected removal failure")
					requireScaleDownReconcileState(t, clientset, namespace, clusterBaseName(ledger), 3)
					t.Logf("failed removal preserved replicas=3 and all PVCs; membership=%v; removal attempts=%v", fixture.members, fixture.removals)
					if outcome == "failure_then_retry" {
						require.Equal(t, []int{3}, fixture.removals, "failure must stop before the next ordinal")
						require.Equal(t, map[int]bool{1: true, 2: true, 3: true}, fixture.members)
					} else {
						require.Equal(t, []int{3, 2}, fixture.removals)
						require.Equal(t, map[int]bool{1: true, 2: true}, fixture.members)
					}
					// A new controller has no memory of the successful removal;
					// retry must recover progress from authoritative membership.
					reconciler = &ClusterReconciler{
						Client: k8sClient, Scheme: k8sClient.Scheme(),
						Config: testEnv.Config, Clientset: clientset,
					}
					fixture.transferred = false
					_, err = reconciler.reconcileStatefulSetWithExec(ctx, ledger, "scaled", nil, fixture.exec)
				}
				require.NoError(t, err)
				requireScaleDownReconcileState(t, clientset, namespace, clusterBaseName(ledger), 1)
				t.Logf("successful reconcile persisted replicas=1 and requested PVC deletion for ordinals 1 and 2; membership=%v; removal attempts=%v", fixture.members, fixture.removals)
				require.Equal(t, map[int]bool{1: true}, fixture.members, "every removed ordinal must be absent before replicas decrease")
				expectedRemovals := []int{3, 2}
				if outcome == "failure_then_retry" {
					expectedRemovals = []int{3, 3, 2}
				} else if outcome == "later_failure_then_retry" {
					expectedRemovals = []int{3, 2, 2}
				}
				require.Equal(t, expectedRemovals, fixture.removals)
			})
		}
	}
}

// TestReconcileScaleDownKeepsClaimsOfReplicaNeverSeenReady is the fail-safe
// side of the provenance stamp on real API state: pod-2 is already replaced by
// a Pending pod, or missing, when the first pass that scales down runs, so no
// pass ever saw it Ready and its claims carry no stamp. Scaling from 3 to 1
// deletes ordinal 1's stamped claims and keeps ordinal 2's, reported by
// ScaleDownPVCsRetained; a later scale-up back to 3 is refused (ScaleUpBlocked)
// rather than remounting them.
func TestReconcileScaleDownKeepsClaimsOfReplicaNeverSeenReady(t *testing.T) {
	t.Parallel()

	for _, replacement := range []string{"pending", "missing"} {
		t.Run(replacement, func(t *testing.T) {
			t.Parallel()

			namespace := createTestNamespace(t)
			ledger := newCluster("unstamped", namespace)
			// Not created, as in TestReconcileScaleDownReplacementMembership.
			ledger.UID = types.UID(namespace + "-cluster")
			applyDefaults(ledger)
			ledger.Spec.Persistence.DeletionProtection = new(false)
			clientset, err := kubernetes.NewForConfig(testEnv.Config)
			require.NoError(t, err)
			reconciler := &ClusterReconciler{
				Client: k8sClient, Scheme: k8sClient.Scheme(),
				Config: testEnv.Config, Clientset: clientset,
			}
			fixture := &scaleDownReconcileMembership{
				t: t, namespace: namespace, pod0: podName(ledger, 0),
				members: map[int]bool{1: true, 2: true, 3: true},
				outcome: "success", forceThird: replacement == "missing",
			}
			_, err = reconciler.reconcileStatefulSetWithExec(ctx, ledger, "initial", nil, fixture.exec)
			require.NoError(t, err)
			sts := &appsv1.StatefulSet{}
			require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: clusterBaseName(ledger)}, sts))
			for ordinal := range 3 {
				switch {
				case ordinal < 2:
					createScaleDownReconcilePod(t, clientset, sts, podName(ledger, ordinal), corev1.PodRunning)
				case replacement == "pending":
					createScaleDownReconcilePod(t, clientset, sts, podName(ledger, ordinal), corev1.PodPending)
				}
				createScaleDownReconcileClaims(t, clientset, sts, podName(ledger, ordinal))
			}

			ledger.Spec.Replicas = new(int32(1))
			_, err = reconciler.reconcileStatefulSetWithExec(ctx, ledger, "scaled", nil, fixture.exec)

			require.NoError(t, err)
			require.Equal(t, map[int]bool{1: true}, fixture.members)
			require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: clusterBaseName(ledger)}, sts))
			require.Equal(t, int32(1), *sts.Spec.Replicas)
			require.Equal(t, "3", sts.Annotations[annotationScaledDownFrom], "the record must stay while claims are kept")
			for _, volume := range []string{"wal", "data"} {
				pvc, err := clientset.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName(ledger, volume, 1), metav1.GetOptions{})
				if !kerrors.IsNotFound(err) {
					require.NoError(t, err)
					// envtest has no PVC-protection controller to clear finalizers.
					require.NotNil(t, pvc.DeletionTimestamp, "ordinal 1's stamped claims must be deleted")
				}
				pvc, err = clientset.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName(ledger, volume, 2), metav1.GetOptions{})
				require.NoError(t, err)
				require.Nil(t, pvc.DeletionTimestamp, "ordinal 2's unstamped claims must be kept")
				require.NotContains(t, pvc.Annotations, annotationClusterUID)
			}
			condition := meta.FindStatusCondition(ledger.Status.Conditions, conditionScaleDownPVCsRetained)
			require.NotNil(t, condition)
			require.Equal(t, "PVCsRetained", condition.Reason)
			require.Contains(t, condition.Message, pvcName(ledger, "data", 2)+" (origin cannot be established")

			ledger.Spec.Replicas = new(int32(3))
			_, err = reconciler.reconcileStatefulSetWithExec(ctx, ledger, "scaled", nil, fixture.exec)

			require.NoError(t, err)
			require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: clusterBaseName(ledger)}, sts))
			require.Equal(t, int32(1), *sts.Spec.Replicas, "the scale-up must not remount the kept claims")
			condition = meta.FindStatusCondition(ledger.Status.Conditions, conditionScaleDownPVCsRetained)
			require.NotNil(t, condition)
			require.Equal(t, "ScaleUpBlocked", condition.Reason)
			require.Contains(t, condition.Message, pvcName(ledger, "data", 2))
		})
	}
}

type scaleDownReconcileMembership struct {
	t           *testing.T
	namespace   string
	pod0        string
	members     map[int]bool
	outcome     string
	forceThird  bool
	transferred bool
	injected    bool
	commands    []string
	removals    []int
}

func (f *scaleDownReconcileMembership) exec(_ context.Context, _ *rest.Config, _ kubernetes.Interface,
	namespace, pod, container string, command []string,
) (*execResult, error) {
	f.t.Helper()
	require.Equal(f.t, f.namespace, namespace)
	require.Equal(f.t, f.pod0, pod)
	require.Equal(f.t, "ledger", container)
	require.Len(f.t, command, 3)
	shell := command[2]
	f.commands = append(f.commands, shell)
	if strings.Contains(shell, "'cluster' 'transfer-leader' '1'") {
		f.transferred = true
		return &execResult{}, nil
	}
	require.True(f.t, f.transferred, "leadership transfer must precede membership checks and removal")
	if strings.Contains(shell, "'cluster' 'status'") {
		nodes := make([]map[string]int, 0, len(f.members))
		for _, id := range []int{1, 2, 3} {
			if f.members[id] {
				nodes = append(nodes, map[string]int{"id": id})
			}
		}
		encoded, err := json.Marshal(map[string]any{"state": "Leader", "nodes": nodes})
		require.NoError(f.t, err)
		return &execResult{Stdout: string(encoded)}, nil
	}
	for _, id := range []int{3, 2} {
		if !strings.Contains(shell, fmt.Sprintf("'cluster' 'remove-node' '%d'", id)) {
			continue
		}
		require.Equal(f.t, id == 3 && f.forceThird, strings.Contains(shell, "'--force'"))
		require.True(f.t, f.members[id], "a removed member must not be removed a second time")
		f.removals = append(f.removals, id)
		inject := f.outcome == "failure_then_retry" ||
			(f.outcome == "later_failure_then_retry" && id == 2) ||
			(f.outcome == "committed_response_lost" && id == 3)
		if !f.injected && inject {
			f.injected = true
			if f.outcome == "committed_response_lost" {
				delete(f.members, id)
			}
			return &execResult{Stderr: "opaque CLI failure"}, errors.New("injected removal failure")
		}
		delete(f.members, id)
		return &execResult{}, nil
	}
	f.t.Fatalf("unexpected ledgerctl invocation: %s", shell)
	return nil, errors.New("unexpected ledgerctl invocation")
}

func createScaleDownReconcilePod(t *testing.T, clientset kubernetes.Interface, sts *appsv1.StatefulSet, name string, phase corev1.PodPhase) {
	t.Helper()
	pod, err := clientset.CoreV1().Pods(sts.Namespace).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(sts, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "ledger", Image: "ledger:test"}},
			// The StatefulSet controller mounts each VolumeClaimTemplate's claim
			// "<volume>-<pod>".
			Volumes: []corev1.Volume{
				{Name: "wal", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "wal-" + name}}},
				{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data-" + name}}},
			},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	pod.Status.Phase = phase
	if phase == corev1.PodRunning {
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: "ledger", Ready: true, Image: "ledger:test", ImageID: "ledger:test",
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		}}
	}
	_, err = clientset.CoreV1().Pods(sts.Namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
	require.NoError(t, err)
}

// createScaleDownReconcileClaims creates the wal and data claims of pod as
// the StatefulSet controller does: "<volume>-<pod>", with the StatefulSet
// selector labels and no owner.
func createScaleDownReconcileClaims(t *testing.T, clientset kubernetes.Interface, sts *appsv1.StatefulSet, pod string) {
	t.Helper()
	for _, volume := range []string{"wal", "data"} {
		_, err := clientset.CoreV1().PersistentVolumeClaims(sts.Namespace).Create(ctx, &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: volume + "-" + pod, Labels: sts.Spec.Selector.MatchLabels},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
			},
		}, metav1.CreateOptions{})
		require.NoError(t, err)
	}
}

func requireScaleDownReconcileState(t *testing.T, clientset kubernetes.Interface, namespace, name string, replicas int32) {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, sts))
	require.Equal(t, replicas, *sts.Spec.Replicas)
	for ordinal := range 3 {
		for _, volume := range []string{"wal", "data"} {
			pvc, err := clientset.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, fmt.Sprintf("%s-%s-%d", volume, name, ordinal), metav1.GetOptions{})
			if int32(ordinal) < replicas {
				require.NoError(t, err)
				require.Nil(t, pvc.DeletionTimestamp, "retained replicas must keep their PVCs")
			} else if !kerrors.IsNotFound(err) {
				require.NoError(t, err)
				// envtest has no PVC-protection controller to clear finalizers;
				// a deletion timestamp proves the real API accepted the DELETE.
				require.NotNil(t, pvc.DeletionTimestamp, "removed replicas must have their PVCs deleted")
			}
		}
	}
}
