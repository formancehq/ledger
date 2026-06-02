//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// newTestClientset creates a typed clientset from the envtest config.
func newTestClientset(t *testing.T) kubernetes.Interface {
	t.Helper()
	cs, err := kubernetes.NewForConfig(testEnv.Config)
	require.NoError(t, err)
	return cs
}

func TestIsPodCrashed_CrashLoopBackOff(t *testing.T) {
	ns := createTestNamespace(t)
	cs := newTestClientset(t)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "crash-pod", Namespace: ns},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "ledger",
				Image: "busybox",
			}},
		},
	}
	_, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	require.NoError(t, err)

	// Update status to simulate CrashLoopBackOff
	pod.Status = corev1.PodStatus{
		Phase: corev1.PodRunning,
		ContainerStatuses: []corev1.ContainerStatus{{
			Name: "ledger",
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{
					Reason: "CrashLoopBackOff",
				},
			},
		}},
	}
	_, err = cs.CoreV1().Pods(ns).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
	require.NoError(t, err)

	require.True(t, isPodCrashed(ctx, cs, ns, "crash-pod"))
}

func TestIsPodCrashed_Running(t *testing.T) {
	ns := createTestNamespace(t)
	cs := newTestClientset(t)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "running-pod", Namespace: ns},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "ledger",
				Image: "busybox",
			}},
		},
	}
	_, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	require.NoError(t, err)

	// Update status to Running with ready container
	pod.Status = corev1.PodStatus{
		Phase: corev1.PodRunning,
		ContainerStatuses: []corev1.ContainerStatus{{
			Name:  "ledger",
			Ready: true,
			State: corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{},
			},
		}},
	}
	_, err = cs.CoreV1().Pods(ns).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
	require.NoError(t, err)

	require.False(t, isPodCrashed(ctx, cs, ns, "running-pod"))
}

func TestIsPodCrashed_NotFound(t *testing.T) {
	ns := createTestNamespace(t)
	cs := newTestClientset(t)

	// Pod does not exist — should be treated as crashed
	require.True(t, isPodCrashed(ctx, cs, ns, "nonexistent-pod"))
}

func TestIsPodCrashed_Pending(t *testing.T) {
	ns := createTestNamespace(t)
	cs := newTestClientset(t)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pending-pod", Namespace: ns},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "ledger",
				Image: "busybox",
			}},
		},
	}
	_, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	require.NoError(t, err)

	// Update status to Pending
	pod.Status = corev1.PodStatus{
		Phase: corev1.PodPending,
	}
	_, err = cs.CoreV1().Pods(ns).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
	require.NoError(t, err)

	require.False(t, isPodCrashed(ctx, cs, ns, "pending-pod"))
}

func TestIsPodCrashed_OOMKilled(t *testing.T) {
	ns := createTestNamespace(t)
	cs := newTestClientset(t)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "oom-pod", Namespace: ns},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "ledger",
				Image: "busybox",
			}},
		},
	}
	_, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	require.NoError(t, err)

	// Update status with OOMKilled terminated container
	pod.Status = corev1.PodStatus{
		Phase: corev1.PodRunning,
		ContainerStatuses: []corev1.ContainerStatus{{
			Name: "ledger",
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					Reason:   "OOMKilled",
					ExitCode: 137,
				},
			},
		}},
	}
	_, err = cs.CoreV1().Pods(ns).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
	require.NoError(t, err)

	require.True(t, isPodCrashed(ctx, cs, ns, "oom-pod"))
}

func TestIsPodCrashed_Failed(t *testing.T) {
	ns := createTestNamespace(t)
	cs := newTestClientset(t)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "failed-pod", Namespace: ns},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "ledger",
				Image: "busybox",
			}},
		},
	}
	_, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	require.NoError(t, err)

	// Update status to Failed phase
	pod.Status = corev1.PodStatus{
		Phase: corev1.PodFailed,
	}
	_, err = cs.CoreV1().Pods(ns).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
	require.NoError(t, err)

	require.True(t, isPodCrashed(ctx, cs, ns, "failed-pod"))
}
