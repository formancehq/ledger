package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// A Pod incarnation's status says nothing about its ordinal's membership.
// Start from three established voters even when node 3's replacement cannot run.
func TestRaftScaleDownReplacementMembership(t *testing.T) {
	t.Parallel()

	for _, podState := range []string{"pending", "missing", "no-container-status", "running", "pending-image-pull-backoff"} {
		for _, outcome := range []string{"success", "removal-fails", "response-lost", "already-absent", "status-fails", "transfer-fails"} {
			t.Run(podState+"/"+outcome, func(t *testing.T) {
				t.Parallel()
				ctx := context.Background()
				cluster := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "replacement", Namespace: "ns"}}
				objects := []runtime.Object{}
				for ordinal := range 3 {
					if ordinal == 2 && podState == "missing" {
						continue
					}
					pod := &corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{Name: podName(cluster.Name, ordinal), Namespace: cluster.Namespace},
						Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{
							Name: "ledger", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
						}}},
					}
					if ordinal == 2 && (podState == "pending" || podState == "no-container-status") {
						pod.Status.ContainerStatuses = nil
						if podState == "pending" {
							pod.Status.Phase = corev1.PodPending
						}
					}
					if ordinal == 2 && podState == "pending-image-pull-backoff" {
						pod.Status = corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{
							Name: "ledger", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
						}}}
					}
					objects = append(objects, pod)
				}
				clientset := fake.NewClientset(objects...)
				members := map[int]bool{1: true, 2: true, 3: true}
				if outcome == "already-absent" {
					delete(members, 3)
				}
				var removals []int
				var calls []string
				injectedErr := errors.New("injected exec failure")
				exec := ledgerctlExec(func(_ context.Context, _ *rest.Config, _ kubernetes.Interface,
					namespace, pod, container string, command []string,
				) (*execResult, error) {
					require.Equal(t, cluster.Namespace, namespace)
					require.Equal(t, podName(cluster.Name, 0), pod)
					require.Equal(t, "ledger", container)
					cmd := command[2]
					calls = append(calls, cmd)
					switch {
					case strings.Contains(cmd, "'transfer-leader' '1'"):
						require.Len(t, calls, 1)
						if outcome == "transfer-fails" {
							return nil, injectedErr
						}

						return &execResult{}, nil
					case strings.Contains(cmd, "'status'"):
						if outcome == "status-fails" {
							return nil, injectedErr
						}
						var nodes []string
						for id := 1; id <= 3; id++ {
							if members[id] {
								nodes = append(nodes, fmt.Sprintf(`{"id":%d}`, id))
							}
						}

						return &execResult{Stdout: `{"state":"Leader","nodes":[` + strings.Join(nodes, ",") + `]}`}, nil
					case strings.Contains(cmd, "'remove-node'"):
						id := 2
						if strings.Contains(cmd, "'remove-node' '3'") {
							id = 3
						}
						removals = append(removals, id)
						require.Equal(t, id == 3 && (podState == "missing" || podState == "pending-image-pull-backoff"), strings.Contains(cmd, "'--force'"))
						if outcome == "removal-fails" {
							return nil, injectedErr
						}
						delete(members, id)
						if outcome == "response-lost" {
							return nil, injectedErr
						}

						return &execResult{}, nil
					default:
						t.Fatalf("unexpected command %s", cmd)

						return nil, nil
					}
				})

				err := raftScaleDown(ctx, nil, clientset, cluster, 3, 1, "disabled", exec)
				t.Logf("outcome=%s removals=%v remainingMembership=%v error=%v", outcome, removals, members, err)
				switch outcome {
				case "transfer-fails", "status-fails":
					require.ErrorIs(t, err, injectedErr)
					require.Empty(t, removals)
					require.Len(t, members, 3)
				case "removal-fails":
					require.ErrorIs(t, err, injectedErr)
					require.Equal(t, []int{3}, removals, "failure must stop before removing the healthy voter")
					require.Len(t, members, 3)
				default:
					require.NoError(t, err)
					require.Equal(t, map[int]bool{1: true}, members, "every removed ordinal needs authoritative absence")
					if outcome == "already-absent" {
						require.Equal(t, []int{2}, removals)
						require.Len(t, calls, 4, "both ordinals must be checked, including the absent replacement")
					} else {
						require.Equal(t, []int{3, 2}, removals)
					}
				}
			})
		}
	}
}
