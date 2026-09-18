package internal

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// pollingConn exercises the generated client and observes the context delivered
// to its transport. Kubernetes cases use the real REST client the same way.
type pollingConn struct {
	grpc.ClientConnInterface
	request func(context.Context) error
}

func (c pollingConn) Invoke(ctx context.Context, _ string, _, reply any, _ ...grpc.CallOption) error {
	if err := c.request(ctx); err != nil {
		return err
	}
	state := reply.(*clusterpb.ClusterState)
	state.Leader = 1
	state.Nodes = []*clusterpb.NodeInfo{{Id: 1, Suffrage: "Voter"}}
	state.ClusterConfig = &commonpb.ClusterConfig{}
	return nil
}

type pollingRoundTripper func(*http.Request) (*http.Response, error)

func (f pollingRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type pollingCase struct {
	name string
	run  func(context.Context, time.Duration, func(context.Context) error) bool
}

func pollingCases(t *testing.T) []pollingCase {
	t.Helper()
	clusterClient := func(request func(context.Context) error) clusterpb.ClusterServiceClient {
		return clusterpb.NewClusterServiceClient(pollingConn{request: request})
	}
	kubeClient := func(request func(context.Context) error, body string) kubernetes.Interface {
		client, err := kubernetes.NewForConfigAndClient(&rest.Config{Host: "http://polling.test", QPS: -1}, &http.Client{
			Transport: pollingRoundTripper(func(r *http.Request) (*http.Response, error) {
				if err := request(r.Context()); err != nil {
					return nil, err
				}
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
			}),
		})
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	return []pollingCase{
		{"voters", func(ctx context.Context, timeout time.Duration, request func(context.Context) error) bool {
			return WaitForVoters(ctx, clusterClient(request), 1, timeout, nil)
		}},
		{"leader_config", func(ctx context.Context, timeout time.Duration, request func(context.Context) error) bool {
			return WaitForClusterConfig(ctx, clusterClient(request), func(*commonpb.ClusterConfig) bool { return true }, timeout)
		}},
		{"node_config", func(ctx context.Context, timeout time.Duration, request func(context.Context) error) bool {
			return WaitForClusterConfigOnNode(ctx, clusterClient(request), 1, func(*commonpb.ClusterConfig) bool { return true }, timeout)
		}},
		{"pod_gone", func(ctx context.Context, timeout time.Duration, request func(context.Context) error) bool {
			client := kubeClient(request, `{"metadata":{"uid":"replacement"}}`)
			return WaitForPodGone(ctx, client, "ledger-ledger-0", "original", timeout)
		}},
		{"pod_ready", func(ctx context.Context, timeout time.Duration, request func(context.Context) error) bool {
			client := kubeClient(request, `{"status":{"conditions":[{"type":"Ready","status":"True"}]}}`)
			return WaitForPodReady(ctx, client, "ledger-ledger-0", timeout)
		}},
		{"statefulset_ready", func(ctx context.Context, timeout time.Duration, request func(context.Context) error) bool {
			client := kubeClient(request, `{"status":{"readyReplicas":1,"currentRevision":"rev","updateRevision":"rev"}}`)
			return WaitForStatefulSetReady(ctx, client, "ledger-ledger", 1, timeout)
		}},
	}
}

func TestPollingHelpers_CancelInFlightRequest(t *testing.T) {
	t.Parallel()
	for _, tc := range pollingCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				entered := make(chan context.Context, 1)
				done := make(chan bool, 1)
				go func() {
					done <- tc.run(parent, 10*time.Second, func(ctx context.Context) error {
						entered <- ctx
						<-ctx.Done()
						return ctx.Err()
					})
				}()
				requestCtx := <-entered
				synctest.Wait()
				timer := time.NewTimer(20 * time.Second)
				defer timer.Stop()
				<-timer.C
				synctest.Wait()
				select {
				case ready := <-done:
					if ready {
						t.Error("blocked request unexpectedly reported convergence")
					}
					if requestCtx.Err() != context.DeadlineExceeded {
						t.Errorf("request context error = %v, want helper deadline exceeded", requestCtx.Err())
					}
					if parent.Err() != nil {
						t.Errorf("helper deadline cancelled parent: %v", parent.Err())
					}
				default:
					t.Error("helper timeout did not cancel the in-flight request: still blocked after twice the configured timeout")
					cancel()
					if <-done {
						t.Error("parent cancellation unexpectedly reported convergence")
					}
				}
			})
		})
	}
}

func TestPollingHelpers_ParentCancellation(t *testing.T) {
	t.Parallel()
	for _, tc := range pollingCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				entered := make(chan struct{})
				done := make(chan bool, 1)
				go func() {
					done <- tc.run(parent, time.Hour, func(ctx context.Context) error {
						close(entered)
						<-ctx.Done()
						return ctx.Err()
					})
				}()
				<-entered
				cancel()
				if <-done {
					t.Error("parent cancellation unexpectedly reported convergence")
				}
			})
		})
	}
}

func TestPollingHelpers_HealthyConvergence(t *testing.T) {
	t.Parallel()
	for _, tc := range pollingCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				var requestCtx context.Context
				if !tc.run(context.Background(), time.Minute, func(ctx context.Context) error {
					calls++
					requestCtx = ctx
					return nil
				}) {
					t.Error("healthy response did not converge")
				}
				if calls != 1 {
					t.Errorf("request count = %d, want 1", calls)
				}
				if requestCtx != nil && requestCtx.Err() != context.Canceled {
					t.Errorf("successful helper did not release its request context: %v", requestCtx.Err())
				}
			})
		})
	}
}

func TestPollingHelpers_ParentDeadline(t *testing.T) {
	t.Parallel()
	for _, tc := range pollingCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				parent, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				var requestCtx context.Context
				if tc.run(parent, time.Minute, func(ctx context.Context) error {
					requestCtx = ctx
					<-ctx.Done()
					return ctx.Err()
				}) {
					t.Error("expired parent unexpectedly reported convergence")
				}
				if requestCtx == nil || requestCtx.Err() != context.DeadlineExceeded {
					t.Error("in-flight request did not observe the parent's earlier deadline")
				}
				if elapsed := time.Since(start); elapsed != 8*time.Second {
					t.Errorf("elapsed = %s, want parent deadline of 8s", elapsed)
				}
			})
		})
	}
}

func TestPollingHelpers_RejectLateConvergence(t *testing.T) {
	t.Parallel()
	for _, tc := range pollingCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				if tc.run(context.Background(), 10*time.Second, func(context.Context) error {
					// Model a response that races cancellation: even if the client
					// delivers successful data, the helper's budget has expired.
					timer := time.NewTimer(20 * time.Second)
					defer timer.Stop()
					<-timer.C
					return nil
				}) {
					t.Error("helper reported convergence from a response delivered after its deadline")
				}
			})
		})
	}
}
