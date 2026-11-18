//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// TestClusterPolicyRevisionAdmission covers the transition rules on
// ClusterSpec: the leader ignores a cluster-policy change whose revision does
// not exceed the applied one, so the API server must refuse it up front.
func TestClusterPolicyRevisionAdmission(t *testing.T) {
	t.Parallel()

	const bumpMessage = "requires increasing clusterPolicyRevision"
	const decreaseMessage = "clusterPolicyRevision must not decrease"

	for _, tc := range []struct {
		name    string
		initial func(*ledgerv1alpha1.ClusterSpec)
		mutate  func(*ledgerv1alpha1.ClusterSpec)
		reject  string // expected message fragment; empty means the update is accepted
	}{
		{
			name:   "ttl change without bump",
			mutate: func(s *ledgerv1alpha1.ClusterSpec) { s.IdempotencyTTL = "48h" },
			reject: bumpMessage,
		},
		{
			name: "ttl change with bump from default revision",
			mutate: func(s *ledgerv1alpha1.ClusterSpec) {
				s.IdempotencyTTL = "48h"
				s.ClusterPolicyRevision = ptr.To[int64](2)
			},
		},
		{
			name: "ttl change with same explicit revision",
			initial: func(s *ledgerv1alpha1.ClusterSpec) {
				s.IdempotencyTTL = "24h"
				s.ClusterPolicyRevision = ptr.To[int64](3)
			},
			mutate: func(s *ledgerv1alpha1.ClusterSpec) { s.IdempotencyTTL = "0" },
			reject: bumpMessage,
		},
		{
			name:    "ttl removed without bump",
			initial: func(s *ledgerv1alpha1.ClusterSpec) { s.IdempotencyTTL = "1h" },
			mutate:  func(s *ledgerv1alpha1.ClusterSpec) { s.IdempotencyTTL = "" },
			reject:  bumpMessage,
		},
		{
			name:   "metadata limit change without bump",
			mutate: func(s *ledgerv1alpha1.ClusterSpec) { s.MetadataMaxEntries = ptr.To[int64](64) },
			reject: bumpMessage,
		},
		{
			name:    "metadata limit change with bump",
			initial: func(s *ledgerv1alpha1.ClusterSpec) { s.MetadataMaxKeyBytes = ptr.To[int64](256) },
			mutate: func(s *ledgerv1alpha1.ClusterSpec) {
				s.MetadataMaxKeyBytes = ptr.To[int64](512)
				s.ClusterPolicyRevision = ptr.To[int64](2)
			},
		},
		{
			name:    "revision decrease",
			initial: func(s *ledgerv1alpha1.ClusterSpec) { s.ClusterPolicyRevision = ptr.To[int64](5) },
			mutate:  func(s *ledgerv1alpha1.ClusterSpec) { s.ClusterPolicyRevision = ptr.To[int64](4) },
			reject:  decreaseMessage,
		},
		{
			name:    "revision removed below explicit value",
			initial: func(s *ledgerv1alpha1.ClusterSpec) { s.ClusterPolicyRevision = ptr.To[int64](2) },
			mutate:  func(s *ledgerv1alpha1.ClusterSpec) { s.ClusterPolicyRevision = nil },
			reject:  decreaseMessage,
		},
		{
			name:   "revision bump alone",
			mutate: func(s *ledgerv1alpha1.ClusterSpec) { s.ClusterPolicyRevision = ptr.To[int64](2) },
		},
		{
			name:   "non-policy change without bump",
			mutate: func(s *ledgerv1alpha1.ClusterSpec) { s.NumscriptCacheSize = ptr.To[int32](512) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cluster := newCluster("policy-revision", createTestNamespace(t))
			if tc.initial != nil {
				tc.initial(&cluster.Spec)
			}
			require.NoError(t, k8sClient.Create(ctx, cluster))

			key := client.ObjectKeyFromObject(cluster)
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				var current ledgerv1alpha1.Cluster
				if err := k8sClient.Get(ctx, key, &current); err != nil {
					return err
				}
				tc.mutate(&current.Spec)

				return k8sClient.Update(ctx, &current)
			})

			if tc.reject == "" {
				require.NoError(t, err)

				return
			}
			require.True(t, apierrors.IsInvalid(err), "update must fail API admission: %v", err)
			require.Contains(t, err.Error(), tc.reject)
		})
	}
}
