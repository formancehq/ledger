package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func sourceRecordScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))

	return scheme
}

func sourceRecordCluster(tag string) *ledgerv1alpha1.Cluster {
	cluster := newMinimalCluster()
	cluster.UID = types.UID("cluster-uid")
	cluster.Spec.Image.Repository = "ghcr.io/formancehq/ledger"
	cluster.Spec.Image.Tag = tag
	applyDefaults(cluster)

	return cluster
}

// sourceRecordStatefulSet builds the owned live StatefulSet of cluster, as the
// operator would have published it for tag, with the given rollout state.
func sourceRecordStatefulSet(cluster *ledgerv1alpha1.Cluster, tag string, converged bool) *appsv1.StatefulSet {
	published := cluster.DeepCopy()
	published.Spec.Image.Tag = tag
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: resourceName(cluster.Name), Namespace: cluster.Namespace,
		Generation: 2, CreationTimestamp: metav1.Now(),
	}}
	sts.Spec = buildStatefulSetSpec(published, "hash", nil, "disabled")
	controlledByCluster(sts, cluster)
	replicas := *sts.Spec.Replicas
	sts.Status = appsv1.StatefulSetStatus{
		ObservedGeneration: 2, Replicas: replicas, UpdatedReplicas: replicas, ReadyReplicas: replicas,
		CurrentRevision: "rev-2", UpdateRevision: "rev-2",
	}
	if !converged {
		sts.Status.UpdatedReplicas--
		sts.Status.CurrentRevision = "rev-1"
	}

	return sts
}

func claimRecord(t *testing.T, c client.Client, cluster *ledgerv1alpha1.Cluster, prefix string, ordinal int) (string, bool) {
	t.Helper()
	claim := &corev1.PersistentVolumeClaim{}
	name := recordedClaim(cluster, prefix, ordinal, "").Name
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: cluster.Namespace}, claim))
	tag, ok := claim.Annotations[annotationClaimSourceTag]

	return tag, ok
}

func TestClaimSourceRecordedOnlyAfterConvergence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cluster := sourceRecordCluster("v3.0.0-rc.4")
	sts := sourceRecordStatefulSet(cluster, "v3.0.0-rc.4", false)
	objects := []client.Object{sts,
		recordedClaim(cluster, "wal", 0, ""), recordedClaim(cluster, "data", 0, ""),
		recordedClaim(cluster, "wal", 1, ""), recordedClaim(cluster, "data", 1, ""),
		recordedClaim(cluster, "wal", 2, ""), recordedClaim(cluster, "data", 2, ""),
		// Retained after an earlier scale-down: no Pod mounts these claims.
		recordedClaim(cluster, "wal", 3, ""), recordedClaim(cluster, "data", 3, "v3.0.0-rc.3"),
	}
	scheme := sourceRecordScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&appsv1.StatefulSet{}).WithObjects(objects...).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	// A rollout that has not converged proves nothing about the claims.
	require.NoError(t, r.reconcileClaimSourceTags(ctx, cluster))
	_, ok := claimRecord(t, c, cluster, "wal", 0)
	require.False(t, ok)

	// Counts may look complete while two revisions still coexist.
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(sts), sts))
	sts.Status.UpdatedReplicas = *sts.Spec.Replicas
	require.NoError(t, c.Status().Update(ctx, sts))
	require.NoError(t, r.reconcileClaimSourceTags(ctx, cluster))
	_, ok = claimRecord(t, c, cluster, "data", 2)
	require.False(t, ok)

	sts.Status.CurrentRevision = sts.Status.UpdateRevision
	require.NoError(t, c.Status().Update(ctx, sts))
	require.NoError(t, r.reconcileClaimSourceTags(ctx, cluster))
	for ordinal := range 3 {
		for _, prefix := range []string{"wal", "data"} {
			tag, ok := claimRecord(t, c, cluster, prefix, ordinal)
			require.True(t, ok, "%s-%d", prefix, ordinal)
			require.Equal(t, "v3.0.0-rc.4", tag)
		}
	}
	_, ok = claimRecord(t, c, cluster, "wal", 3)
	require.False(t, ok, "an ordinal beyond the replica count is not mounted and is not recorded")
	tag, _ := claimRecord(t, c, cluster, "data", 3)
	require.Equal(t, "v3.0.0-rc.3", tag, "an existing out-of-range record is left as it is")
}

// A claim retained after its volume moved to hostPath is not mounted by the
// converged Pods. Recording their tag on it would let a later recreation on
// PVC storage accept data that those Pods never wrote.
func TestClaimSourceRecordSkipsClaimsWithoutTemplate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cluster := sourceRecordCluster("v3.0.0-rc.4")
	cluster.Spec.Persistence.Data = ledgerv1alpha1.VolumeSpec{
		HostPath: &ledgerv1alpha1.HostPathVolumeSpec{Path: "/mnt/nvme0/data"},
	}
	sts := sourceRecordStatefulSet(cluster, "v3.0.0-rc.4", true)
	for _, template := range sts.Spec.VolumeClaimTemplates {
		require.NotEqual(t, "data", template.Name, "precondition: data is hostPath-backed")
	}
	objects := []client.Object{sts}
	for ordinal := range 3 {
		objects = append(objects,
			recordedClaim(cluster, "wal", ordinal, ""),
			// Retained from the earlier PVC-backed data volume, last written by rc.1.
			recordedClaim(cluster, "data", ordinal, ""))
	}
	scheme := sourceRecordScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&appsv1.StatefulSet{}).WithObjects(objects...).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	require.NoError(t, r.reconcileClaimSourceTags(ctx, cluster))
	for ordinal := range 3 {
		tag, ok := claimRecord(t, c, cluster, "wal", ordinal)
		require.True(t, ok, "wal-%d is mounted and recorded", ordinal)
		require.Equal(t, "v3.0.0-rc.4", tag)
		_, ok = claimRecord(t, c, cluster, "data", ordinal)
		require.False(t, ok, "data-%d has no claim template and is not recorded", ordinal)
	}
}

// The stamp reads the StatefulSet through the API reader. A cache that still
// shows the previous converged template after a new publish must not re-stamp
// the previous tag.
func TestClaimSourceRecordIgnoresStaleCachedRollout(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cluster := sourceRecordCluster("v3.0.0-rc.5")
	scheme := sourceRecordScheme(t)
	cached := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(sourceRecordStatefulSet(cluster, "v3.0.0-rc.4", true), recordedClaim(cluster, "wal", 0, "")).Build()
	live := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(sourceRecordStatefulSet(cluster, "v3.0.0-rc.5", false), recordedClaim(cluster, "wal", 0, "")).Build()
	r := &ClusterReconciler{Client: cached, APIReader: live, Scheme: scheme}

	require.NoError(t, r.reconcileClaimSourceTags(ctx, cluster))
	_, ok := claimRecord(t, cached, cluster, "wal", 0)
	require.False(t, ok)
}

func TestClaimSourceRecordSkipsForeignStatefulSet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cluster := sourceRecordCluster("v3.0.0-rc.4")
	sts := sourceRecordStatefulSet(cluster, "v3.0.0-rc.4", true)
	sts.OwnerReferences = nil
	scheme := sourceRecordScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts, recordedClaim(cluster, "wal", 0, "")).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	require.NoError(t, r.reconcileClaimSourceTags(ctx, cluster))
	_, ok := claimRecord(t, c, cluster, "wal", 0)
	require.False(t, ok)
}

func TestPublishingAnotherTagClearsClaimSourceFirst(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cluster := sourceRecordCluster("v3.0.0-rc.5")
	scheme := sourceRecordScheme(t)
	key := types.NamespacedName{Name: resourceName(cluster.Name), Namespace: cluster.Namespace}

	t.Run("same tag keeps the record", func(t *testing.T) {
		t.Parallel()
		same := sourceRecordCluster("v3.0.0-rc.4")
		c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&appsv1.StatefulSet{}).
			WithObjects(sourceRecordStatefulSet(same, "v3.0.0-rc.4", true),
				recordedClaim(same, "wal", 0, "v3.0.0-rc.4"), recordedClaim(same, "data", 0, "v3.0.0-rc.4")).Build()
		r := &ClusterReconciler{Client: c, Scheme: scheme}

		_, err := r.upsertStatefulSet(ctx, same, buildStatefulSetSpec(same, "hash", nil, "disabled"), "hash", nil)
		require.NoError(t, err)
		tag, _ := claimRecord(t, c, same, "wal", 0)
		require.Equal(t, "v3.0.0-rc.4", tag)
	})

	t.Run("new tag clears the record before publishing", func(t *testing.T) {
		t.Parallel()
		c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&appsv1.StatefulSet{}).
			WithObjects(sourceRecordStatefulSet(cluster, "v3.0.0-rc.4", true),
				recordedClaim(cluster, "wal", 0, "v3.0.0-rc.4"), recordedClaim(cluster, "data", 0, "v3.0.0-rc.4"),
				recordedClaim(cluster, "data", 3, "v3.0.0-rc.4")).Build()
		r := &ClusterReconciler{Client: c, Scheme: scheme}

		sts, err := r.upsertStatefulSet(ctx, cluster, buildStatefulSetSpec(cluster, "hash", nil, "disabled"), "hash", nil)
		require.NoError(t, err)
		require.Equal(t, "ghcr.io/formancehq/ledger:v3.0.0-rc.5", sts.Spec.Template.Spec.Containers[0].Image)
		for _, claim := range []struct {
			prefix  string
			ordinal int
		}{{"wal", 0}, {"data", 0}, {"data", 3}} {
			_, ok := claimRecord(t, c, cluster, claim.prefix, claim.ordinal)
			require.False(t, ok, "%s-%d", claim.prefix, claim.ordinal)
		}
	})

	t.Run("a failed clear does not publish", func(t *testing.T) {
		t.Parallel()
		failure := errors.New("claim patch unavailable")
		base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&appsv1.StatefulSet{}).
			WithObjects(sourceRecordStatefulSet(cluster, "v3.0.0-rc.4", true),
				recordedClaim(cluster, "wal", 0, "v3.0.0-rc.4")).Build()
		c := interceptor.NewClient(base, interceptor.Funcs{Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
				return failure
			}

			return cl.Patch(ctx, obj, patch, opts...)
		}})
		r := &ClusterReconciler{Client: c, Scheme: scheme}

		_, err := r.upsertStatefulSet(ctx, cluster, buildStatefulSetSpec(cluster, "hash", nil, "disabled"), "hash", nil)
		require.ErrorIs(t, err, failure)
		live := &appsv1.StatefulSet{}
		require.NoError(t, base.Get(ctx, key, live))
		require.Equal(t, "ghcr.io/formancehq/ledger:v3.0.0-rc.4", live.Spec.Template.Spec.Containers[0].Image)
		tag, _ := claimRecord(t, base, cluster, "wal", 0)
		require.Equal(t, "v3.0.0-rc.4", tag)
	})

	t.Run("restoring a manually edited image clears an equal record", func(t *testing.T) {
		t.Parallel()
		restored := sourceRecordCluster("v3.0.0-rc.4")
		c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&appsv1.StatefulSet{}).
			WithObjects(sourceRecordStatefulSet(restored, "v3.0.0-rc.5", false),
				recordedClaim(restored, "wal", 0, "v3.0.0-rc.4")).Build()
		r := &ClusterReconciler{Client: c, Scheme: scheme}

		sts, err := r.upsertStatefulSet(ctx, restored, buildStatefulSetSpec(restored, "hash", nil, "disabled"), "hash", nil)
		require.NoError(t, err)
		require.Equal(t, "ghcr.io/formancehq/ledger:v3.0.0-rc.4", sts.Spec.Template.Spec.Containers[0].Image)
		_, ok := claimRecord(t, c, restored, "wal", 0)
		require.False(t, ok, "rc.5 Pods may have mounted the claim before the restore")
	})

	t.Run("creating over recorded claims clears another tag", func(t *testing.T) {
		t.Parallel()
		c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&appsv1.StatefulSet{}).
			WithObjects(recordedClaim(cluster, "wal", 0, "v3.0.0-rc.4")).Build()
		r := &ClusterReconciler{Client: c, Scheme: scheme}

		_, err := r.upsertStatefulSet(ctx, cluster, buildStatefulSetSpec(cluster, "hash", nil, "disabled"), "hash", nil)
		require.NoError(t, err)
		_, ok := claimRecord(t, c, cluster, "wal", 0)
		require.False(t, ok)
	})
}

// End to end without an orphan Pod: a converged Cluster records its source,
// the Cluster and StatefulSet disappear with the claims retained, and a new
// Cluster with the same tag recreates the workload.
func TestRecreatedClusterStartsFromRecordedClaims(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cluster := sourceRecordCluster("v3.0.0-rc.4")
	cluster.Spec.Replicas = new(int32(1))
	sts := sourceRecordStatefulSet(cluster, "v3.0.0-rc.4", true)
	scheme := sourceRecordScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&appsv1.StatefulSet{}).
		WithObjects(sts, recordedClaim(cluster, "wal", 0, ""), recordedClaim(cluster, "data", 0, "")).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	require.NoError(t, r.reconcileClaimSourceTags(ctx, cluster))
	require.NoError(t, c.Delete(ctx, sts))

	recreated := sourceRecordCluster("v3.0.0-rc.4")
	recreated.UID = types.UID("recreated-cluster-uid")
	require.ErrorIs(t, r.validateRCUpgradeWithoutStatefulSet(ctx, recreated), errRCClaimMissing,
		"recreating at the default 3 replicas over a 1-replica claim set provisions empty members")
	recreated.Spec.Replicas = new(int32(1))
	require.NoError(t, r.validateRCUpgradeWithoutStatefulSet(ctx, recreated))
	recreated.Spec.Image.Tag = "v3.0.0-rc.5"
	require.ErrorContains(t, r.validateRCUpgradeWithoutStatefulSet(ctx, recreated), "restore spec.image.tag to v3.0.0-rc.4")
}
