package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func rcStatefulSet(image string, converged bool) *appsv1.StatefulSet {
	replicas := int32(3)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Generation: 2},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{
				{Name: "ledger", Image: image},
			}}},
		},
		Status: appsv1.StatefulSetStatus{ObservedGeneration: 2, UpdatedReplicas: replicas, ReadyReplicas: replicas},
	}
	if !converged {
		sts.Status.ReadyReplicas--
	}

	return sts
}

func TestValidateRCUpgrade(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		target    string
		converged bool
		wantError string
	}{
		{"adjacent", "v3.0.0-rc.1", "v3.0.0-rc.2", true, ""},
		{"rolling rc.3", "v3.0.0-rc.2", "v3.0.0-rc.3", true, ""},
		{"stopped rc.4 remains adjacent", "v3.0.0-rc.3", "v3.0.0-rc.4", true, ""},
		{"generic release line", "v4.12.7-rc.9", "v4.12.7-rc.10", true, ""},
		{"skip", "v3.0.0-rc.1", "v3.0.0-rc.3", true, "immediately next RC"},
		{"downgrade", "v3.0.0-rc.2", "v3.0.0-rc.1", true, "immediately next RC"},
		{"different release line", "v3.0.0-rc.2", "v3.1.0-rc.1", true, "same release line"},
		{"source unknown", "latest", "v3.0.0-rc.2", true, "cannot verify source RC"},
		{"malformed target", "v3.0.0-rc.1", "v3.0.0-rc.02", true, "invalid target RC"},
		{"malformed RC suffix", "v3.0.0-rc.1", "v3.0.0-rc2", true, "invalid target RC"},
		{"unfinished rollout", "v3.0.0-rc.1", "v3.0.0-rc.2", false, "rollout is complete"},
		{"accepted target", "v3.0.0-rc.2", "v3.0.0-rc.2", false, ""},
		{"non-RC target", "v3.0.0-rc.2", "v3.0.0", true, ""},
		{"non-RC change", "latest", "latest-new", true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sts := rcStatefulSet("ghcr.io/formancehq/ledger:"+tt.source, tt.converged)
			err := validateRCUpgrade(tt.target, sts)
			if tt.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantError)
			}
		})
	}
}

func TestValidateRCUpgradeUnknownImage(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateRCUpgrade("v3.0.0-rc.2", nil))
	require.ErrorContains(t,
		validateRCUpgrade("v3.0.0-rc.2", rcStatefulSet("ghcr.io/formancehq/ledger@sha256:abc", true)),
		"cannot determine the running RC",
	)
	require.NoError(t,
		validateRCUpgrade("latest", rcStatefulSet("ghcr.io/formancehq/ledger@sha256:abc", true)),
	)
}

func TestReconcileRejectsSkippedRCBeforeChangingStatefulSet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cluster := newMinimalCluster()
	replicas := int32(3)
	cluster.Spec.Replicas = &replicas
	cluster.Spec.Image.Tag = "v3.0.0-rc.3"
	sts := controlledByCluster(rcStatefulSet("ghcr.io/formancehq/ledger:v3.0.0-rc.1", true), cluster)
	sts.Name = resourceName(cluster.Name)
	sts.Namespace = cluster.Namespace
	sts.CreationTimestamp = metav1.Now()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&ledgerv1alpha1.Cluster{}).
		WithObjects(cluster, sts).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme, OperatorVersion: "v3.0.0-rc.4"}
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	stsKey := types.NamespacedName{Name: sts.Name, Namespace: sts.Namespace}

	result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.Zero(t, result)
	require.NoError(t, c.Get(ctx, key, cluster))
	condition := meta.FindStatusCondition(cluster.Status.Conditions, "ConfigValid")
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Contains(t, condition.Message, "immediately next RC")
	require.Equal(t, "Error", cluster.Status.Phase)
	actual := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, stsKey, actual))
	require.Equal(t, sts.Spec, actual.Spec)
	require.Equal(t, sts.ResourceVersion, actual.ResourceVersion)
}

func TestReconcileRejectsSkippedRCWhenStatefulSetIsBeingRecreated(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cluster := newMinimalCluster()
	replicas := int32(3)
	cluster.Spec.Replicas = &replicas
	cluster.Spec.Image.Tag = "v3.0.0-rc.3"
	orphan := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName(cluster.Name) + "-0",
			Namespace: cluster.Namespace,
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "ledger", Image: "ghcr.io/formancehq/ledger:v3.0.0-rc.1",
		}}},
	}
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&ledgerv1alpha1.Cluster{}).
		WithObjects(append(retainedClaimSet(cluster, 3, ""), cluster, orphan)...).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme, OperatorVersion: "v3.0.0-rc.4"} // New reconciler after orphan deletion.
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	stsKey := types.NamespacedName{Name: resourceName(cluster.Name), Namespace: cluster.Namespace}

	result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.Zero(t, result)
	require.NoError(t, c.Get(ctx, key, cluster))
	condition := meta.FindStatusCondition(cluster.Status.Conditions, "ConfigValid")
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Contains(t, condition.Message, "restore the source image tag first")
	require.Equal(t, "Error", cluster.Status.Phase)
	sts := &appsv1.StatefulSet{}
	require.Error(t, c.Get(ctx, stsKey, sts))

	cluster.Spec.Image.Tag = "v3.0.0-rc.1"
	require.NoError(t, c.Update(ctx, cluster))
	_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, stsKey, sts))
	require.Equal(t, "ghcr.io/formancehq/ledger:v3.0.0-rc.1", sts.Spec.Template.Spec.Containers[0].Image)
}

func TestMissingStatefulSetWithPVCKeepsRCSourceUnknown(t *testing.T) {
	t.Parallel()

	cluster := newMinimalCluster()
	cluster.Spec.Image.Tag = "v3.0.0-rc.3"
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(retainedClaimSet(cluster, 3, "")...).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	require.ErrorContains(t, r.validateRCUpgradeWithoutStatefulSet(context.Background(), cluster), "cannot verify RC source")
}

func TestRCVolumeClaimRecreationWaitsForRollout(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cluster := newMinimalCluster()
	cluster.Spec.Image.Repository = "ghcr.io/formancehq/ledger"
	cluster.Spec.Image.Tag = "v3.0.0-rc.2"
	applyDefaults(cluster)
	old := cluster.DeepCopy()
	old.Spec.Image.Tag = "v3.0.0-rc.1"
	old.Spec.Persistence.Data.HostPath = &ledgerv1alpha1.HostPathVolumeSpec{Path: "/mnt/data"}
	sts := rcStatefulSet("ghcr.io/formancehq/ledger:v3.0.0-rc.1", true)
	sts.Spec = buildStatefulSetSpec(old, "old", nil, "disabled")
	sts.Name = resourceName(cluster.Name)
	sts.Namespace = cluster.Namespace
	sts.CreationTimestamp = metav1.Now()
	controlledByCluster(sts, cluster)
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&appsv1.StatefulSet{}).
		WithObjects(sts).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	key := types.NamespacedName{Name: sts.Name, Namespace: sts.Namespace}

	// The volume templates differ, but the first pass must publish rc.2
	// without orphaning the rc.1 Pods.
	_, err := r.reconcileStatefulSetWithExec(ctx, cluster, "hash", nil, nil)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, key, sts))
	require.Equal(t, "ghcr.io/formancehq/ledger:v3.0.0-rc.2", sts.Spec.Template.Spec.Containers[0].Image)
	require.Len(t, sts.Spec.VolumeClaimTemplates, 1)
	require.Equal(t, "wal", sts.Spec.VolumeClaimTemplates[0].Name)
	require.NotNil(t, storageVolume(sts.Spec.Template.Spec.Volumes, "data"))
	require.Equal(t, "$(POD_INDEX)", storageMount(sts.Spec.Template.Spec.Containers[0].VolumeMounts, "data").SubPathExpr)

	// An interrupted mixed-image rollout must not delete the StatefulSet.
	sts.Status.ObservedGeneration = sts.Generation
	sts.Status.UpdatedReplicas = 2
	sts.Status.ReadyReplicas = 2
	require.NoError(t, c.Status().Update(ctx, sts))
	r = &ClusterReconciler{Client: c, Scheme: scheme} // Restart the controller.
	_, err = r.reconcileStatefulSetWithExec(ctx, cluster, "hash", nil, nil)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, key, sts))
	require.Equal(t, "ghcr.io/formancehq/ledger:v3.0.0-rc.2", sts.Spec.Template.Spec.Containers[0].Image)

	// Only after all Pods use rc.2 may the volume-template path orphan them.
	sts.Status.ObservedGeneration = sts.Generation
	sts.Status.UpdatedReplicas = 3
	sts.Status.ReadyReplicas = 3
	require.NoError(t, c.Status().Update(ctx, sts))
	_, err = r.reconcileStatefulSetWithExec(ctx, cluster, "hash", nil, nil)
	require.NoError(t, err)
	require.True(t, apierrors.IsNotFound(c.Get(ctx, key, sts)))
}

func storageVolume(volumes []corev1.Volume, name string) *corev1.Volume {
	for i := range volumes {
		if volumes[i].Name == name {
			return &volumes[i]
		}
	}

	return nil
}

func storageMount(mounts []corev1.VolumeMount, name string) *corev1.VolumeMount {
	for i := range mounts {
		if mounts[i].Name == name {
			return &mounts[i]
		}
	}

	return nil
}

func recordedClaim(cluster *ledgerv1alpha1.Cluster, prefix string, ordinal int, tag string) *corev1.PersistentVolumeClaim {
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:      fmt.Sprintf("%s-%s-%d", prefix, resourceName(cluster.Name), ordinal),
		Namespace: cluster.Namespace,
		UID:       types.UID(fmt.Sprintf("%s-uid-%d", prefix, ordinal)),
	}}
	if tag != "" {
		claim.Annotations = map[string]string{annotationClaimSourceTag: tag}
	}

	return claim
}

// retainedClaimSet returns the WAL and data claims of every ordinal below
// replicas, as a Cluster deleted with deletion protection leaves them.
func retainedClaimSet(cluster *ledgerv1alpha1.Cluster, replicas int, tag string) []client.Object {
	var objects []client.Object
	for ordinal := range replicas {
		objects = append(objects, recordedClaim(cluster, "wal", ordinal, tag), recordedClaim(cluster, "data", ordinal, tag))
	}

	return objects
}

// A Cluster deleted with retained claims and recreated with the same RC tag
// proves its source from the claim record, without an orphan Pod.
func TestMissingStatefulSetWithRecordedClaimsValidatesRCSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		tags      []string // per ordinal, for both the WAL and data claim
		dataTag   string   // overrides the data claim of ordinal 0 when set
		wantError string
	}{
		{name: "every claim records the target", target: "v3.0.0-rc.4", tags: []string{"v3.0.0-rc.4", "v3.0.0-rc.4", "v3.0.0-rc.4"}},
		{name: "one claim has no record", target: "v3.0.0-rc.4", tags: []string{"v3.0.0-rc.4", "", "v3.0.0-rc.4"}, wantError: "without the ledger.formance.com/data-source-tag annotation"},
		{name: "claims record different tags", target: "v3.0.0-rc.4", tags: []string{"v3.0.0-rc.4", "v3.0.0-rc.4", "v3.0.0-rc.4"}, dataTag: "v3.0.0-rc.3", wantError: "record different source tags v3.0.0-rc.3, v3.0.0-rc.4"},
		{name: "adjacent RC needs a StatefulSet", target: "v3.0.0-rc.5", tags: []string{"v3.0.0-rc.4", "v3.0.0-rc.4", "v3.0.0-rc.4"}, wantError: "restore spec.image.tag to v3.0.0-rc.4"},
		{name: "downgrade", target: "v3.0.0-rc.3", tags: []string{"v3.0.0-rc.4", "v3.0.0-rc.4", "v3.0.0-rc.4"}, wantError: "record source v3.0.0-rc.4"},
		{name: "non-RC target is outside the rule", target: "latest", tags: []string{"", "", ""}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cluster := newMinimalCluster()
			cluster.Spec.Image.Tag = tt.target
			var objects []client.Object
			for ordinal, tag := range tt.tags {
				objects = append(objects, recordedClaim(cluster, "wal", ordinal, tag))
				dataTag := tag
				if ordinal == 0 && tt.dataTag != "" {
					dataTag = tt.dataTag
				}
				objects = append(objects, recordedClaim(cluster, "data", ordinal, dataTag))
			}
			scheme := runtime.NewScheme()
			require.NoError(t, clientgoscheme.AddToScheme(scheme))
			require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			r := &ClusterReconciler{Client: c, Scheme: scheme}

			err := r.validateRCUpgradeWithoutStatefulSet(context.Background(), cluster)
			if tt.wantError == "" {
				require.NoError(t, err)

				return
			}
			require.ErrorContains(t, err, tt.wantError)
		})
	}
}

// A deleting claim cannot prove the source, even when its record matches: the
// recreated Pods would mount its empty replacement, not the recorded data.
func TestClaimSourceValidationRejectsDeletingClaim(t *testing.T) {
	t.Parallel()

	cluster := newMinimalCluster()
	var claims []corev1.PersistentVolumeClaim
	for ordinal := range 3 {
		claims = append(claims,
			*recordedClaim(cluster, "wal", ordinal, "v3.0.0-rc.4"),
			*recordedClaim(cluster, "data", ordinal, "v3.0.0-rc.4"))
	}
	stsName := resourceName(cluster.Name)
	require.NoError(t, validateClaimSourceTags(stsName, "v3.0.0-rc.4", claims))

	deleting := metav1.Now()
	claims[3].DeletionTimestamp = &deleting
	require.ErrorContains(t, validateClaimSourceTags(stsName, "v3.0.0-rc.4", claims),
		fmt.Sprintf("PVC %q is being deleted", claims[3].Name))
}

// A matching orphan Pod proves the image, not the data: a deleting claim at
// another ordinal still blocks the recreation that would mount its empty
// replacement.
func TestOrphanPodRecreationRejectsDeletingClaim(t *testing.T) {
	t.Parallel()

	cluster := newMinimalCluster()
	cluster.Spec.Image.Tag = "v3.0.0-rc.4"
	orphan := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName(cluster.Name) + "-0", Namespace: cluster.Namespace},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "ledger", Image: "ghcr.io/formancehq/ledger:v3.0.0-rc.4",
		}}},
	}
	retained := retainedClaimSet(cluster, 3, "")
	deleting := retained[3].(*corev1.PersistentVolumeClaim) // data, ordinal 1
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))

	admitted := fake.NewClientBuilder().WithScheme(scheme).WithObjects(append(retained, orphan)...).Build()
	r := &ClusterReconciler{Client: admitted, Scheme: scheme}
	require.NoError(t, r.validateRCUpgradeWithoutStatefulSet(context.Background(), cluster))

	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	deleting.Finalizers = []string{"kubernetes.io/pvc-protection"}
	blocked := fake.NewClientBuilder().WithScheme(scheme).WithObjects(append(retained, orphan)...).Build()
	r = &ClusterReconciler{Client: blocked, Scheme: scheme}
	require.ErrorContains(t, r.validateRCUpgradeWithoutStatefulSet(context.Background(), cluster),
		fmt.Sprintf("PVC %q is being deleted", deleting.Name))
}

// PVCs are not watched, so the deleting-claim gate requeues itself. Once the
// deletion finishes, a claim of a member below the replica count is missing:
// the recreation stays refused rather than provisioning it empty. A deleting
// claim beyond the replica count only delays the recreation.
func TestReconcileDeletingClaimRequeuesThenBlocksMissingMember(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name        string
		ordinal     int
		wantCreated bool
	}{
		{name: "member claim", ordinal: 1},
		{name: "claim beyond the replica count", ordinal: 3, wantCreated: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			cluster := newMinimalCluster()
			cluster.Spec.Image.Tag = "v3.0.0-rc.4"
			orphan := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName(cluster.Name) + "-0", Namespace: cluster.Namespace},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name: "ledger", Image: "ghcr.io/formancehq/ledger:v3.0.0-rc.4",
				}}},
			}
			objects := retainedClaimSet(cluster, 4, "")
			deleting := objects[2*tt.ordinal+1].(*corev1.PersistentVolumeClaim) // data claim
			now := metav1.Now()
			deleting.DeletionTimestamp = &now
			deleting.Finalizers = []string{"kubernetes.io/pvc-protection"}
			scheme := runtime.NewScheme()
			require.NoError(t, clientgoscheme.AddToScheme(scheme))
			require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&ledgerv1alpha1.Cluster{}).
				WithObjects(append(objects, cluster, orphan)...).Build()
			r := &ClusterReconciler{Client: c, Scheme: scheme, OperatorVersion: "v3.0.0-rc.4"}
			key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
			stsKey := types.NamespacedName{Name: resourceName(cluster.Name), Namespace: cluster.Namespace}

			result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			require.NoError(t, err)
			require.Equal(t, rcClaimDeletionRequeueInterval, result.RequeueAfter)
			require.NoError(t, c.Get(ctx, key, cluster))
			condition := meta.FindStatusCondition(cluster.Status.Conditions, "ConfigValid")
			require.NotNil(t, condition)
			require.Equal(t, metav1.ConditionFalse, condition.Status)
			require.Contains(t, condition.Message, fmt.Sprintf("PVC %q is being deleted", deleting.Name))
			require.True(t, apierrors.IsNotFound(c.Get(ctx, stsKey, &appsv1.StatefulSet{})))

			// The deletion finishes once the finalizer is released.
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(deleting), deleting))
			deleting.Finalizers = nil
			require.NoError(t, c.Update(ctx, deleting))
			result, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			require.NoError(t, err)
			if tt.wantCreated {
				require.NoError(t, c.Get(ctx, stsKey, &appsv1.StatefulSet{}))

				return
			}
			require.Zero(t, result, "a missing member claim needs a decision, not a retry")
			require.NoError(t, c.Get(ctx, key, cluster))
			condition = meta.FindStatusCondition(cluster.Status.Conditions, "ConfigValid")
			require.NotNil(t, condition)
			require.Equal(t, metav1.ConditionFalse, condition.Status)
			require.Contains(t, condition.Message, "missing PVC(s) "+deleting.Name)
			require.True(t, apierrors.IsNotFound(c.Get(ctx, stsKey, &appsv1.StatefulSet{})),
				"the empty replacement must not be admitted")
		})
	}
}

// Evidence of a previous cluster requires every claim the recreated Pods will
// mount. Only PVC-backed volumes count, and a recreation that scales up is
// refused because a never-created claim cannot be told from a lost one.
func TestRecreationRejectsMissingClaims(t *testing.T) {
	t.Parallel()

	hostPathData := func(cluster *ledgerv1alpha1.Cluster) {
		cluster.Spec.Persistence.Data = ledgerv1alpha1.VolumeSpec{
			HostPath: &ledgerv1alpha1.HostPathVolumeSpec{Path: "/mnt/nvme0/data"},
		}
	}
	for _, tt := range []struct {
		name        string
		replicas    int32
		retained    int  // ordinals with retained claims
		walOnly     bool // keep only the WAL claims of those ordinals
		orphanPods  bool
		mutate      func(*ledgerv1alpha1.Cluster)
		wantMissing string
	}{
		{name: "no evidence creates from scratch", replicas: 3},
		{name: "complete set", replicas: 3, retained: 3},
		{name: "orphan Pods without claims", replicas: 1, orphanPods: true, wantMissing: "wal-ledger-test-0, data-ledger-test-0"},
		{name: "scale-up during recreation", replicas: 3, retained: 2, wantMissing: "wal-ledger-test-2, data-ledger-test-2"},
		{name: "scale-down during recreation", replicas: 1, retained: 3},
		{name: "hostPath data needs only WAL claims", replicas: 3, retained: 3, walOnly: true, mutate: hostPathData},
		{name: "PVC data needs data claims", replicas: 1, retained: 1, walOnly: true, wantMissing: "data-ledger-test-0"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cluster := newMinimalCluster()
			cluster.Spec.Replicas = &tt.replicas
			if tt.mutate != nil {
				tt.mutate(cluster)
			}
			var claims []corev1.PersistentVolumeClaim
			for ordinal := range tt.retained {
				claims = append(claims, *recordedClaim(cluster, "wal", ordinal, ""))
				if !tt.walOnly {
					claims = append(claims, *recordedClaim(cluster, "data", ordinal, ""))
				}
			}

			err := rejectMissingClaims(cluster, tt.orphanPods, claims)
			if tt.wantMissing == "" {
				require.NoError(t, err)

				return
			}
			require.ErrorIs(t, err, errRCClaimMissing)
			require.ErrorContains(t, err, "missing PVC(s) "+tt.wantMissing+" would be provisioned empty")
		})
	}
}

// An API failure while reading the RC evidence is not a verdict on the spec:
// Reconcile returns it for controller backoff instead of parking the Cluster
// in Error without a requeue.
func TestReconcileRetriesRCEvidenceLookupFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cluster := newMinimalCluster()
	cluster.Spec.Image.Tag = "v3.0.0-rc.4"
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	for _, failing := range []client.ObjectList{&corev1.PodList{}, &corev1.PersistentVolumeClaimList{}} {
		c := fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&ledgerv1alpha1.Cluster{}).
			WithObjects(cluster.DeepCopy()).
			WithInterceptorFuncs(interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if fmt.Sprintf("%T", list) == fmt.Sprintf("%T", failing) {
					return apierrors.NewServiceUnavailable("injected")
				}

				return c.List(ctx, list, opts...)
			}}).Build()
		r := &ClusterReconciler{Client: c, Scheme: scheme, OperatorVersion: "v3.0.0-rc.4"}
		key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}

		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		require.ErrorIs(t, err, errRCValidationUnavailable, "%T", failing)
		require.True(t, apierrors.IsServiceUnavailable(err), "%T", failing)
		stored := &ledgerv1alpha1.Cluster{}
		require.NoError(t, c.Get(ctx, key, stored))
		require.Nil(t, meta.FindStatusCondition(stored.Status.Conditions, "ConfigValid"), "%T", failing)
		require.True(t, apierrors.IsNotFound(c.Get(ctx, types.NamespacedName{Name: resourceName(cluster.Name), Namespace: cluster.Namespace}, &appsv1.StatefulSet{})))
	}
}

// The record is read through the uncached API reader: a stale cache must not
// show a record that the operator already cleared.
func TestMissingStatefulSetReadsClaimRecordsThroughAPIReader(t *testing.T) {
	t.Parallel()

	cluster := newMinimalCluster()
	cluster.Spec.Image.Tag = "v3.0.0-rc.4"
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	cluster.Spec.Replicas = new(int32(1))
	cached := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(retainedClaimSet(cluster, 1, "v3.0.0-rc.4")...).Build()
	live := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(recordedClaim(cluster, "wal", 0, ""), recordedClaim(cluster, "data", 0, "v3.0.0-rc.4")).Build()
	r := &ClusterReconciler{Client: cached, APIReader: live, Scheme: scheme}

	require.ErrorContains(t, r.validateRCUpgradeWithoutStatefulSet(context.Background(), cluster), "cannot verify RC source")
}
