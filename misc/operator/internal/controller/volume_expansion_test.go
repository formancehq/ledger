package controller

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestParsePodDiskUsage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload string
		want    podDiskUsage
		wantErr string
	}{
		{
			name:    "protobuf strings",
			payload: `{"walVolume":{"usedBytes":"10","totalBytes":"100"},"dataVolume":{"usedBytes":"20","totalBytes":"200"}}`,
			want:    podDiskUsage{WAL: measuredVolume{UsedBytes: 10, TotalBytes: 100}, Data: measuredVolume{UsedBytes: 20, TotalBytes: 200}},
		},
		{
			name:    "JSON numbers",
			payload: `{"walVolume":{"usedBytes":10,"totalBytes":100},"dataVolume":{"usedBytes":20,"totalBytes":200}}`,
			want:    podDiskUsage{WAL: measuredVolume{UsedBytes: 10, TotalBytes: 100}, Data: measuredVolume{UsedBytes: 20, TotalBytes: 200}},
		},
		{
			name:    "zero total",
			payload: `{"walVolume":{"usedBytes":"10","totalBytes":"0"},"dataVolume":{"usedBytes":"20","totalBytes":"200"}}`,
			wantErr: "zero totalBytes",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parsePodDiskUsage([]byte(tt.payload))
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestVolumeExpansionReconcilerExpandsAllLiveReplicas(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, storagev1.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))

	allowExpansion := true
	storageClassName := "gp3-expandable"
	maximum := resource.MustParse("200Gi")
	rejectedSpecReplicas := int32(2)
	liveReplicas := int32(3)
	ledger := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: ledgerv1alpha1.ClusterSpec{
			Replicas: &rejectedSpecReplicas,
			Persistence: ledgerv1alpha1.PersistenceSpec{
				Data: ledgerv1alpha1.VolumeSpec{
					AutoExpansion: &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: &maximum},
				},
			},
		},
	}
	objects := []runtime.Object{
		ledger,
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "ledger-test", Namespace: "default"},
			Spec:       appsv1.StatefulSetSpec{Replicas: &liveReplicas},
		},
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: storageClassName}, AllowVolumeExpansion: &allowExpansion},
	}
	for ordinal := range liveReplicas {
		objects = append(objects, boundTestPVC("data-ledger-test-"+strconv.Itoa(int(ordinal)), storageClassName, "100Gi"))
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build()
	recorder := record.NewFakeRecorder(10)
	reconciler := &VolumeExpansionReconciler{
		Client:    k8sClient,
		APIReader: k8sClient,
		Recorder:  recorder,
		Now:       func() time.Time { return now },
		ReadDiskUsage: func(_ context.Context, _ *ledgerv1alpha1.Cluster, pod, _ string) (podDiskUsage, error) {
			if strings.HasSuffix(pod, "-2") {
				return podDiskUsage{}, errors.New("replica unavailable")
			}
			used := uint64(testQuantityValue("60Gi"))
			if strings.HasSuffix(pod, "-1") {
				used = uint64(testQuantityValue("80Gi"))
			}

			return podDiskUsage{
				WAL:  measuredVolume{UsedBytes: 1, TotalBytes: 100},
				Data: measuredVolume{UsedBytes: used, TotalBytes: uint64(testQuantityValue("100Gi"))},
			}, nil
		},
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ledger.Namespace, Name: ledger.Name}})
	require.NoError(t, err)
	assert.Equal(t, volumeExpansionRetryInterval, result.RequeueAfter)

	for ordinal := range liveReplicas {
		name := "data-ledger-test-" + strconv.Itoa(int(ordinal))
		var pvc corev1.PersistentVolumeClaim
		require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, &pvc))
		request := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		assert.Equal(t, "146Gi", request.String())
		assert.Equal(t, now.Format(time.RFC3339), pvc.Annotations[annotationLastExpansionAt])
		assert.Equal(t, "146Gi", pvc.Annotations[annotationLastExpansionTarget])
	}

	events := make([]string, 0, 2)
	for len(events) < 2 {
		select {
		case event := <-recorder.Events:
			events = append(events, event)
		case <-time.After(time.Second):
			t.Fatalf("expected measurement failure and expansion events, got %v", events)
		}
	}
	assert.Condition(t, func() bool {
		return strings.Contains(events[0], "VolumeExpansionMeasurementFailed") || strings.Contains(events[1], "VolumeExpansionMeasurementFailed")
	})
	assert.Condition(t, func() bool {
		return strings.Contains(events[0], "VolumeExpansionRequested") || strings.Contains(events[1], "VolumeExpansionRequested")
	})
}

func TestVolumeExpansionReconcilerRejectsNonExpandableStorageClass(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, storagev1.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))

	storageClassName := "fixed"
	maximum := resource.MustParse("200Gi")
	replicas := int32(1)
	ledger := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "fixed", Namespace: "default"},
		Spec: ledgerv1alpha1.ClusterSpec{
			Replicas: &replicas,
			Persistence: ledgerv1alpha1.PersistenceSpec{
				Data: ledgerv1alpha1.VolumeSpec{AutoExpansion: &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: &maximum}},
			},
		},
	}
	pvc := boundTestPVC("data-ledger-fixed-0", storageClassName, "100Gi")
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(
		ledger,
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: storageClassName}},
		pvc,
	).Build()
	reconciler := &VolumeExpansionReconciler{
		Client:    k8sClient,
		APIReader: k8sClient,
		Recorder:  record.NewFakeRecorder(10),
		ReadDiskUsage: func(context.Context, *ledgerv1alpha1.Cluster, string, string) (podDiskUsage, error) {
			return podDiskUsage{}, errors.New("must not be called")
		},
	}

	retry, err := reconciler.reconcileVolume(ctx, ledger, volumeExpansionDefinition{Name: "data", Policy: ledger.Spec.Persistence.Data.AutoExpansion}, "disabled", replicas)
	require.NoError(t, err)
	assert.True(t, retry)

	var got corev1.PersistentVolumeClaim
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: pvc.Name}, &got))
	request := got.Spec.Resources.Requests[corev1.ResourceStorage]
	assert.Equal(t, "100Gi", request.String())
}

func TestEnabledVolumeExpansionDefinitionsIsStrictlyOptIn(t *testing.T) {
	t.Parallel()

	ledger := &ledgerv1alpha1.Cluster{}
	assert.Empty(t, enabledVolumeExpansionDefinitions(ledger))

	ledger.Spec.Persistence.Data.AutoExpansion = &ledgerv1alpha1.VolumeAutoExpansionSpec{}
	assert.Empty(t, enabledVolumeExpansionDefinitions(ledger))
}

func boundTestPVC(name, storageClass, size string) *corev1.PersistentVolumeClaim {
	quantity := resource.MustParse(size)

	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &storageClass,
			VolumeName:       "pv-" + name,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: quantity},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase:    corev1.ClaimBound,
			Capacity: corev1.ResourceList{corev1.ResourceStorage: quantity},
		},
	}
}
