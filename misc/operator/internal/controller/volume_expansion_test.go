package controller

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

type failNthPatchClient struct {
	client.Client

	failAt   int
	attempts int
}

func (c *failNthPatchClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	c.attempts++
	if c.attempts == c.failAt {
		return errors.New("injected PVC patch failure")
	}

	return c.Client.Patch(ctx, object, patch, options...)
}

func TestParsePodDiskUsage(t *testing.T) {
	t.Parallel()
	observedAt := time.UnixMicro(123456789).UTC()

	tests := []struct {
		name    string
		payload string
		want    podDiskUsage
		wantErr string
	}{
		{
			name:    "protobuf strings",
			payload: `{"walVolume":{"usedBytes":"10","totalBytes":"100","observedAtUs":"123456789","sampleAgeMs":"250","valid":true},"dataVolume":{"usedBytes":"20","totalBytes":"200","observedAtUs":"123456789","sampleAgeMs":"500","valid":true}}`,
			want: podDiskUsage{
				WAL:  measuredVolume{UsedBytes: 10, TotalBytes: 100, ObservedAt: observedAt, SampleAge: 250 * time.Millisecond, Valid: true},
				Data: measuredVolume{UsedBytes: 20, TotalBytes: 200, ObservedAt: observedAt, SampleAge: 500 * time.Millisecond, Valid: true},
			},
		},
		{
			name:    "JSON numbers",
			payload: `{"walVolume":{"usedBytes":10,"totalBytes":100,"observedAtUs":123456789,"sampleAgeMs":250,"valid":true},"dataVolume":{"usedBytes":20,"totalBytes":200,"observedAtUs":123456789,"sampleAgeMs":500,"valid":true}}`,
			want: podDiskUsage{
				WAL:  measuredVolume{UsedBytes: 10, TotalBytes: 100, ObservedAt: observedAt, SampleAge: 250 * time.Millisecond, Valid: true},
				Data: measuredVolume{UsedBytes: 20, TotalBytes: 200, ObservedAt: observedAt, SampleAge: 500 * time.Millisecond, Valid: true},
			},
		},
		{
			name:    "timestamp overflow",
			payload: `{"walVolume":{"observedAtUs":"18446744073709551615"}}`,
			wantErr: "observedAtUs exceeds int64",
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

func TestValidateMeasuredVolume(t *testing.T) {
	t.Parallel()

	fresh := measuredVolume{
		UsedBytes:  80,
		TotalBytes: 100,
		ObservedAt: time.Now(),
		SampleAge:  maximumDiskUsageSampleAge,
		Valid:      true,
	}
	require.NoError(t, validateMeasuredVolume(fresh))

	tests := []struct {
		name    string
		mutate  func(*measuredVolume)
		wantErr string
	}{
		{name: "failed collection", mutate: func(v *measuredVolume) { v.Valid = false; v.Error = "input/output error" }, wantErr: "input/output error"},
		{name: "zero total", mutate: func(v *measuredVolume) { v.TotalBytes = 0 }, wantErr: "totalBytes"},
		{name: "missing timestamp", mutate: func(v *measuredVolume) { v.ObservedAt = time.Time{} }, wantErr: "observedAtUs"},
		{name: "stale", mutate: func(v *measuredVolume) { v.SampleAge += time.Millisecond }, wantErr: "stale"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			volume := fresh
			tt.mutate(&volume)
			require.ErrorContains(t, validateMeasuredVolume(volume), tt.wantErr)
		})
	}
}

func TestMeasurementsForVolumeValidatesOnlySelectedVolume(t *testing.T) {
	t.Parallel()

	now := time.Now()
	reconciler := &VolumeExpansionReconciler{}
	results := []podDiskUsageResult{
		{
			Pod: "ledger-test-0",
			Usage: podDiskUsage{
				WAL:  measuredVolume{UsedBytes: 90, TotalBytes: 100, ObservedAt: now, Valid: false, Error: "WAL Statfs failed"},
				Data: measuredVolume{UsedBytes: 80, TotalBytes: 100, ObservedAt: now, Valid: true},
			},
		},
	}

	data := reconciler.measurementsForVolume(results, "data")
	require.Len(t, data, 1)
	require.NoError(t, data[0].Err)
	require.Equal(t, uint64(80), data[0].UsedBytes)

	wal := reconciler.measurementsForVolume(results, "wal")
	require.Len(t, wal, 1)
	require.ErrorContains(t, wal[0].Err, "WAL Statfs failed")
}

func TestRemoveDisabledVolumeExpansionConditions(t *testing.T) {
	t.Parallel()

	ledger := &ledgerv1alpha1.Cluster{Status: ledgerv1alpha1.ClusterStatus{Conditions: []metav1.Condition{
		{Type: walVolumeExpansionCondition, Status: metav1.ConditionTrue, Reason: "Ready"},
		{Type: dataVolumeExpansionCondition, Status: metav1.ConditionFalse, Reason: "ExpansionPending"},
		{Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllReplicasReady"},
	}}}

	removeDisabledVolumeExpansionConditions(ledger, []persistenceVolumeDefinition{{Name: "data"}})

	require.Nil(t, meta.FindStatusCondition(ledger.Status.Conditions, walVolumeExpansionCondition))
	require.NotNil(t, meta.FindStatusCondition(ledger.Status.Conditions, dataVolumeExpansionCondition))
	require.NotNil(t, meta.FindStatusCondition(ledger.Status.Conditions, "Ready"))
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
			GrpcPort: 9999,
			Persistence: ledgerv1alpha1.PersistenceSpec{
				Data: ledgerv1alpha1.VolumeSpec{
					AutoExpansion: &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: &maximum},
				},
			},
		},
	}
	// The rejected desired port must not enter the real measurement command.
	command := volumeDiskUsageCommand(ledger.Name, tlsModeDisabled)
	require.Len(t, command, 3)
	require.Contains(t, command[2], `--server "$POD_NAME.ledger-test-headless.$POD_NAMESPACE.svc.cluster.local:$GRPC_PORT"`)
	require.NotContains(t, command[2], ":9999")
	objects := []runtime.Object{
		ledger,
		controlledByCluster(&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "ledger-test", Namespace: "default"},
			Spec:       appsv1.StatefulSetSpec{Replicas: &liveReplicas},
		}, ledger),
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: storageClassName}, AllowVolumeExpansion: &allowExpansion},
	}
	for ordinal := range liveReplicas {
		objects = append(objects, boundTestPVC("data-ledger-test-"+strconv.Itoa(int(ordinal)), storageClassName, "100Gi"))
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(ledger).WithRuntimeObjects(volumeExpansionTestObjects(ledger, objects...)...).Build()
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
				WAL:  freshMeasuredVolume(1, 100, now),
				Data: freshMeasuredVolume(used, uint64(testQuantityValue("100Gi")), now),
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
	var updatedCluster ledgerv1alpha1.Cluster
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(ledger), &updatedCluster))
	condition := meta.FindStatusCondition(updatedCluster.Status.Conditions, dataVolumeExpansionCondition)
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Equal(t, "ExpansionPending", condition.Reason)

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

func TestVolumeExpansionReconcilerGrowsDespiteStorageDeclarationDrift(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name          string
		templateClass *string
		desiredClass  string
		desiredMode   string
		unsupported   string
	}{
		{name: "defaulted class becomes explicit", desiredClass: "gp3"},
		{name: "explicit class becomes omitted", templateClass: new("gp3")},
		{name: "different class", templateClass: new("gp3"), desiredClass: "io2", unsupported: "changing storageClass"},
		{name: "different access mode", templateClass: new("gp3"), desiredClass: "gp3", desiredMode: "ReadWriteMany", unsupported: "changing accessMode"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ledger, sts, c := persistenceTestFixture(t, 2)
			sts.Spec.VolumeClaimTemplates[1].Spec.StorageClassName = tt.templateClass
			require.NoError(t, c.Update(t.Context(), sts))
			require.NoError(t, c.Create(t.Context(), &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "gp3"}, AllowVolumeExpansion: new(true)}))
			before := make([]*corev1.PersistentVolumeClaim, 2)
			for ordinal := range 2 {
				pvc := readPersistencePVC(t, c, "data", ordinal)
				pvc.Spec.StorageClassName = new("gp3")
				require.NoError(t, c.Update(t.Context(), pvc))
				before[ordinal] = pvc.DeepCopy()
			}
			ledger.Spec.Persistence.Data.StorageClass = tt.desiredClass
			ledger.Spec.Persistence.Data.AccessMode = tt.desiredMode
			ledger.Spec.Persistence.Data.AutoExpansion = &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: new(resource.MustParse("200Gi"))}
			require.NoError(t, c.Update(t.Context(), ledger))
			now := time.Date(2026, time.October, 23, 12, 0, 0, 0, time.UTC)
			var measurements atomic.Int32
			r := &VolumeExpansionReconciler{
				Client: c, APIReader: c, Now: func() time.Time { return now },
				ReadDiskUsage: func(context.Context, *ledgerv1alpha1.Cluster, string, string) (podDiskUsage, error) {
					measurements.Add(1)

					return podDiskUsage{Data: freshMeasuredVolume(uint64(testQuantityValue("80Gi")), uint64(testQuantityValue("100Gi")), now)}, nil
				},
			}
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ledger)})
			require.NoError(t, err)
			require.Equal(t, int32(2), measurements.Load(), "the automatic policy must reach measurement despite declaration drift")
			for ordinal := range 2 {
				pvc := readPersistencePVC(t, c, "data", ordinal)
				expected := before[ordinal].Spec.DeepCopy()
				expected.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("146Gi")
				require.Equal(t, *expected, pvc.Spec)
				require.Equal(t, before[ordinal].UID, pvc.UID)
				require.Equal(t, "146Gi", pvc.Annotations[annotationLastExpansionTarget])
				pvc.Status.Capacity = pvc.Spec.Resources.Requests.DeepCopy()
				require.NoError(t, c.Status().Update(t.Context(), pvc))
			}
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
			expansion := meta.FindStatusCondition(ledger.Status.Conditions, dataVolumeExpansionCondition)
			require.NotNil(t, expansion)
			require.Equal(t, "ExpansionPending", expansion.Reason)
			storage := meta.FindStatusCondition(ledger.Status.Conditions, dataStorageCondition)
			require.NotNil(t, storage)
			if tt.unsupported != "" {
				require.Equal(t, "StorageUnsupported", storage.Reason, "post-auto refresh must preserve the migration diagnosis")
				require.Contains(t, storage.Message, tt.unsupported)
			} else {
				require.Equal(t, "StorageUpdatePending", storage.Reason)
			}
			_, err = r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ledger)})
			require.NoError(t, err)
			require.Equal(t, int32(2), measurements.Load(), "cooldown must prevent another measurement")
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
			storage = meta.FindStatusCondition(ledger.Status.Conditions, dataStorageCondition)
			if tt.unsupported != "" {
				require.Equal(t, metav1.ConditionFalse, storage.Status)
				require.Equal(t, "StorageUnsupported", storage.Reason, "converged capacity must not report a migration as Ready")
				require.Contains(t, storage.Message, tt.unsupported)
			} else {
				require.Equal(t, metav1.ConditionTrue, storage.Status)
			}
			var current appsv1.StatefulSet
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(sts), &current))
			require.Equal(t, sts.Spec, current.Spec, "automatic growth must not rewrite templates")
		})
	}
}

func TestVolumeExpansionReconcilerConvergesPartialPatchAfterRestart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, time.September, 30, 8, 0, 0, 0, time.UTC)
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, storagev1.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))

	allowExpansion := true
	storageClassName := "expandable"
	maximum := resource.MustParse("200Gi")
	replicas := int32(3)
	ledger := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "restart", Namespace: "default"},
		Spec: ledgerv1alpha1.ClusterSpec{
			Replicas: &replicas,
			Persistence: ledgerv1alpha1.PersistenceSpec{
				Data: ledgerv1alpha1.VolumeSpec{
					AutoExpansion: &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: &maximum},
				},
			},
		},
	}
	objects := []runtime.Object{
		ledger,
		controlledByCluster(&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "ledger-restart", Namespace: "default"},
			Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
		}, ledger),
		&storagev1.StorageClass{
			ObjectMeta:           metav1.ObjectMeta{Name: storageClassName},
			AllowVolumeExpansion: &allowExpansion,
		},
	}
	for ordinal := range replicas {
		objects = append(objects, boundTestPVC("data-ledger-restart-"+strconv.Itoa(int(ordinal)), storageClassName, "100Gi"))
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(ledger).WithRuntimeObjects(volumeExpansionTestObjects(ledger, objects...)...).Build()
	failingClient := &failNthPatchClient{Client: baseClient, failAt: 2}
	var measurementCalls atomic.Int64
	readDiskUsage := func(context.Context, *ledgerv1alpha1.Cluster, string, string) (podDiskUsage, error) {
		measurementCalls.Add(1)

		return podDiskUsage{Data: freshMeasuredVolume(
			uint64(testQuantityValue("80Gi")),
			uint64(testQuantityValue("100Gi")),
			now,
		)}, nil
	}
	firstGeneration := &VolumeExpansionReconciler{
		Client:        failingClient,
		APIReader:     baseClient,
		Now:           func() time.Time { return now },
		ReadDiskUsage: readDiskUsage,
	}

	_, err := firstGeneration.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "restart"}})
	require.ErrorContains(t, err, "injected PVC patch failure")
	require.Equal(t, 2, failingClient.attempts)
	require.Equal(t, int64(3), measurementCalls.Load())

	requests := map[string]int{}
	for ordinal := range replicas {
		var pvc corev1.PersistentVolumeClaim
		require.NoError(t, baseClient.Get(ctx, types.NamespacedName{
			Namespace: "default",
			Name:      "data-ledger-restart-" + strconv.Itoa(int(ordinal)),
		}, &pvc))
		request := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		requests[request.String()]++
	}
	require.Equal(t, map[string]int{"100Gi": 2, "146Gi": 1}, requests)

	// A fresh reconciler instance models an operator restart. It must recover
	// from persisted PVC state alone, without taking another usage measurement
	// and without allowing cooldown to block convergence.
	secondGeneration := &VolumeExpansionReconciler{
		Client:        baseClient,
		APIReader:     baseClient,
		Now:           func() time.Time { return now.Add(time.Minute) },
		ReadDiskUsage: readDiskUsage,
	}
	result, err := secondGeneration.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "restart"}})
	require.NoError(t, err)
	require.Equal(t, volumeExpansionRetryInterval, result.RequeueAfter)
	require.Equal(t, int64(3), measurementCalls.Load(), "partial-patch convergence must not depend on a new measurement")

	for ordinal := range replicas {
		var pvc corev1.PersistentVolumeClaim
		require.NoError(t, baseClient.Get(ctx, types.NamespacedName{
			Namespace: "default",
			Name:      "data-ledger-restart-" + strconv.Itoa(int(ordinal)),
		}, &pvc))
		request := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		require.Equal(t, "146Gi", request.String())
		require.Equal(t, "146Gi", pvc.Annotations[annotationLastExpansionTarget])
		require.Equal(t, now.Format(time.RFC3339), pvc.Annotations[annotationLastExpansionAt])
	}
}

func TestLoadVolumePVCsUsesDirectAPIReader(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	pvc := boundTestPVC("data-ledger-test-0", "expandable", "100Gi")
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	directReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc).Build()
	reconciler := &VolumeExpansionReconciler{Client: cachedClient, APIReader: directReader}
	ledger := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}}

	pvcs, states, err := reconciler.loadVolumePVCs(t.Context(), ledger, "data", 1)
	require.NoError(t, err)
	require.Len(t, pvcs, 1)
	require.Len(t, states, 1)
	require.Equal(t, pvc.Name, pvcs[0].Name)

	reconciler.APIReader = nil
	_, _, err = reconciler.loadVolumePVCs(t.Context(), ledger, "data", 1)
	require.ErrorContains(t, err, "APIReader is required")
}

func TestLoadVolumePVCsIgnoresDiagnosticTargetAndFutureCooldown(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	pvc := boundTestPVC("data-ledger-test-0", "expandable", "100Gi")
	pvc.Annotations = map[string]string{
		annotationLastExpansionAt:     now.Add(365 * 24 * time.Hour).Format(time.RFC3339),
		annotationLastExpansionTarget: "not-a-quantity",
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc).Build()
	recorder := record.NewFakeRecorder(1)
	reconciler := &VolumeExpansionReconciler{
		Client:    k8sClient,
		APIReader: k8sClient,
		Recorder:  recorder,
		Now:       func() time.Time { return now },
	}
	ledger := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}}

	_, states, err := reconciler.loadVolumePVCs(t.Context(), ledger, "data", 1)
	require.NoError(t, err)
	require.Len(t, states, 1)
	require.True(t, states[0].LastExpansionAt.IsZero(), "a future annotation must not create an unbounded cooldown")

	select {
	case event := <-recorder.Events:
		require.Contains(t, event, "VolumeExpansionAnnotationInvalid")
	case <-time.After(time.Second):
		t.Fatal("expected a warning for the future cooldown annotation")
	}
}

func TestVolumeExpansionReconcilerIgnoresMalformedCooldownAnnotation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 30, 9, 0, 0, 0, time.UTC)
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, storagev1.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))

	allowExpansion := true
	storageClassName := "expandable"
	maximum := resource.MustParse("200Gi")
	replicas := int32(1)
	ledger := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "malformed-cooldown", Namespace: "default"},
		Spec: ledgerv1alpha1.ClusterSpec{
			Replicas: &replicas,
			Persistence: ledgerv1alpha1.PersistenceSpec{
				Data: ledgerv1alpha1.VolumeSpec{
					AutoExpansion: &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: &maximum},
				},
			},
		},
	}
	pvc := boundTestPVC("data-ledger-malformed-cooldown-0", storageClassName, "100Gi")
	pvc.Annotations = map[string]string{annotationLastExpansionAt: "not-a-timestamp"}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(ledger).WithRuntimeObjects(volumeExpansionTestObjects(ledger,
		ledger,
		controlledByCluster(&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "ledger-malformed-cooldown", Namespace: "default"},
			Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
		}, ledger),
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: storageClassName}, AllowVolumeExpansion: &allowExpansion},
		pvc,
	)...).Build()
	recorder := record.NewFakeRecorder(10)
	reconciler := &VolumeExpansionReconciler{
		Client:    k8sClient,
		APIReader: k8sClient,
		Recorder:  recorder,
		Now:       func() time.Time { return now },
		ReadDiskUsage: func(context.Context, *ledgerv1alpha1.Cluster, string, string) (podDiskUsage, error) {
			return podDiskUsage{Data: freshMeasuredVolume(uint64(testQuantityValue("80Gi")), uint64(testQuantityValue("100Gi")), now)}, nil
		},
	}

	_, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ledger)})
	require.NoError(t, err)

	var got corev1.PersistentVolumeClaim
	require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(pvc), &got))
	requested := got.Spec.Resources.Requests[corev1.ResourceStorage]
	require.Equal(t, "146Gi", requested.String())
	require.Equal(t, now.Format(time.RFC3339), got.Annotations[annotationLastExpansionAt])

	events := make([]string, 0, 2)
	for range 2 {
		select {
		case event := <-recorder.Events:
			events = append(events, event)
		case <-time.After(time.Second):
			t.Fatalf("expected invalid annotation and expansion events, got %v", events)
		}
	}
	require.Condition(t, func() bool {
		return strings.Contains(events[0], "VolumeExpansionAnnotationInvalid") || strings.Contains(events[1], "VolumeExpansionAnnotationInvalid")
	})
}

func TestVolumeExpansionReconcilerCollectsWALAndDataOnceInParallel(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 30, 10, 0, 0, 0, time.UTC)
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, storagev1.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))

	allowExpansion := true
	storageClassName := "expandable"
	maximum := resource.MustParse("200Gi")
	replicas := int32(3)
	autoExpansion := &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: &maximum}
	ledger := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "shared-measurement", Namespace: "default"},
		Spec: ledgerv1alpha1.ClusterSpec{
			Replicas: &replicas,
			Persistence: ledgerv1alpha1.PersistenceSpec{
				WAL:  ledgerv1alpha1.VolumeSpec{AutoExpansion: autoExpansion.DeepCopy()},
				Data: ledgerv1alpha1.VolumeSpec{AutoExpansion: autoExpansion.DeepCopy()},
			},
		},
	}
	objects := []runtime.Object{
		ledger,
		controlledByCluster(&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "ledger-shared-measurement", Namespace: "default"}, Spec: appsv1.StatefulSetSpec{Replicas: &replicas}}, ledger),
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: storageClassName}, AllowVolumeExpansion: &allowExpansion},
	}
	for ordinal := range replicas {
		objects = append(objects,
			boundTestPVC("wal-ledger-shared-measurement-"+strconv.Itoa(int(ordinal)), storageClassName, "100Gi"),
			boundTestPVC("data-ledger-shared-measurement-"+strconv.Itoa(int(ordinal)), storageClassName, "100Gi"),
		)
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(ledger).WithRuntimeObjects(volumeExpansionTestObjects(ledger, objects...)...).Build()
	started := make(chan struct{}, replicas)
	release := make(chan struct{})
	var lock sync.Mutex
	calls := 0
	active := 0
	maxActive := 0
	reconciler := &VolumeExpansionReconciler{
		Client:    k8sClient,
		APIReader: k8sClient,
		Now:       func() time.Time { return now },
		ReadDiskUsage: func(context.Context, *ledgerv1alpha1.Cluster, string, string) (podDiskUsage, error) {
			lock.Lock()
			calls++
			active++
			maxActive = max(maxActive, active)
			lock.Unlock()
			started <- struct{}{}
			<-release
			lock.Lock()
			active--
			lock.Unlock()

			fresh := freshMeasuredVolume(uint64(testQuantityValue("50Gi")), uint64(testQuantityValue("100Gi")), now)

			return podDiskUsage{WAL: fresh, Data: fresh}, nil
		},
	}
	type reconcileResult struct {
		result ctrl.Result
		err    error
	}
	done := make(chan reconcileResult, 1)
	go func() {
		result, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ledger)})
		done <- reconcileResult{result: result, err: err}
	}()
	for range replicas {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			close(release)
			t.Fatal("disk usage calls did not start concurrently")
		}
	}
	close(release)
	outcome := <-done
	require.NoError(t, outcome.err)
	require.Equal(t, volumeExpansionRequeueInterval, outcome.result.RequeueAfter)
	require.Equal(t, int(replicas), calls, "WAL and data must share one disk-usage call per pod")
	require.Equal(t, int(replicas), maxActive, "disk-usage calls should run concurrently across replicas")
}

func TestPatchPVCGroupRetainsLargerRequestBeforeDirectRead(t *testing.T) {
	t.Parallel()

	ledger, sts, k8sClient := persistenceTestFixture(t, 1)
	pvc := readPersistencePVC(t, k8sClient, "data", 0)
	reconciler := &VolumeExpansionReconciler{Client: k8sClient, APIReader: k8sClient}
	storage := &persistenceStorageReconciler{client: k8sClient, reader: k8sClient, ledger: ledger, statefulSet: sts}

	var stale corev1.PersistentVolumeClaim
	require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(pvc), &stale))
	var concurrent corev1.PersistentVolumeClaim
	require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(pvc), &concurrent))
	concurrent.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("180Gi")
	require.NoError(t, k8sClient.Update(t.Context(), &concurrent))

	err := reconciler.patchPVCGroup(
		t.Context(),
		[]*corev1.PersistentVolumeClaim{stale.DeepCopy()},
		testQuantityValue("146Gi"),
		time.Now(),
		storage, persistenceVolumeDefinitions(ledger)[1],
	)
	require.NoError(t, err)

	var persisted corev1.PersistentVolumeClaim
	require.NoError(t, k8sClient.Get(t.Context(), client.ObjectKeyFromObject(pvc), &persisted))
	requested := persisted.Spec.Resources.Requests[corev1.ResourceStorage]
	require.Equal(t, "180Gi", requested.String())
}

func TestPatchPVCGroupUsesOptimisticLock(t *testing.T) {
	t.Parallel()
	ledger, sts, k8sClient := persistenceTestFixture(t, 1)
	pvc := readPersistencePVC(t, k8sClient, "data", 0)
	watchClient, ok := k8sClient.(client.WithWatch)
	require.True(t, ok)
	reads, patches, conflicts := 0, 0, 0
	var concurrent corev1.PersistentVolumeClaim
	c := interceptor.NewClient(watchClient, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.PersistentVolumeClaim); ok && key == client.ObjectKeyFromObject(pvc) {
				reads++
			}

			return c.Get(ctx, key, obj, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			require.IsType(t, &corev1.PersistentVolumeClaim{}, obj)
			require.Equal(t, client.ObjectKeyFromObject(pvc), client.ObjectKeyFromObject(obj))
			patches++
			require.Equal(t, 1, reads, "the concurrent increase must follow the direct read")
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(pvc), &concurrent))
			require.Equal(t, pvc.ResourceVersion, obj.GetResourceVersion())
			concurrent.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("180Gi")
			concurrent.Annotations = map[string]string{"concurrent": "preserved"}
			require.NoError(t, c.Update(ctx, &concurrent))

			err := c.Patch(ctx, obj, patch, opts...)
			require.True(t, apierrors.IsConflict(err), "a stale expansion patch must conflict instead of overwriting 180Gi: %v", err)
			if apierrors.IsConflict(err) {
				conflicts++
			}

			return err
		},
	})
	reconciler := &VolumeExpansionReconciler{Client: c, APIReader: c}
	storage := &persistenceStorageReconciler{client: c, reader: c, ledger: ledger, statefulSet: sts}
	err := reconciler.patchPVCGroup(t.Context(), []*corev1.PersistentVolumeClaim{pvc}, testQuantityValue("146Gi"),
		time.Now(), storage, persistenceVolumeDefinitions(ledger)[1])
	require.NoError(t, err)
	require.Equal(t, 2, reads, "the retry must reread and retain the larger live request")
	require.Equal(t, 1, patches, "the larger request must prevent another patch")
	require.Equal(t, 1, conflicts)
	persisted := readPersistencePVC(t, k8sClient, "data", 0)
	require.Equal(t, pvc.UID, persisted.UID)
	require.Equal(t, concurrent.Spec, persisted.Spec)
	require.Equal(t, concurrent.Annotations, persisted.Annotations, "a rejected expansion must not stamp its annotations")
}

func TestVolumeMetricCleanup(t *testing.T) {
	t.Parallel()

	const (
		namespace = "metric-cleanup-namespace"
		cluster   = "metric-cleanup-cluster"
		pod       = "ledger-metric-cleanup-cluster-0"
		volume    = "data"
	)
	volumeUsageRatioMetric.WithLabelValues(namespace, cluster, pod, volume).Set(0.8)
	volumeUsageSampleAgeMetric.WithLabelValues(namespace, cluster, pod, volume).Set(10)
	volumeRequestedBytesMetric.WithLabelValues(namespace, cluster, volume).Set(100)
	volumeExpansionsMetric.WithLabelValues(namespace, cluster, volume, "requested").Inc()

	resetVolumeGaugeMetrics(namespace, cluster)
	require.False(t, volumeUsageRatioMetric.DeleteLabelValues(namespace, cluster, pod, volume))
	require.False(t, volumeUsageSampleAgeMetric.DeleteLabelValues(namespace, cluster, pod, volume))
	require.False(t, volumeRequestedBytesMetric.DeleteLabelValues(namespace, cluster, volume))
	require.True(t, volumeExpansionsMetric.DeleteLabelValues(namespace, cluster, volume, "requested"))

	volumeExpansionsMetric.WithLabelValues(namespace, cluster, volume, "requested").Inc()
	deleteVolumeMetrics(namespace, cluster)
	require.False(t, volumeExpansionsMetric.DeleteLabelValues(namespace, cluster, volume, "requested"))
}

func TestVolumeExpansionReconcilerRejectsNonExpandableStorageClass(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
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
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(ledger).WithRuntimeObjects(volumeExpansionTestObjects(ledger,
		ledger,
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: storageClassName}},
		pvc,
	)...).Build()
	reconciler := &VolumeExpansionReconciler{
		Client:    k8sClient,
		APIReader: k8sClient,
		Recorder:  record.NewFakeRecorder(10),
		ReadDiskUsage: func(context.Context, *ledgerv1alpha1.Cluster, string, string) (podDiskUsage, error) {
			return podDiskUsage{}, errors.New("must not be called")
		},
	}

	retry, err := reconciler.reconcileVolume(ctx, ledger, persistenceVolumeDefinition{
		Name:                 "data",
		Field:                "persistence.data",
		Spec:                 &ledger.Spec.Persistence.Data,
		DefaultSize:          "10Gi",
		AutoExpansionAllowed: true,
	}, "disabled", replicas, volumeExpansionTestStatefulSet(ledger, replicas), &reconcileDiskUsage{})
	require.NoError(t, err)
	assert.True(t, retry)

	var got corev1.PersistentVolumeClaim
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: pvc.Name}, &got))
	request := got.Spec.Resources.Requests[corev1.ResourceStorage]
	assert.Equal(t, "100Gi", request.String())
}

func TestVolumeExpansionReconcilerDoesNotExpandFromInvalidMeasurement(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, storagev1.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))

	allowExpansion := true
	storageClassName := "expandable"
	maximum := resource.MustParse("200Gi")
	ledger := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "invalid-sample", Namespace: "default"},
		Spec: ledgerv1alpha1.ClusterSpec{
			Persistence: ledgerv1alpha1.PersistenceSpec{
				Data: ledgerv1alpha1.VolumeSpec{
					AutoExpansion: &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: &maximum},
				},
			},
		},
	}
	pvc := boundTestPVC("data-ledger-invalid-sample-0", storageClassName, "100Gi")
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(ledger).WithRuntimeObjects(volumeExpansionTestObjects(ledger,
		ledger,
		&storagev1.StorageClass{
			ObjectMeta:           metav1.ObjectMeta{Name: storageClassName},
			AllowVolumeExpansion: &allowExpansion,
		},
		pvc,
	)...).Build()
	reconciler := &VolumeExpansionReconciler{
		Client:    k8sClient,
		APIReader: k8sClient,
		Recorder:  record.NewFakeRecorder(10),
		Now:       func() time.Time { return now },
		ReadDiskUsage: func(context.Context, *ledgerv1alpha1.Cluster, string, string) (podDiskUsage, error) {
			return podDiskUsage{
				Data: measuredVolume{
					UsedBytes:  uint64(testQuantityValue("90Gi")),
					TotalBytes: uint64(testQuantityValue("100Gi")),
					ObservedAt: now.Add(-time.Second),
					Valid:      false,
					Error:      "Statfs failed",
				},
			}, nil
		},
	}

	retry, err := reconciler.reconcileVolume(ctx, ledger, persistenceVolumeDefinition{
		Name:                 "data",
		Field:                "persistence.data",
		Spec:                 &ledger.Spec.Persistence.Data,
		DefaultSize:          "10Gi",
		AutoExpansionAllowed: true,
	}, "disabled", 1, volumeExpansionTestStatefulSet(ledger, 1), &reconcileDiskUsage{})
	require.NoError(t, err)
	require.True(t, retry)

	var got corev1.PersistentVolumeClaim
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: pvc.Name}, &got))
	request := got.Spec.Resources.Requests[corev1.ResourceStorage]
	require.Equal(t, "100Gi", request.String())
}

func TestVolumeExpansionReconcilerRejectsHostPathPolicyBeforeSideEffects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))

	maximum := resource.MustParse("200Gi")
	replicas := int32(1)
	ledger := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "host-path", Namespace: "default"},
		Spec: ledgerv1alpha1.ClusterSpec{
			Replicas: &replicas,
			Persistence: ledgerv1alpha1.PersistenceSpec{
				Data: ledgerv1alpha1.VolumeSpec{
					HostPath: &ledgerv1alpha1.HostPathVolumeSpec{Path: "/data"},
					AutoExpansion: &ledgerv1alpha1.VolumeAutoExpansionSpec{
						Enabled:     true,
						MaximumSize: &maximum,
					},
				},
			},
		},
	}
	pvc := boundTestPVC("data-ledger-host-path-0", "fixed", "100Gi")
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(ledger).WithRuntimeObjects(volumeExpansionTestObjects(ledger,
		ledger,
		controlledByCluster(&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "ledger-host-path", Namespace: "default"},
			Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
		}, ledger),
		pvc,
	)...).Build()
	recorder := record.NewFakeRecorder(10)
	readCalls := 0
	reconciler := &VolumeExpansionReconciler{
		Client:    k8sClient,
		APIReader: k8sClient,
		Recorder:  recorder,
		ReadDiskUsage: func(context.Context, *ledgerv1alpha1.Cluster, string, string) (podDiskUsage, error) {
			readCalls++

			return podDiskUsage{}, errors.New("must not be called")
		},
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: ledger.Namespace,
		Name:      ledger.Name,
	}})
	require.NoError(t, err)
	assert.Equal(t, volumeExpansionRequeueInterval, result.RequeueAfter)
	assert.Zero(t, readCalls)

	var got corev1.PersistentVolumeClaim
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: pvc.Name}, &got))
	request := got.Spec.Resources.Requests[corev1.ResourceStorage]
	assert.Equal(t, "100Gi", request.String())

	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, "VolumeExpansionUnsupported")
		assert.Contains(t, event, "mutually exclusive")
	case <-time.After(time.Second):
		t.Fatal("expected invalid hostPath policy event")
	}
}

func TestVolumeExpansionReconcilerDoesNotConvergeAboveMaximum(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, storagev1.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))

	allowExpansion := true
	storageClassName := "expandable"
	maximum := resource.MustParse("150Gi")
	replicas := int32(2)
	ledger := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "above-maximum", Namespace: "default"},
		Spec: ledgerv1alpha1.ClusterSpec{
			Replicas: &replicas,
			Persistence: ledgerv1alpha1.PersistenceSpec{
				Data: ledgerv1alpha1.VolumeSpec{
					AutoExpansion: &ledgerv1alpha1.VolumeAutoExpansionSpec{
						Enabled:     true,
						MaximumSize: &maximum,
					},
				},
			},
		},
	}
	pvc0 := boundTestPVC("data-ledger-above-maximum-0", storageClassName, "200Gi")
	pvc1 := boundTestPVC("data-ledger-above-maximum-1", storageClassName, "100Gi")
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(ledger).WithRuntimeObjects(volumeExpansionTestObjects(ledger,
		ledger,
		controlledByCluster(&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "ledger-above-maximum", Namespace: "default"},
			Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
		}, ledger),
		&storagev1.StorageClass{
			ObjectMeta:           metav1.ObjectMeta{Name: storageClassName},
			AllowVolumeExpansion: &allowExpansion,
		},
		pvc0,
		pvc1,
	)...).Build()
	recorder := record.NewFakeRecorder(10)
	readCalls := 0
	reconciler := &VolumeExpansionReconciler{
		Client:    k8sClient,
		APIReader: k8sClient,
		Recorder:  recorder,
		ReadDiskUsage: func(context.Context, *ledgerv1alpha1.Cluster, string, string) (podDiskUsage, error) {
			readCalls++

			return podDiskUsage{}, errors.New("must not be called")
		},
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: ledger.Namespace,
		Name:      ledger.Name,
	}})
	require.NoError(t, err)
	assert.Equal(t, volumeExpansionRequeueInterval, result.RequeueAfter)
	assert.Zero(t, readCalls)

	for name, expected := range map[string]string{
		pvc0.Name: "200Gi",
		pvc1.Name: "100Gi",
	} {
		var got corev1.PersistentVolumeClaim
		require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, &got))
		request := got.Spec.Resources.Requests[corev1.ResourceStorage]
		assert.Equal(t, expected, request.String())
	}

	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, "VolumeExpansionUnsupported")
		assert.Contains(t, event, "exceeds maximumSize")
	case <-time.After(time.Second):
		t.Fatal("expected above-maximum policy event")
	}
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
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name + "-uid"), OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "StatefulSet", Name: name[strings.Index(name, "ledger-"):strings.LastIndex(name, "-")], UID: types.UID(name[strings.Index(name, "ledger-"):strings.LastIndex(name, "-")] + "-uid"),
		}}},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &storageClass,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
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

func freshMeasuredVolume(used, total uint64, observedAt time.Time) measuredVolume {
	return measuredVolume{
		UsedBytes:  used,
		TotalBytes: total,
		ObservedAt: observedAt,
		Valid:      true,
	}
}
