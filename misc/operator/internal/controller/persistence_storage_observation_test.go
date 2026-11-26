package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestPersistenceRefreshPreservesUnsupportedMigrationDiagnosis(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		class   string
		mode    string
		message string
	}{
		{name: "StorageClass migration", class: "io2", message: "changing storageClass"},
		{name: "access mode migration", class: "expandable", mode: "ReadWriteMany", message: "changing accessMode"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ledger, sts, c := persistenceTestFixture(t, 1)
			ledger.Spec.Persistence.Data.StorageClass = tt.class
			ledger.Spec.Persistence.Data.AccessMode = tt.mode
			setStorageCondition(ledger, "data", metav1.ConditionTrue, "Ready", "previous capacity observation")
			r := &persistenceStorageReconciler{client: c, reader: c, ledger: ledger, statefulSet: sts}
			before := readPersistencePVC(t, c, "data", 0)

			require.True(t, r.refreshCondition(t.Context(), persistenceVolumeDefinitions(ledger)[1], 1))
			condition := meta.FindStatusCondition(ledger.Status.Conditions, dataStorageCondition)
			require.NotNil(t, condition)
			require.Equal(t, metav1.ConditionFalse, condition.Status)
			require.Equal(t, "StorageUnsupported", condition.Reason)
			require.Contains(t, condition.Message, tt.message)
			var after corev1.PersistentVolumeClaim
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(before), &after))
			require.Equal(t, before.Spec, after.Spec)
			require.Equal(t, before.ResourceVersion, after.ResourceVersion)
		})
	}
}

func newStorageObservationPVC() *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data-ledger-test-0"},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-data-ledger-test-0",
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("10Gi"),
			}},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("10Gi"),
			},
		},
	}
}

func TestPVCStorageObservationReadiness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		configure       func(*corev1.PersistentVolumeClaim)
		class           string
		wantReady       bool
		wantObservation string
	}{
		{
			name:            "bound capacity without a profile",
			wantReady:       true,
			wantObservation: `requested=10Gi capacity=10Gi minimum=10Gi requestedClass="" currentClass=""`,
		},
		{
			name: "request below the configured minimum",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("5Gi")
			},
			wantObservation: "requested=5Gi capacity=10Gi minimum=10Gi",
		},
		{
			name: "capacity reaches minimum but not the larger live request",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("20Gi")
			},
			wantObservation: "requested=20Gi capacity=10Gi minimum=10Gi",
		},
		{
			name: "larger existing capacity is ready",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("20Gi")
				pvc.Status.Capacity[corev1.ResourceStorage] = resource.MustParse("30Gi")
			},
			wantReady:       true,
			wantObservation: "requested=20Gi capacity=30Gi minimum=10Gi",
		},
		{
			name: "equivalent capacity units",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("10240Mi")
			},
			wantReady: true,
		},
		{
			name: "capacity has not been observed",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Status.Capacity = nil
			},
			wantObservation: "requested=10Gi capacity=0 minimum=10Gi",
		},
		{
			name: "request is missing",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.Resources.Requests = nil
			},
			wantObservation: "requested=0 capacity=10Gi minimum=10Gi",
		},
		{
			name: "requested class has not reached desired class",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.VolumeAttributesClassName = new("standard")
				pvc.Status.CurrentVolumeAttributesClassName = new("premium")
			},
			class:           "premium",
			wantObservation: `requestedClass="standard" currentClass="premium"`,
		},
		{
			name: "current class has not reached the requested class",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.VolumeAttributesClassName = new("premium")
				pvc.Status.CurrentVolumeAttributesClassName = new("standard")
			},
			class:           "premium",
			wantObservation: `requestedClass="premium" currentClass="standard"`,
		},
		{
			name: "requested class is stored but CSI has not reported a class",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.VolumeAttributesClassName = new("premium")
			},
			class:           "premium",
			wantObservation: `requestedClass="premium" currentClass=""`,
		},
		{
			name: "matching old classes do not establish the desired profile",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.VolumeAttributesClassName = new("standard")
				pvc.Status.CurrentVolumeAttributesClassName = new("standard")
			},
			class:           "premium",
			wantObservation: `requestedClass="standard" currentClass="standard"`,
		},
		{
			name: "class modification completed",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.VolumeAttributesClassName = new("premium")
				pvc.Status.CurrentVolumeAttributesClassName = new("premium")
			},
			class:           "premium",
			wantReady:       true,
			wantObservation: `requestedClass="premium" currentClass="premium"`,
		},
		{
			name: "an unconfigured desired profile does not match an existing profile",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.VolumeAttributesClassName = new("premium")
				pvc.Status.CurrentVolumeAttributesClassName = new("premium")
			},
			wantObservation: `requestedClass="premium" currentClass="premium"`,
		},
		{
			name: "empty profile pointers match an unconfigured profile",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.VolumeAttributesClassName = new("")
				pvc.Status.CurrentVolumeAttributesClassName = new("")
			},
			wantReady: true,
		},
		{
			name: "pending claim with converged capacity is not ready",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Status.Phase = corev1.ClaimPending
			},
		},
		{
			name: "lost claim with converged capacity is not ready",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Status.Phase = corev1.ClaimLost
			},
		},
		{
			name: "unobserved binding phase is not ready",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Status.Phase = ""
			},
		},
		{
			name: "bound claim without a volume name is not ready",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.VolumeName = ""
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pvc := newStorageObservationPVC()
			if tt.configure != nil {
				tt.configure(pvc)
			}
			ready, failed, observation := pvcStorageObservation(pvc, resource.MustParse("10Gi"), tt.class)
			assert.Equal(t, tt.wantReady, ready, observation)
			assert.False(t, failed, observation)
			assert.Contains(t, observation, "PVC data-ledger-test-0:")
			if tt.wantObservation != "" {
				assert.Contains(t, observation, tt.wantObservation)
			}
		})
	}
}

func TestPVCStorageObservationModifyVolumeStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     corev1.PersistentVolumeClaimModifyVolumeStatus
		wantFailed bool
		wantSuffix string
	}{
		{name: "pending", status: corev1.PersistentVolumeClaimModifyVolumePending, wantSuffix: ` modifyTarget="premium" modifyStatus="Pending"`},
		{name: "in progress", status: corev1.PersistentVolumeClaimModifyVolumeInProgress, wantSuffix: ` modifyTarget="premium" modifyStatus="InProgress"`},
		{name: "infeasible", status: corev1.PersistentVolumeClaimModifyVolumeInfeasible, wantFailed: true, wantSuffix: ` modifyTarget="premium" modifyStatus="Infeasible"`},
		{name: "unknown status cannot certify success", status: "DriverReportedSuccess", wantFailed: true, wantSuffix: ` modifyTarget="premium" modifyStatus="DriverReportedSuccess"`},
		{name: "empty status cannot certify success", wantFailed: true, wantSuffix: ` modifyTarget="premium" modifyStatus=""`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pvc := newStorageObservationPVC()
			pvc.Spec.VolumeAttributesClassName = new("premium")
			pvc.Status.CurrentVolumeAttributesClassName = new("premium")
			pvc.Status.ModifyVolumeStatus = &corev1.ModifyVolumeStatus{
				TargetVolumeAttributesClassName: "premium", Status: tt.status,
			}
			ready, failed, observation := pvcStorageObservation(pvc, resource.MustParse("10Gi"), "premium")
			assert.False(t, ready, "matching classes cannot override an outstanding modification: %s", observation)
			assert.Equal(t, tt.wantFailed, failed, observation)
			assert.Equal(t, `PVC data-ledger-test-0: requested=10Gi capacity=10Gi minimum=10Gi requestedClass="premium" currentClass="premium"`+tt.wantSuffix, observation)
		})
	}
}

func TestPVCStorageObservationAllocatedResizeStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     corev1.ClaimResourceStatus
		wantReady  bool
		wantFailed bool
		wantSuffix string
	}{
		{name: "cleared status", wantReady: true},
		{name: "controller resizing", status: corev1.PersistentVolumeClaimControllerResizeInProgress, wantSuffix: ` resizeStatus="ControllerResizeInProgress"`},
		{name: "node resize pending", status: corev1.PersistentVolumeClaimNodeResizePending, wantSuffix: ` resizeStatus="NodeResizePending"`},
		{name: "node resizing", status: corev1.PersistentVolumeClaimNodeResizeInProgress, wantSuffix: ` resizeStatus="NodeResizeInProgress"`},
		{name: "controller resize infeasible", status: corev1.PersistentVolumeClaimControllerResizeInfeasible, wantFailed: true, wantSuffix: ` resizeStatus="ControllerResizeInfeasible"`},
		{name: "node resize infeasible", status: corev1.PersistentVolumeClaimNodeResizeInfeasible, wantFailed: true, wantSuffix: ` resizeStatus="NodeResizeInfeasible"`},
		{name: "unknown resize status remains pending", status: "FutureResizeStatus", wantSuffix: ` resizeStatus="FutureResizeStatus"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pvc := newStorageObservationPVC()
			pvc.Status.AllocatedResourceStatuses = map[corev1.ResourceName]corev1.ClaimResourceStatus{
				corev1.ResourceStorage: tt.status,
			}
			ready, failed, observation := pvcStorageObservation(pvc, resource.MustParse("10Gi"), "")
			assert.Equal(t, tt.wantReady, ready, observation)
			assert.Equal(t, tt.wantFailed, failed, observation)
			assert.Equal(t, `PVC data-ledger-test-0: requested=10Gi capacity=10Gi minimum=10Gi requestedClass="" currentClass=""`+tt.wantSuffix, observation)
		})
	}
}

func TestPVCStorageObservationConditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		conditionType corev1.PersistentVolumeClaimConditionType
		wantReady     bool
		wantFailed    bool
		wantSuffix    string
	}{
		{conditionType: corev1.PersistentVolumeClaimControllerResizeError, wantFailed: true, wantSuffix: " ControllerResizeError=CSIDiagnostic: driver diagnostic details"},
		{conditionType: corev1.PersistentVolumeClaimNodeResizeError, wantFailed: true, wantSuffix: " NodeResizeError=CSIDiagnostic: driver diagnostic details"},
		{conditionType: corev1.PersistentVolumeClaimVolumeModifyVolumeError, wantFailed: true, wantSuffix: " ModifyVolumeError=CSIDiagnostic: driver diagnostic details"},
		{conditionType: corev1.PersistentVolumeClaimResizing, wantSuffix: " Resizing=CSIDiagnostic: driver diagnostic details"},
		{conditionType: corev1.PersistentVolumeClaimFileSystemResizePending, wantSuffix: " FileSystemResizePending=CSIDiagnostic: driver diagnostic details"},
		{conditionType: corev1.PersistentVolumeClaimVolumeModifyingVolume, wantSuffix: " ModifyingVolume=CSIDiagnostic: driver diagnostic details"},
		{conditionType: "UnrelatedCondition", wantReady: true},
	}
	for _, tt := range tests {
		for _, status := range []corev1.ConditionStatus{corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionUnknown} {
			t.Run(string(tt.conditionType)+"/"+string(status), func(t *testing.T) {
				t.Parallel()

				pvc := newStorageObservationPVC()
				pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{{
					Type: tt.conditionType, Status: status, Reason: "CSIDiagnostic", Message: "driver diagnostic details",
				}}
				wantReady, wantFailed, wantSuffix := tt.wantReady, tt.wantFailed, tt.wantSuffix
				if status != corev1.ConditionTrue {
					wantReady, wantFailed, wantSuffix = true, false, ""
				}
				ready, failed, observation := pvcStorageObservation(pvc, resource.MustParse("10Gi"), "")
				assert.Equal(t, wantReady, ready, observation)
				assert.Equal(t, wantFailed, failed, observation)
				assert.Equal(t, `PVC data-ledger-test-0: requested=10Gi capacity=10Gi minimum=10Gi requestedClass="" currentClass=""`+wantSuffix, observation)
			})
		}
	}
}

func TestPVCStorageObservationPreservesFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		configure func(*corev1.PersistentVolumeClaim)
		wantParts []string
	}{
		{
			name: "pending resize does not clear an infeasible modification",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Status.ModifyVolumeStatus = &corev1.ModifyVolumeStatus{TargetVolumeAttributesClassName: "premium", Status: corev1.PersistentVolumeClaimModifyVolumeInfeasible}
				pvc.Status.AllocatedResourceStatuses = map[corev1.ResourceName]corev1.ClaimResourceStatus{corev1.ResourceStorage: corev1.PersistentVolumeClaimNodeResizePending}
			},
			wantParts: []string{`modifyStatus="Infeasible"`, `resizeStatus="NodeResizePending"`},
		},
		{
			name: "infeasible resize is reported during a pending modification",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Status.ModifyVolumeStatus = &corev1.ModifyVolumeStatus{TargetVolumeAttributesClassName: "premium", Status: corev1.PersistentVolumeClaimModifyVolumePending}
				pvc.Status.AllocatedResourceStatuses = map[corev1.ResourceName]corev1.ClaimResourceStatus{corev1.ResourceStorage: corev1.PersistentVolumeClaimNodeResizeInfeasible}
			},
			wantParts: []string{`modifyStatus="Pending"`, `resizeStatus="NodeResizeInfeasible"`},
		},
		{
			name: "later progress conditions preserve the earlier error and both messages",
			configure: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
					{Type: corev1.PersistentVolumeClaimControllerResizeError, Status: corev1.ConditionTrue, Reason: "QuotaExceeded", Message: "CSI capacity quota exhausted"},
					{Type: corev1.PersistentVolumeClaimVolumeModifyingVolume, Status: corev1.ConditionTrue, Reason: "ApplyingProfile", Message: "CSI profile modification is running"},
				}
			},
			wantParts: []string{"ControllerResizeError=QuotaExceeded: CSI capacity quota exhausted", "ModifyingVolume=ApplyingProfile: CSI profile modification is running"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pvc := newStorageObservationPVC()
			tt.configure(pvc)
			ready, failed, observation := pvcStorageObservation(pvc, resource.MustParse("10Gi"), "")
			assert.False(t, ready, observation)
			assert.True(t, failed, observation)
			for _, part := range tt.wantParts {
				assert.Contains(t, observation, part)
			}
		})
	}
}

func TestSetStorageCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		volume    string
		wantType  string
		otherType string
		status    metav1.ConditionStatus
		reason    string
		message   string
	}{
		{name: "WAL pending", volume: "wal", wantType: "WALStorageReady", otherType: "DataStorageReady", status: metav1.ConditionFalse, reason: "StorageUpdatePending", message: "WAL capacity is still expanding"},
		{name: "data failed", volume: "data", wantType: "DataStorageReady", otherType: "WALStorageReady", status: metav1.ConditionFalse, reason: "StorageUpdateFailed", message: "CSI rejected the requested data profile"},
		{name: "WAL ready", volume: "wal", wantType: "WALStorageReady", otherType: "DataStorageReady", status: metav1.ConditionTrue, reason: "Ready", message: "WAL capacity and profile converged"},
		{name: "data unknown", volume: "data", wantType: "DataStorageReady", otherType: "WALStorageReady", status: metav1.ConditionUnknown, reason: "ObservationUnavailable", message: "data storage observation is unavailable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			transition := metav1.NewTime(time.Unix(123, 0))
			unrelated := metav1.Condition{Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllReplicasReady", LastTransitionTime: transition, ObservedGeneration: 6}
			otherVolume := metav1.Condition{Type: tt.otherType, Status: metav1.ConditionTrue, Reason: "Ready", Message: "other volume is unchanged", LastTransitionTime: transition, ObservedGeneration: 6}
			ledger := &ledgerv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Generation: 7},
				Status:     ledgerv1alpha1.ClusterStatus{Conditions: []metav1.Condition{unrelated, otherVolume}},
			}

			setStorageCondition(ledger, tt.volume, tt.status, tt.reason, tt.message)

			require.Len(t, ledger.Status.Conditions, 3)
			condition := meta.FindStatusCondition(ledger.Status.Conditions, tt.wantType)
			require.NotNil(t, condition)
			assert.Equal(t, tt.status, condition.Status)
			assert.Equal(t, tt.reason, condition.Reason)
			assert.Equal(t, tt.message, condition.Message)
			assert.Equal(t, int64(7), condition.ObservedGeneration)
			assert.False(t, condition.LastTransitionTime.IsZero())
			assert.Contains(t, ledger.Status.Conditions, unrelated)
			assert.Contains(t, ledger.Status.Conditions, otherVolume)

			lastTransition := condition.LastTransitionTime
			ledger.Generation = 8
			setStorageCondition(ledger, tt.volume, tt.status, "RefreshedObservation", "updated PVC diagnostic")

			require.Len(t, ledger.Status.Conditions, 3, "refresh must update rather than duplicate the condition")
			condition = meta.FindStatusCondition(ledger.Status.Conditions, tt.wantType)
			require.NotNil(t, condition)
			assert.Equal(t, tt.status, condition.Status)
			assert.Equal(t, "RefreshedObservation", condition.Reason)
			assert.Equal(t, "updated PVC diagnostic", condition.Message)
			assert.Equal(t, int64(8), condition.ObservedGeneration)
			assert.Equal(t, lastTransition, condition.LastTransitionTime, "an unchanged status preserves the transition time")
			assert.Contains(t, ledger.Status.Conditions, unrelated)
			assert.Contains(t, ledger.Status.Conditions, otherVolume)
		})
	}
}
