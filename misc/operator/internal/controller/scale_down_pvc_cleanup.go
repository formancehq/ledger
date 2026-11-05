package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

const annotationScaleDownPVCPlan = "ledger.formance.com/scale-down-pvc-plan"

// The inventory is committed with the old count, before Raft or replica changes.
// A retry must not authorize a new object merely because its name was reused.
type scaleDownPVCPlan struct {
	Version        int                 `json:"version"`
	ClusterUID     types.UID           `json:"clusterUID"`
	StatefulSetUID types.UID           `json:"statefulSetUID"`
	From           int32               `json:"from"`
	To             int32               `json:"to"`
	Claims         []scaleDownPVCClaim `json:"claims"`
}

type scaleDownPVCClaim struct {
	Name                   string                  `json:"name"`
	Ordinal                int32                   `json:"ordinal"`
	Volume                 string                  `json:"volume"`
	UID                    types.UID               `json:"uid"`
	InitialResourceVersion string                  `json:"initialResourceVersion"`
	PodUID                 types.UID               `json:"podUID,omitempty"`
	VolumeName             string                  `json:"volumeName,omitempty"`
	PVUID                  types.UID               `json:"pvUID,omitempty"`
	ClaimRef               *corev1.ObjectReference `json:"claimRef,omitempty"`
}

func prepareScaleDownPVCPlan(ctx context.Context, cs kubernetes.Interface, reader client.Reader, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, to int32) (*scaleDownPVCPlan, error) {
	if sts.UID == "" || ledger.UID == "" || !metav1.IsControlledBy(sts, ledger) || !sts.DeletionTimestamp.IsZero() || sts.Spec.Replicas == nil {
		return nil, errors.New("cannot authorize scale-down PVC cleanup without an owned StatefulSet identity")
	}
	live := &appsv1.StatefulSet{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(sts), live); err != nil {
		return nil, fmt.Errorf("reading StatefulSet before PVC identity capture: %w", err)
	}
	if !sameScaleDownSource(sts, live, ledger) {
		return nil, errors.New("StatefulSet changed before PVC identity capture")
	}
	if from, pending := sts.Annotations[annotationScaleDownPVCFrom]; pending {
		p, err := readScaleDownPVCPlan(sts, ledger)
		if err != nil {
			return nil, err
		}
		if from != strconv.Itoa(int(p.From)) || p.From != *sts.Spec.Replicas || p.To != to {
			return nil, errors.New("pending PVC cleanup replica range changed; finish the recorded scale-down or cancel remaining cleanup with Retain")
		}
		for _, expected := range p.Claims {
			if _, err := p.readClaim(ctx, cs, sts.Namespace, sts.Name, expected); err != nil {
				return nil, err
			}
		}

		return p, nil
	}
	if sts.Annotations[annotationScaleDownPVCPlan] != "" {
		return nil, errors.New("PVC cleanup inventory has no previous replica count")
	}
	p := &scaleDownPVCPlan{Version: 1, ClusterUID: ledger.UID, StatefulSetUID: sts.UID, From: *sts.Spec.Replicas, To: to}
	identity := &persistenceStorageReconciler{reader: reader, statefulSet: sts}
	for ordinal := p.From - 1; ordinal >= to; ordinal-- {
		for _, template := range sts.Spec.VolumeClaimTemplates {
			name := fmt.Sprintf("%s-%s-%d", template.Name, sts.Name, ordinal)
			pvc, err := cs.CoreV1().PersistentVolumeClaims(sts.Namespace).Get(ctx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				continue // An absent claim grants no authority over a later claim.
			}
			if err != nil {
				return nil, fmt.Errorf("capturing scale-down PVC %s: %w", name, err)
			}
			if pvc.UID == "" || pvc.ResourceVersion == "" || !pvc.DeletionTimestamp.IsZero() {
				return nil, fmt.Errorf("scale-down PVC %s has no stable live identity", name)
			}
			if err := identity.validatePVCIdentity(ctx, template.Name, pvc); err != nil {
				return nil, err
			}
			claim := scaleDownPVCClaim{Name: name, Ordinal: ordinal, Volume: template.Name, UID: pvc.UID, InitialResourceVersion: pvc.ResourceVersion, VolumeName: pvc.Spec.VolumeName}
			pod := &corev1.Pod{}
			err = reader.Get(ctx, types.NamespacedName{Namespace: sts.Namespace, Name: fmt.Sprintf("%s-%d", sts.Name, ordinal)}, pod)
			if err == nil {
				if pod.UID == "" || !metav1.IsControlledBy(pod, sts) || !podReferencesPVC(pod, template.Name, name) {
					return nil, fmt.Errorf("scale-down PVC %s has a foreign or incompatible Pod", name)
				}
				claim.PodUID = pod.UID
			} else if !apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("capturing scale-down Pod identity: %w", err)
			}
			if claim.VolumeName != "" {
				pv, err := cs.CoreV1().PersistentVolumes().Get(ctx, claim.VolumeName, metav1.GetOptions{})
				if err != nil {
					return nil, fmt.Errorf("capturing scale-down PV %s: %w", claim.VolumeName, err)
				}
				if pv.UID == "" || !pv.DeletionTimestamp.IsZero() || !scaleDownPVBoundToClaim(pv, pvc) {
					return nil, fmt.Errorf("scale-down PV %s has an inconsistent binding", pv.Name)
				}
				claim.PVUID, claim.ClaimRef = pv.UID, pv.Spec.ClaimRef.DeepCopy()
			}
			p.Claims = append(p.Claims, claim)
		}
	}

	return p, nil
}

// A refreshed cache read must not turn a captured pre-reduction plan into
// authority for a different generation or an already-reduced workload.
func sameScaleDownSource(expected, live *appsv1.StatefulSet, ledger *ledgerv1alpha1.Cluster) bool {
	return expected.UID != "" && live.UID == expected.UID && live.Generation == expected.Generation && metav1.IsControlledBy(live, ledger) && live.DeletionTimestamp.IsZero() && apiequality.Semantic.DeepEqual(live.Spec, expected.Spec) && live.Annotations[annotationScaleDownPVCFrom] == expected.Annotations[annotationScaleDownPVCFrom] && live.Annotations[annotationScaleDownPVCPlan] == expected.Annotations[annotationScaleDownPVCPlan]
}

func scaleDownPVBoundToClaim(pv *corev1.PersistentVolume, pvc *corev1.PersistentVolumeClaim) bool {
	ref := pv.Spec.ClaimRef

	return ref != nil && ref.UID == pvc.UID && ref.Namespace == pvc.Namespace && ref.Name == pvc.Name && (ref.Kind == "" || ref.Kind == "PersistentVolumeClaim") && (ref.APIVersion == "" || ref.APIVersion == "v1")
}

func readScaleDownPVCPlan(sts *appsv1.StatefulSet, ledger *ledgerv1alpha1.Cluster) (*scaleDownPVCPlan, error) {
	var p scaleDownPVCPlan
	if err := json.Unmarshal([]byte(sts.Annotations[annotationScaleDownPVCPlan]), &p); err != nil {
		return nil, fmt.Errorf("pending PVC cleanup has no valid identity inventory: %w", err)
	}
	if p.Version != 1 || p.ClusterUID == "" || p.ClusterUID != ledger.UID || p.StatefulSetUID == "" || p.StatefulSetUID != sts.UID || p.To < 0 || p.From <= p.To {
		return nil, errors.New("pending PVC cleanup inventory has an invalid source identity or replica range")
	}
	seen := make(map[string]bool, len(p.Claims))
	for _, c := range p.Claims {
		if seen[c.Name] || c.Ordinal < p.To || c.Ordinal >= p.From || c.UID == "" || c.InitialResourceVersion == "" || c.Name != fmt.Sprintf("%s-%s-%d", c.Volume, sts.Name, c.Ordinal) || !slices.ContainsFunc(sts.Spec.VolumeClaimTemplates, func(t corev1.PersistentVolumeClaim) bool { return t.Name == c.Volume }) {
			return nil, errors.New("pending PVC cleanup inventory has an invalid claim identity")
		}
		if (c.VolumeName == "") != (c.PVUID == "") || (c.VolumeName == "") != (c.ClaimRef == nil) {
			return nil, errors.New("pending PVC cleanup inventory has an incomplete volume binding")
		}
		seen[c.Name] = true
	}

	return &p, nil
}

func (p *scaleDownPVCPlan) validateSource(ctx context.Context, reader client.Reader, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet) (*appsv1.StatefulSet, error) {
	live := &appsv1.StatefulSet{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(sts), live); err != nil {
		return nil, fmt.Errorf("reading StatefulSet before PVC cleanup: %w", err)
	}
	if live.UID != p.StatefulSetUID || ledger.UID != p.ClusterUID || !metav1.IsControlledBy(live, ledger) || !live.DeletionTimestamp.IsZero() || live.Spec.Replicas == nil || *live.Spec.Replicas >= p.From || *live.Spec.Replicas < p.To || live.Annotations[annotationScaleDownPVCPlan] != sts.Annotations[annotationScaleDownPVCPlan] || live.Annotations[annotationScaleDownPVCFrom] != sts.Annotations[annotationScaleDownPVCFrom] || live.Spec.PersistentVolumeClaimRetentionPolicy == nil || live.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled != appsv1.DeletePersistentVolumeClaimRetentionPolicyType {
		return nil, errors.New("StatefulSet identity, cleanup inventory or retention changed before PVC deletion")
	}

	return live, nil
}

func deleteScaledDownPVCs(ctx context.Context, cs kubernetes.Interface, reader client.Reader, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, p *scaleDownPVCPlan) error {
	for _, expected := range p.Claims {
		live, err := p.validateSource(ctx, reader, ledger, sts)
		if err != nil {
			return err
		}
		if expected.Ordinal < *live.Spec.Replicas {
			continue // A still-active ordinal is never a cleanup target.
		}
		pvc, err := p.readClaim(ctx, cs, sts.Namespace, sts.Name, expected)
		if err != nil {
			return err
		}
		if pvc == nil || !pvc.DeletionTimestamp.IsZero() {
			continue
		}
		// ResourceVersion is refreshed only after identity and binding checks.
		// This bounds changes to this PVC, not an atomic PVC/PV transaction.
		if err := cs.CoreV1().PersistentVolumeClaims(sts.Namespace).Delete(ctx, pvc.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pvc.UID, ResourceVersion: &pvc.ResourceVersion}}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting PVC %s: %w", pvc.Name, err)
		}
	}

	return nil
}

// Read from the frozen inventory both before resumed Raft work and before a
// DELETE. Neither a new resourceVersion nor a restart changes its authority.
func (p *scaleDownPVCPlan) readClaim(ctx context.Context, cs kubernetes.Interface, namespace, stsName string, expected scaleDownPVCClaim) (*corev1.PersistentVolumeClaim, error) {
	pvc, err := cs.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading scale-down PVC %s: %w", expected.Name, err)
	}
	if pvc.UID != expected.UID || pvc.ResourceVersion == "" {
		return nil, fmt.Errorf("scale-down PVC %s identity changed", pvc.Name)
	}
	if !pvc.DeletionTimestamp.IsZero() {
		return pvc, nil // This exact claim already has an accepted deletion.
	}
	for _, ref := range pvc.OwnerReferences {
		if ref.APIVersion == appsv1.SchemeGroupVersion.String() && ref.Kind == "StatefulSet" && ref.Name == stsName && ref.UID == p.StatefulSetUID {
			continue
		}
		if ref.APIVersion == "v1" && ref.Kind == "Pod" && ref.Name == fmt.Sprintf("%s-%d", stsName, expected.Ordinal) && expected.PodUID != "" && ref.UID == expected.PodUID {
			continue
		}

		return nil, fmt.Errorf("scale-down PVC %s has a foreign owner", pvc.Name)
	}
	if pvc.Spec.VolumeName != expected.VolumeName {
		return nil, fmt.Errorf("scale-down PVC %s binding changed", pvc.Name)
	}
	if expected.VolumeName != "" {
		pv, err := cs.CoreV1().PersistentVolumes().Get(ctx, expected.VolumeName, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("reading scale-down PV: %w", err)
		}
		if pv.UID != expected.PVUID || !pv.DeletionTimestamp.IsZero() || !scaleDownPVBoundToClaim(pv, pvc) || !apiequality.Semantic.DeepEqual(pv.Spec.ClaimRef, expected.ClaimRef) {
			return nil, fmt.Errorf("scale-down PV %s identity or binding changed", pv.Name)
		}
	}

	return pvc, nil
}
