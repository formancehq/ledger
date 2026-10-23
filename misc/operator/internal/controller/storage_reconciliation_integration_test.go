//go:build integration

package controller

import (
	"encoding/json"
	"fmt"
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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// The shared suite manager creates the source. Pause normal reconciliation with
// invalid replicas only while installing the Pod/binding/status observations
// that envtest's absent StatefulSet controller, CSI and kubelet would supply.
func storageManagerFixture(t *testing.T, legacyHash, implicitClass bool) (*ledgerv1alpha1.Cluster, *appsv1.StatefulSet, string, retainedClaimIdentity) {
	t.Helper()
	namespace := createTestNamespace(t)
	className := "storage-manager-" + namespace
	class := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: className}, Provisioner: "example.csi", AllowVolumeExpansion: new(true)}
	require.NoError(t, k8sClient.Create(ctx, class))
	cluster := newCluster("storage-manager", namespace)
	cluster.Spec.Replicas = new(int32(1))
	cluster.Spec.Persistence = ledgerv1alpha1.PersistenceSpec{
		DeletionProtection: new(false),
		WAL:                ledgerv1alpha1.VolumeSpec{HostPath: &ledgerv1alpha1.HostPathVolumeSpec{Path: "/data/wal"}},
		Data:               ledgerv1alpha1.VolumeSpec{Size: resource.MustParse("100Gi")},
	}
	if !implicitClass {
		cluster.Spec.Persistence.Data.StorageClass = className
	}
	require.NoError(t, k8sClient.Create(ctx, cluster))
	objects := []client.Object{cluster, class}
	t.Cleanup(func() {
		// Delete the Cluster first, stopping the manager before releasing the
		// finalizer that holds its accepted orphan delete for inspection.
		for _, obj := range objects {
			if err := k8sClient.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
				assert.NoError(t, err)
				continue
			}
			latest := obj.DeepCopyObject().(client.Object)
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), latest); apierrors.IsNotFound(err) {
				continue
			} else if !assert.NoError(t, err) {
				continue
			}
			if len(latest.GetFinalizers()) != 0 {
				before := latest.DeepCopyObject().(client.Object)
				latest.SetFinalizers(nil)
				assert.NoError(t, k8sClient.Patch(ctx, latest, client.MergeFrom(before)))
			}
		}
	})
	source := &appsv1.StatefulSet{}
	sourceKey := client.ObjectKey{Namespace: namespace, Name: resourceName(cluster.Name)}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, sourceKey, source) == nil && hasPodConfigHashBinding(source)
	}, "the manager must create the source with its installed workload hash")
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster))
	original := cluster.DeepCopy()
	before := cluster.DeepCopy()
	cluster.Spec.Replicas = new(int32(2))
	require.NoError(t, k8sClient.Patch(ctx, cluster, client.MergeFrom(before)))
	requireEventually(t, func() bool {
		if k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster) != nil {
			return false
		}
		condition := meta.FindStatusCondition(cluster.Status.Conditions, "ConfigValid")
		return condition != nil && condition.Status == metav1.ConditionFalse && condition.ObservedGeneration == cluster.Generation
	}, "fixture preparation must wait until the manager has paused")
	require.NoError(t, k8sClient.Get(ctx, sourceKey, source))
	source.Finalizers = append(source.Finalizers, "test.ledger.formance.com/hold-storage-source")
	if legacyHash {
		applyDefaults(original)
		source.Spec.Template.Annotations[annotationSpecHash] = computePodSpecHash(&original.Spec, false)
		delete(source.Annotations, annotationPodConfigHash)
		delete(source.Annotations, annotationPodConfigSpecHash)
	}
	require.NoError(t, k8sClient.Update(ctx, source))
	objects = append(objects, source.DeepCopy())

	// The live PVC has the effective provisioning class even when the original
	// immutable claim template omitted it (as with a default StorageClass).
	claim := source.Spec.VolumeClaimTemplates[0].DeepCopy()
	claim.Name, claim.Namespace = "data-"+source.Name+"-0", namespace
	claim.Spec.StorageClassName = &className
	claim.Spec.VolumeName = "storage-manager-" + namespace
	require.NoError(t, k8sClient.Create(ctx, claim))
	objects = append(objects, claim)
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: claim.Spec.VolumeName},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: claim.Spec.Resources.Requests.DeepCopy(), AccessModes: claim.Spec.AccessModes,
			StorageClassName: className, PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			ClaimRef:               &corev1.ObjectReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: claim.Name, Namespace: namespace, UID: claim.UID},
			PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: "example.csi", VolumeHandle: claim.Spec.VolumeName}},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, pv))
	objects = append(objects, pv)
	pv.Status.Phase = corev1.VolumeBound
	require.NoError(t, k8sClient.Status().Update(ctx, pv))
	claim.Status.Phase = corev1.ClaimBound
	claim.Status.Capacity = claim.Spec.Resources.Requests.DeepCopy()
	require.NoError(t, k8sClient.Status().Update(ctx, claim))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: source.Name + "-0", Namespace: namespace, Labels: source.Spec.Template.Labels},
		Spec:       *source.Spec.Template.Spec.DeepCopy(),
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim.Name}}})
	require.NoError(t, controllerutil.SetControllerReference(source, pod, k8sClient.Scheme()))
	require.NoError(t, k8sClient.Create(ctx, pod))
	objects = append(objects, pod)
	storageManagerSetConverged(t, source)
	for _, probe := range []*corev1.Probe{source.Spec.Template.Spec.Containers[0].LivenessProbe, source.Spec.Template.Spec.Containers[0].ReadinessProbe, source.Spec.Template.Spec.Containers[0].StartupProbe} {
		require.EqualValues(t, 1, probe.TimeoutSeconds, "the API must have defaulted the source probes")
		require.EqualValues(t, 1, probe.SuccessThreshold)
	}
	return cluster, source, className, retainedClaimIdentity{Name: claim.Name, UID: claim.UID, VolumeName: pv.Name, PVUID: pv.UID}
}

func storageManagerSetConverged(t *testing.T, source *appsv1.StatefulSet) {
	t.Helper()
	source.Status.ObservedGeneration = source.Generation
	source.Status.Replicas, source.Status.UpdatedReplicas, source.Status.ReadyReplicas = 1, 1, 1
	require.NoError(t, k8sClient.Status().Update(ctx, source))
	require.True(t, rolloutConverged(source))
}

func TestStorageRecoveryStartsThroughManagerIntegration(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name                                    string
		legacyHash, explicitClass, workloadEdit bool
	}{
		{name: "API-defaulted probes"},
		{name: "modern hash and explicit default class", explicitClass: true},
		{name: "released hash and explicit default class", legacyHash: true, explicitClass: true},
		{name: "unrelated workload rolls out first", workloadEdit: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cluster, source, className, identity := storageManagerFixture(t, tt.legacyHash, tt.explicitClass)
			original := source.DeepCopy()
			if tt.legacyHash {
				require.Empty(t, source.Annotations[annotationPodConfigHash])
			} else {
				require.NotEmpty(t, source.Annotations[annotationPodConfigHash])
			}
			before := cluster.DeepCopy()
			cluster.Spec.Replicas = new(int32(1))
			cluster.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
			if tt.explicitClass {
				cluster.Spec.Persistence.Data.StorageClass = className
			}
			if tt.workloadEdit {
				cluster.Spec.NodeSelector = map[string]string{"test.ledger.formance.com/workload": "changed"}
			}
			require.NoError(t, k8sClient.Patch(ctx, cluster, client.MergeFrom(before)))
			journalKey := client.ObjectKey{Namespace: cluster.Namespace}
			if tt.workloadEdit {
				requireEventually(t, func() bool {
					if k8sClient.Get(ctx, client.ObjectKeyFromObject(source), source) != nil || k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster) != nil {
						return false
					}
					condition := meta.FindStatusCondition(cluster.Status.Conditions, "ClaimTemplatesReady")
					return source.Spec.Template.Spec.NodeSelector["test.ledger.formance.com/workload"] == "changed" && condition != nil && condition.Reason == "WaitingForRollout"
				}, "a real workload change must be applied before storage recovery")
				require.False(t, rolloutConverged(source))
				require.True(t, source.DeletionTimestamp.IsZero())
				require.Nil(t, cluster.Status.ClaimTemplateRecovery)
				requireNoClaimTemplateRecovery(t, k8sClient, cluster)
				require.Greater(t, source.Generation, original.Generation)
				storageManagerSetConverged(t, source)
				original = source.DeepCopy()
			}

			journal := &corev1.ConfigMap{}
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				require.NoError(collect, k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster))
				require.NotNil(collect, cluster.Status.ClaimTemplateRecovery)
				journalKey.Name = cluster.Status.ClaimTemplateRecovery.ConfigMapName
				require.NoError(collect, k8sClient.Get(ctx, journalKey, journal))
				require.NoError(collect, k8sClient.Get(ctx, client.ObjectKeyFromObject(source), source))
				require.False(collect, source.DeletionTimestamp.IsZero(), "the manager must request the real orphan delete")
				require.Equal(collect, &ledgerv1alpha1.ClaimTemplateRecoveryStatus{ConfigMapName: journal.Name, ConfigMapUID: string(journal.UID), SourceUID: string(original.UID), PayloadSHA256: claimTemplateRecoveryDigest(cluster, []byte(journal.Data[claimTemplateRecoveryKey]))}, cluster.Status.ClaimTemplateRecovery)
			}, 15*time.Second, 100*time.Millisecond)
			require.Contains(t, source.Finalizers, metav1.FinalizerOrphanDependents)
			require.Equal(t, original.UID, source.UID)
			// The API can increment generation when it accepts deletion. Prove
			// absence of a preparatory rollout through the saved source generation
			// and exact live/saved Pod templates instead.
			require.Equal(t, original.Spec.Template, source.Spec.Template)
			require.True(t, metav1.IsControlledBy(journal, cluster))
			require.Equal(t, new(true), journal.Immutable)
			var recovery claimTemplateRecovery
			require.NoError(t, json.Unmarshal([]byte(journal.Data[claimTemplateRecoveryKey]), &recovery))
			require.Equal(t, original.UID, recovery.SourceUID)
			require.Equal(t, original.Generation, recovery.SourceGeneration)
			require.Equal(t, original.Spec.Template, recovery.StatefulSet.Spec.Template, "keep the complete API-defaulted Pod template and installed hash")
			expected := original.Spec.DeepCopy()
			expected.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("200Gi")
			require.Equal(t, *expected, recovery.StatefulSet.Spec, "only the supported claim capacity may change")
			require.Equal(t, []retainedClaimIdentity{identity}, recovery.Claims)
			claim := &corev1.PersistentVolumeClaim{}
			require.NoError(t, k8sClient.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: identity.Name}, claim))
			require.Equal(t, identity.UID, claim.UID)
			require.Equal(t, identity.VolumeName, claim.Spec.VolumeName)
			require.True(t, claim.DeletionTimestamp.IsZero())
			pv := &corev1.PersistentVolume{}
			require.NoError(t, k8sClient.Get(ctx, client.ObjectKey{Name: identity.VolumeName}, pv))
			require.Equal(t, identity.PVUID, pv.UID)
			require.Equal(t, identity.UID, pv.Spec.ClaimRef.UID)
			require.True(t, pv.DeletionTimestamp.IsZero())
		})
	}
}

// Exercise the normal manager ordering: the ServiceAccount is updated before
// StatefulSet reconciliation. Reading that latest account cannot establish the
// annotations under which the old Pods were admitted.
func TestStorageServiceAccountRolloutThroughManagerIntegration(t *testing.T) {
	t.Parallel()
	for _, sourceType := range []string{"modern", "released", "released stale claims"} {
		for _, remove := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/remove=%t", sourceType, remove), func(t *testing.T) {
				t.Parallel()
				cluster, source, _, _ := storageManagerFixture(t, sourceType != "modern", false)
				prior := cluster.DeepCopy()
				applyDefaults(prior)
				role := "arn:aws:iam::111111111111:role/previous"
				if remove {
					prior.Spec.ServiceAccount.Annotations = map[string]string{"eks.amazonaws.com/role-arn": role}
				}
				if remove {
					account := &corev1.ServiceAccount{}
					require.NoError(t, k8sClient.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: serviceAccountName(cluster)}, account))
					account.Annotations = prior.Spec.ServiceAccount.Annotations
					require.NoError(t, k8sClient.Update(ctx, account))
				}
				if sourceType == "released stale claims" {
					prior.Spec.Persistence.Data.Size = resource.MustParse("150Gi")
				}
				hash := computeSpecHash(&prior.Spec)
				if sourceType != "modern" {
					hash = computePodSpecHash(&prior.Spec, false)
				}
				source.Spec.Template.Annotations[annotationSpecHash] = hash
				if sourceType == "modern" {
					recordPodConfigHash(source, hash)
				}
				require.NoError(t, k8sClient.Update(ctx, source))
				storageManagerSetConverged(t, source)
				original := source.DeepCopy()
				before := cluster.DeepCopy()
				if sourceType != "modern" {
					installLegacySpecHashProof(t, cluster, source, &prior.Spec)
				}
				cluster.Spec.Replicas = new(int32(1))
				cluster.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
				if remove {
					cluster.Spec.ServiceAccount.Annotations = nil
				} else {
					role = "arn:aws:iam::111111111111:role/replacement"
					cluster.Spec.ServiceAccount.Annotations = map[string]string{"eks.amazonaws.com/role-arn": role}
				}
				require.NoError(t, k8sClient.Patch(ctx, cluster, client.MergeFrom(before)))
				desired := cluster.DeepCopy()
				applyDefaults(desired)
				desiredHash := computeSpecHash(&desired.Spec)
				account := &corev1.ServiceAccount{}
				requireEventually(t, func() bool {
					if k8sClient.Get(ctx, client.ObjectKeyFromObject(source), source) != nil || k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster) != nil ||
						k8sClient.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: serviceAccountName(cluster)}, account) != nil {
						return false
					}
					appliedRole := role
					if remove {
						appliedRole = ""
					}
					return source.Spec.Template.Annotations[annotationSpecHash] == desiredHash && account.Annotations["eks.amazonaws.com/role-arn"] == appliedRole
				}, "fresh Pod admission must follow the ServiceAccount annotation change before storage recovery")
				require.NotEqual(t, original.Spec.Template.Annotations[annotationSpecHash], source.Spec.Template.Annotations[annotationSpecHash])
				require.Greater(t, source.Generation, original.Generation)
				require.Equal(t, original.UID, source.UID)
				require.Equal(t, original.Spec.VolumeClaimTemplates, source.Spec.VolumeClaimTemplates)
				require.True(t, source.DeletionTimestamp.IsZero())
				require.Nil(t, cluster.Status.ClaimTemplateRecovery)
				requireNoClaimTemplateRecovery(t, k8sClient, cluster)
			})
		}
	}
}

func TestStorageReleasedHashWorkloadRolloutThroughManagerIntegration(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"RC upgrade", "configuration", "enable TLS", "disable TLS"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			cluster, source, _, identity := storageManagerFixture(t, true, false)
			prior := cluster.DeepCopy()
			prior.Spec.Replicas = new(int32(1))
			prior.Spec.Image.Tag = "v3.0.0-rc.1"
			applyDefaults(prior)
			oldTLSMode := tlsModeDisabled
			if change == "disable TLS" {
				prior.Spec.TLS = &ledgerv1alpha1.TLSConfig{Enabled: true, SecretName: "existing-certs", CASecretKey: "custom-ca.pem"}
				oldTLSMode = tlsModeRequired
			}
			oldHash := computePodSpecHash(&prior.Spec, false)
			template := buildStatefulSetSpec(prior, oldHash, nil, oldTLSMode).Template
			template.Annotations[annotationAuthKeysHash] = source.Spec.Template.Annotations[annotationAuthKeysHash]
			source.Spec.Template = template
			require.Empty(t, source.Annotations[annotationPodConfigHash])
			require.NoError(t, k8sClient.Update(ctx, source))
			storageManagerSetConverged(t, source)
			original := source.DeepCopy()
			before := cluster.DeepCopy()
			cluster.Spec.Replicas = new(int32(1))
			cluster.Spec.Image = prior.Spec.Image
			switch change {
			case "RC upgrade":
				cluster.Spec.Image.Tag = "v3.0.0-rc.2"
				require.NoError(t, validateRCUpgrade(cluster.Spec.Image.Tag, source))
			case "configuration":
				cluster.Spec.HttpPort = 9001
			case "enable TLS":
				cluster.Spec.TLS = &ledgerv1alpha1.TLSConfig{Enabled: true, SecretName: "existing-certs", CASecretKey: "custom-ca.pem"}
			}
			require.Empty(t, cluster.Annotations[annotationLegacySpecHashProof])
			require.NoError(t, k8sClient.Patch(ctx, cluster, client.MergeFrom(before)))
			desired := cluster.DeepCopy()
			applyDefaults(desired)
			hash := computeSpecHash(&desired.Spec)
			requireEventually(t, func() bool {
				return k8sClient.Get(ctx, client.ObjectKeyFromObject(source), source) == nil &&
					source.Spec.Template.Annotations[annotationSpecHash] == hash && source.Annotations[annotationPodConfigHash] == hash
			}, "a released source with a changed admitted template must roll without historical proof")
			switch change {
			case "RC upgrade":
				require.Equal(t, "ghcr.io/formancehq/ledger:v3.0.0-rc.2", source.Spec.Template.Spec.Containers[0].Image)
			case "configuration":
				require.Contains(t, source.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "HTTP_PORT", Value: "9001"})
			default:
				requireTLSCertificates(t, source, tlsModeOptional)
			}
			require.NotEqual(t, oldHash, source.Spec.Template.Annotations[annotationSpecHash])
			require.Greater(t, source.Generation, original.Generation)
			require.Equal(t, original.UID, source.UID)
			require.Equal(t, original.Spec.VolumeClaimTemplates, source.Spec.VolumeClaimTemplates)
			require.True(t, source.DeletionTimestamp.IsZero())
			require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster))
			require.Nil(t, cluster.Status.ClaimTemplateRecovery)
			requireNoClaimTemplateRecovery(t, k8sClient, cluster)
			claim := &corev1.PersistentVolumeClaim{}
			require.NoError(t, k8sClient.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: identity.Name}, claim))
			require.Equal(t, identity.UID, claim.UID)
			require.Equal(t, identity.VolumeName, claim.Spec.VolumeName)
			pv := &corev1.PersistentVolume{}
			require.NoError(t, k8sClient.Get(ctx, client.ObjectKey{Name: identity.VolumeName}, pv))
			require.Equal(t, identity.PVUID, pv.UID)
			require.Equal(t, identity.UID, pv.Spec.ClaimRef.UID)
		})
	}
}

func TestStorageReleasedHashServiceAccountRequiresProofThroughManagerIntegration(t *testing.T) {
	t.Parallel()
	for _, storageEdit := range []bool{false, true} {
		t.Run(fmt.Sprintf("storage=%t", storageEdit), func(t *testing.T) {
			t.Parallel()
			cluster, source, _, _ := storageManagerFixture(t, true, false)
			original := source.DeepCopy()
			before := cluster.DeepCopy()
			cluster.Spec.Replicas = new(int32(1))
			cluster.Spec.ServiceAccount.Annotations = map[string]string{"eks.amazonaws.com/role-arn": "arn:aws:iam::111111111111:role/replacement"}
			if storageEdit {
				cluster.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
			}
			require.Empty(t, cluster.Annotations[annotationLegacySpecHashProof])
			require.NoError(t, k8sClient.Patch(ctx, cluster, client.MergeFrom(before)))
			requireEventually(t, func() bool {
				if k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster) != nil {
					return false
				}
				condition := meta.FindStatusCondition(cluster.Status.Conditions, "ClaimTemplatesReady")
				return condition != nil && condition.Reason == "HashMigrationRequired" && condition.ObservedGeneration == cluster.Generation
			}, "equal admitted templates must still require authenticated historical admission inputs")
			require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(source), source))
			require.Equal(t, original.UID, source.UID)
			require.Equal(t, original.Generation, source.Generation)
			require.Equal(t, original.Spec, source.Spec)
			require.Empty(t, source.Annotations[annotationPodConfigHash])
			require.True(t, source.DeletionTimestamp.IsZero())
			require.Nil(t, cluster.Status.ClaimTemplateRecovery)
		})
	}
}

func TestStorageHashMigrationRequiredThroughManagerIntegration(t *testing.T) {
	t.Parallel()
	cluster, source, _, _ := storageManagerFixture(t, true, false)
	prior := cluster.DeepCopy()
	applyDefaults(prior)
	prior.Spec.Persistence.Data.Size = resource.MustParse("150Gi")
	source.Spec.Template.Annotations[annotationSpecHash] = computePodSpecHash(&prior.Spec, false)
	require.NoError(t, k8sClient.Update(ctx, source))
	storageManagerSetConverged(t, source)
	original := source.DeepCopy()
	before := cluster.DeepCopy()
	cluster.Spec.Replicas = new(int32(1))
	cluster.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
	require.NoError(t, k8sClient.Patch(ctx, cluster, client.MergeFrom(before)))
	requireEventually(t, func() bool {
		if k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster) != nil {
			return false
		}
		condition := meta.FindStatusCondition(cluster.Status.Conditions, "ClaimTemplatesReady")
		return condition != nil && condition.Status == metav1.ConditionFalse && condition.Reason == "HashMigrationRequired" && condition.ObservedGeneration == cluster.Generation
	}, "the manager must expose missing historical proof on the actual Cluster")
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(source), source))
	require.Equal(t, original.UID, source.UID)
	require.Equal(t, original.Generation, source.Generation)
	require.Equal(t, original.Spec, source.Spec)
	require.Empty(t, source.Annotations[annotationPodConfigHash], "do not certify unknown admission inputs")
	require.True(t, source.DeletionTimestamp.IsZero())
	require.Nil(t, cluster.Status.ClaimTemplateRecovery)
	journalKey := client.ObjectKey{Namespace: cluster.Namespace}
	requireNoClaimTemplateRecovery(t, k8sClient, cluster)
	before = cluster.DeepCopy()
	installLegacySpecHashProof(t, cluster, source, &prior.Spec)
	require.NoError(t, k8sClient.Patch(ctx, cluster, client.MergeFrom(before)))
	journal := &corev1.ConfigMap{}
	requireEventually(t, func() bool {
		if k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster) != nil || cluster.Status.ClaimTemplateRecovery == nil {
			return false
		}
		journalKey.Name = cluster.Status.ClaimTemplateRecovery.ConfigMapName
		return k8sClient.Get(ctx, journalKey, journal) == nil && k8sClient.Get(ctx, client.ObjectKeyFromObject(source), source) == nil && !source.DeletionTimestamp.IsZero()
	}, "valid historical proof must unblock storage recovery without rolling the installed Pods")
	var recovery claimTemplateRecovery
	require.NoError(t, json.Unmarshal([]byte(journal.Data[claimTemplateRecoveryKey]), &recovery))
	require.Equal(t, original.Spec.Template, source.Spec.Template)
	require.Equal(t, original.Spec.Template, recovery.StatefulSet.Spec.Template)
	require.Equal(t, original.Generation, recovery.SourceGeneration)
	require.Equal(t, original.UID, recovery.SourceUID)
}
