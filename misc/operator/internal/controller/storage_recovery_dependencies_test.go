package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	clienttesting "k8s.io/client-go/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// Start a real journal through the production preparation path, including its
// first orphan delete. Subsequent Reconcile calls must restore from this journal.
func newStorageRecoveryDependenciesFixture(t *testing.T) *claimRecoveryFixture {
	t.Helper()
	f := newClaimRecoveryFixture(t)
	f.ledger = f.latestLedger(t)
	f.ledger.Labels = map[string]string{"tier": "gold"}
	require.NoError(t, f.client.Update(t.Context(), f.ledger))
	f.source.Spec.Template.Spec.Containers[0].Env = []corev1.EnvVar{
		{Name: "TLS_MODE", Value: tlsModeRequired},
		{Name: "AUTH_ENABLED", Value: "true"},
		{Name: "AUTH_ED25519_KEYS", Value: "/auth-keys/auth-keys.json"},
		{Name: "CLUSTER_SECRET", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: clusterSecretName(f.ledger.Name)}, Key: clusterSecretKey,
		}}},
	}
	f.source.Spec.Template.Spec.Volumes = append(f.source.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: "auth-keys", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: authKeysConfigMapName(f.ledger.Name)},
		}},
	})
	f.source.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "auth-keys", MountPath: "/auth-keys", ReadOnly: true}}
	require.NoError(t, f.client.Update(t.Context(), f.source))
	cred := matchingCredentials("recovery-credential", f.ledger.Labels, true, f.ledger.Namespace, "recovery-key")
	cred.Spec.Superuser = true
	require.NoError(t, f.client.Create(t.Context(), cred))
	require.NoError(t, f.client.Create(t.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "recovery-key", Namespace: f.ledger.Namespace},
		Data:       map[string][]byte{"key-id": []byte("original-key"), "pubkey.hex": []byte("original-public-key")},
	}))
	_, err := f.reconciler(f.client).startClaimTemplateRecovery(t.Context(), f.ledger, f.source, f.claims)
	require.NoError(t, err)
	require.True(t, apierrors.IsNotFound(f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &appsv1.StatefulSet{})))

	return f
}

func storageRecoveryTypedClient(f *claimRecoveryFixture, protected bool) *kubefake.Clientset {
	objects := make([]runtime.Object, 0, len(f.pvcs)+len(f.pvs))
	for _, pvc := range f.pvcs {
		retainedPVC := pvc.DeepCopy()
		retainedPVC.ResourceVersion = "1"
		retainedPVC.Labels = map[string]string{"keep": "pvc"}
		if protected {
			retainedPVC.Labels[labelDeletionProtection] = labelDeletionProtectionValue
		}
		objects = append(objects, retainedPVC)
	}
	for _, pv := range f.pvs {
		retainedPV := pv.DeepCopy()
		retainedPV.ResourceVersion = "1"
		retainedPV.Labels = map[string]string{"keep": "pv"}
		if protected {
			retainedPV.Labels[labelDeletionProtection] = labelDeletionProtectionValue
		}
		objects = append(objects, retainedPV)
	}

	return kubefake.NewClientset(objects...)
}

func requireStorageRecoveryProtection(t *testing.T, f *claimRecoveryFixture, c *kubefake.Clientset, protected bool) {
	t.Helper()
	for _, original := range f.pvcs {
		pvc, err := c.CoreV1().PersistentVolumeClaims(original.Namespace).Get(t.Context(), original.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.Equal(t, "pvc", pvc.Labels["keep"])
		require.Equal(t, protected, pvc.Labels[labelDeletionProtection] == labelDeletionProtectionValue)
		pv, err := c.CoreV1().PersistentVolumes().Get(t.Context(), original.Spec.VolumeName, metav1.GetOptions{})
		require.NoError(t, err)
		require.Equal(t, "pv", pv.Labels["keep"])
		require.Equal(t, protected, pv.Labels[labelDeletionProtection] == labelDeletionProtectionValue)
	}
}

func TestStorageRecoveryDependenciesPreserveCurrentAdditionalLabels(t *testing.T) {
	t.Parallel()
	f := newStorageRecoveryDependenciesFixture(t)
	_, snapshot := f.journal(t)
	desired := f.latestLedger(t)
	desired.Spec.AdditionalLabels = map[string]string{"team": "ledger", "previous": "label", labelManagedBy: "tenant"}
	require.NoError(t, f.client.Update(t.Context(), desired))
	cm := &corev1.ConfigMap{}
	cmKey := client.ObjectKey{Namespace: f.ledger.Namespace, Name: authKeysConfigMapName(f.ledger.Name)}
	require.NoError(t, f.client.Get(t.Context(), cmKey, cm))
	secret := &corev1.Secret{}
	secretKey := client.ObjectKey{Namespace: f.ledger.Namespace, Name: clusterSecretName(f.ledger.Name)}
	require.NoError(t, f.client.Get(t.Context(), secretKey, secret))
	// Seed the labels a preceding normal reconciliation applies to both objects.
	for _, obj := range []client.Object{cm, secret} {
		obj.SetLabels(commonLabels(desired))
		require.NoError(t, f.client.Update(t.Context(), obj))
	}
	originalCMUID, originalSecretUID := cm.UID, secret.UID
	require.NotEmpty(t, originalCMUID)
	require.NotEmpty(t, originalSecretUID)
	originalKeys := cm.DeepCopy().Data
	originalToken := bytes.Clone(secret.Data[clusterSecretKey])
	require.NotEmpty(t, originalToken)
	r := f.reconciler(f.client)
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.ledger)}
	for _, additionalLabels := range []map[string]string{
		desired.Spec.AdditionalLabels,
		{"team": "platform", "current": "label", labelManagedBy: "another-tenant"},
	} {
		desired = f.latestLedger(t)
		desired.Spec.AdditionalLabels = additionalLabels
		require.NoError(t, f.client.Update(t.Context(), desired))
		for range 2 {
			result, err := r.Reconcile(t.Context(), request)
			require.NoError(t, err)
			require.Positive(t, result.RequeueAfter)
			require.NotNil(t, f.latestLedger(t).Status.ClaimTemplateRecovery, "exercise repeated active recovery, before adoption")
			require.Equal(t, snapshot.StatefulSet.Spec, f.replacement(t).Spec, "labels must not change the saved workload")
			require.NoError(t, f.client.Get(t.Context(), cmKey, cm))
			require.Equal(t, commonLabels(desired), cm.Labels)
			require.Equal(t, originalCMUID, cm.UID)
			require.Equal(t, originalKeys, cm.Data)
			require.NoError(t, f.client.Get(t.Context(), secretKey, secret))
			require.Equal(t, commonLabels(desired), secret.Labels)
			require.Equal(t, originalSecretUID, secret.UID)
			require.True(t, bytes.Equal(originalToken, secret.Data[clusterSecretKey]), "label reconciliation must preserve the inter-node token")
		}
	}
}

func TestStorageRecoveryAuthKeysRolloutDeferredClearsAfterSuccessfulNormalWorkloadPass(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	ledger := newMinimalCluster()
	ledger.UID = "normal-cluster"
	ledger.Generation = 9
	ledger.Labels = map[string]string{"tier": "gold"}
	ledger.Spec.Image.Tag = "v3.0.0-rc.1"
	ledger.Spec.Auth = &ledgerv1alpha1.AuthorizationConfig{Enabled: new(true)}
	ledger.Spec.TLS = &ledgerv1alpha1.TLSConfig{Enabled: true, SecretName: "synthetic-tls"}
	applyDefaults(ledger)
	previousKeys := []credentialsKeyInfo{{ConfigMapPrefix: "credentials", CredentialsName: "previous-credential", KeyID: "previous-key", PublicKey: "synthetic-previous-public-key", Scopes: []string{"read"}}}
	hash := computeSpecHash(&ledger.Spec)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: resourceName(ledger.Name), Namespace: ledger.Namespace, UID: "normal-sts", Generation: 1, CreationTimestamp: metav1.Now(),
			Annotations: map[string]string{annotationPodConfigHash: hash, annotationPodConfigSpecHash: hash},
		},
		Spec: buildStatefulSetSpec(ledger, hash, previousKeys, tlsModeRequired),
	}
	require.NoError(t, controllerutil.SetControllerReference(ledger, sts, scheme))
	originalSpec := sts.Spec.DeepCopy()
	ledger.Spec.PodAnnotations = map[string]string{"normal-workload-pass": "applied"}
	deferred := metav1.Condition{
		Type: "AuthKeysRolloutDeferred", Status: metav1.ConditionTrue, Reason: "StorageRecovery",
		Message: "Auth-key changes await the normal workload pass", ObservedGeneration: ledger.Generation, LastTransitionTime: metav1.Now(),
	}
	// Recovery cleanup has removed its pointer and journal, but its signal must
	// survive until the normal StatefulSet pass can apply the refreshed keys.
	ledger.Status.Conditions = []metav1.Condition{deferred}
	cred := matchingCredentials("normal-credential", ledger.Labels, false, "", "")
	cm := authKeysConfigMapWithEntries(ledger.Name, ledger.Namespace, map[string]struct{ keyID, pubKey string }{
		"previous-credential": {keyID: "previous-key", pubKey: "synthetic-previous-public-key"},
	})
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(ledger, sts).WithObjects(ledger, sts, cred, cm).Build()
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ledger), ledger))
	deferred = *meta.FindStatusCondition(ledger.Status.Conditions, "AuthKeysRolloutDeferred")
	workloadFailure := errors.New("normal StatefulSet update unavailable once")
	workloadAttempts := 0
	wrapped := interceptor.NewClient(c, interceptor.Funcs{Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if _, ok := obj.(*appsv1.StatefulSet); ok {
			options := &client.UpdateOptions{}
			options.ApplyOptions(opts)
			if len(options.DryRun) == 0 {
				workloadAttempts++
				if workloadAttempts == 1 {
					return workloadFailure
				}
			}
		}

		return c.Update(ctx, obj, opts...)
	}})
	r := &ClusterReconciler{Client: wrapped, APIReader: c, Scheme: scheme}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ledger)}
	result, err := r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, authKeysPendingRequeueInterval, result.RequeueAfter)
	require.Zero(t, workloadAttempts, "pending auth keys must defer the workload pass")
	latest := &ledgerv1alpha1.Cluster{}
	require.NoError(t, c.Get(t.Context(), request.NamespacedName, latest))
	require.Nil(t, latest.Status.ClaimTemplateRecovery)
	require.Equal(t, &deferred, meta.FindStatusCondition(latest.Status.Conditions, "AuthKeysRolloutDeferred"))
	pending := meta.FindStatusCondition(latest.Status.Conditions, "AuthKeysPending")
	require.NotNil(t, pending)
	require.Equal(t, string(authKeysPendingNoneDistributed), pending.Reason)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(sts), sts))
	require.Equal(t, *originalSpec, sts.Spec)

	cred.Status.DistributedSecretRefs = []ledgerv1alpha1.SecretReference{{Namespace: ledger.Namespace, Name: "normal-key"}}
	require.NoError(t, c.Update(t.Context(), cred))
	require.NoError(t, c.Create(t.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ledger.Namespace, Name: "normal-key"},
		Data:       map[string][]byte{"key-id": []byte("current-key"), "pubkey.hex": []byte("synthetic-current-public-key")},
	}))
	_, err = r.Reconcile(t.Context(), request)
	require.ErrorIs(t, err, workloadFailure)
	require.Equal(t, 1, workloadAttempts)
	require.NoError(t, c.Get(t.Context(), request.NamespacedName, latest))
	require.Nil(t, latest.Status.ClaimTemplateRecovery)
	require.Equal(t, &deferred, meta.FindStatusCondition(latest.Status.Conditions, "AuthKeysRolloutDeferred"))
	require.Nil(t, meta.FindStatusCondition(latest.Status.Conditions, "AuthKeysPending"), "the failure must occur after auth keys resolve")
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(sts), sts))
	require.Equal(t, *originalSpec, sts.Spec)

	_, err = r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, 2, workloadAttempts, "retry the failed workload write exactly once")
	require.NoError(t, c.Get(t.Context(), request.NamespacedName, latest))
	require.Nil(t, latest.Status.ClaimTemplateRecovery)
	require.Nil(t, meta.FindStatusCondition(latest.Status.Conditions, "AuthKeysRolloutDeferred"))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(sts), sts))
	require.Equal(t, "applied", sts.Spec.Template.Annotations["normal-workload-pass"])
	wantKeys := []credentialsKeyInfo{{ConfigMapPrefix: "credentials", CredentialsName: cred.Name, KeyID: "current-key", PublicKey: "synthetic-current-public-key", Scopes: cred.Spec.Scopes}}
	require.Equal(t, computeAuthKeysHash(wantKeys), sts.Spec.Template.Annotations[annotationAuthKeysHash])
	require.Zero(t, sts.Status.ReadyReplicas, "clearing the deferred signal does not prove Pod rollout completion")
}

func TestStorageRecoveryDependenciesReconcileIgnoresLaterInvalidWorkloadSpec(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"scopes", "rotation", "unresolved", "removal"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			f := newStorageRecoveryDependenciesFixture(t)
			journal, snapshot := f.journal(t)
			cm := &corev1.ConfigMap{}
			cmKey := client.ObjectKey{Namespace: f.ledger.Namespace, Name: authKeysConfigMapName(f.ledger.Name)}
			require.NoError(t, f.client.Get(t.Context(), cmKey, cm))
			originalCMUID := cm.UID
			clusterSecret := &corev1.Secret{}
			secretKey := client.ObjectKey{Namespace: f.ledger.Namespace, Name: clusterSecretName(f.ledger.Name)}
			require.NoError(t, f.client.Get(t.Context(), secretKey, clusterSecret))
			originalToken := string(clusterSecret.Data[clusterSecretKey])
			if change == "rotation" {
				// Recreate a missing dependency from the saved TLS template even
				// though desired TLS is now off.
				require.NoError(t, f.client.Delete(t.Context(), clusterSecret))
			}

			cred := &ledgerv1alpha1.Credentials{}
			require.NoError(t, f.client.Get(t.Context(), client.ObjectKey{Name: "recovery-credential"}, cred))
			cred.Spec.Scopes = []string{"ledger:read"}
			cred.Spec.Superuser = false
			switch change {
			case "removal":
				require.NoError(t, f.client.Delete(t.Context(), cred))
			case "rotation":
				key := &corev1.Secret{}
				require.NoError(t, f.client.Get(t.Context(), client.ObjectKey{Namespace: f.ledger.Namespace, Name: "recovery-key"}, key))
				key.Data = map[string][]byte{"key-id": []byte("rotated-key"), "pubkey.hex": []byte("rotated-public-key")}
				require.NoError(t, f.client.Update(t.Context(), key))
			case "unresolved":
				cred.Status.DistributedSecretRefs = nil
			}
			if change != "removal" {
				require.NoError(t, f.client.Update(t.Context(), cred))
			}
			desired := f.latestLedger(t)
			desired.Spec.TLS = nil
			desired.Spec.Auth = &ledgerv1alpha1.AuthorizationConfig{Enabled: new(false)}
			desired.Spec.Replicas = new(int32(2))
			desired.Spec.Persistence.WAL.HostPath = &ledgerv1alpha1.HostPathVolumeSpec{Path: "/later-wal"}
			desired.Spec.Persistence.Data.HostPath = &ledgerv1alpha1.HostPathVolumeSpec{Path: "/later-data"}
			desired.Spec.Health = &ledgerv1alpha1.HealthConfig{Interval: "invalid"}
			protect := change != "removal"
			desired.Spec.Persistence.DeletionProtection = &protect
			require.Error(t, validateSpec(desired))
			require.NoError(t, f.client.Update(t.Context(), desired))
			// A stale cache must not decide credential removal, auth policy or
			// the current explicit deletion-protection toggle.
			cached := interceptor.NewClient(f.client, interceptor.Funcs{List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return errors.New("cached credential list must not be used during recovery")
			}})
			r := f.reconciler(cached)
			r.APIReader = f.client
			r.Clientset = storageRecoveryTypedClient(f, !protect)
			result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(desired)})
			require.NoError(t, err)
			require.Positive(t, result.RequeueAfter)
			require.Equal(t, snapshot.StatefulSet.Spec, f.replacement(t).Spec, "never rebuild from newer desired auth, TLS, replicas or hostPath")
			afterJournal, _ := f.journal(t)
			require.Equal(t, journal.Data, afterJournal.Data)
			require.NoError(t, f.client.Get(t.Context(), cmKey, cm))
			require.Equal(t, originalCMUID, cm.UID, "preserve the mounted ConfigMap identity even on explicit removal")
			var keys authKeysJSON
			require.NoError(t, json.Unmarshal([]byte(cm.Data["auth-keys.json"]), &keys))
			if change == "removal" {
				require.JSONEq(t, `{"keys":[]}`, cm.Data["auth-keys.json"])
				require.Len(t, cm.Data, 1, "remove revoked public-key blobs too")
			} else {
				require.Len(t, keys.Keys, 1)
				require.Equal(t, []string{"ledger:read"}, keys.Keys[0].Scopes)
				require.False(t, keys.Keys[0].Superuser)
				wantKey := "original-key"
				if change == "rotation" {
					wantKey = "rotated-key"
				}
				require.Equal(t, wantKey, keys.Keys[0].KeyID)
				require.Equal(t, strings.TrimSuffix(wantKey, "-key")+"-public-key", cm.Data[pubKeyFileName("credentials", cred.Name)])
			}
			require.NoError(t, f.client.Get(t.Context(), secretKey, clusterSecret))
			require.NotEmpty(t, clusterSecret.Data[clusterSecretKey])
			if change != "rotation" {
				require.Equal(t, originalToken, string(clusterSecret.Data[clusterSecretKey]), "preserve the token still used by running peers")
			}
			requireStorageRecoveryProtection(t, f, r.Clientset.(*kubefake.Clientset), protect)
			latest := f.latestLedger(t)
			require.Nil(t, latest.Status.TLSRollout, "dependency maintenance must not advance TLS state")
			require.Nil(t, meta.FindStatusCondition(latest.Status.Conditions, "ConfigValid"), "invalid later desired configuration must not gate snapshot restoration")
			condition := meta.FindStatusCondition(latest.Status.Conditions, "AuthKeysRolloutDeferred")
			require.NotNil(t, condition)
			require.Contains(t, condition.Message, "only after a later restart or rollout")
			if change == "removal" {
				require.Contains(t, condition.Message, "rejects on new starts")
			}
		})
	}
}

func TestStorageRecoveryDependenciesUnresolvedMatchedKeysStayPending(t *testing.T) {
	t.Parallel()
	f := newStorageRecoveryDependenciesFixture(t)
	cred := &ledgerv1alpha1.Credentials{}
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKey{Name: "recovery-credential"}, cred))
	cred.Status.DistributedSecretRefs = nil
	require.NoError(t, f.client.Update(t.Context(), cred))
	cm := &corev1.ConfigMap{}
	cmKey := client.ObjectKey{Namespace: f.ledger.Namespace, Name: authKeysConfigMapName(f.ledger.Name)}
	require.NoError(t, f.client.Get(t.Context(), cmKey, cm))
	delete(cm.Data, pubKeyFileName("credentials", cred.Name))
	require.NoError(t, f.client.Update(t.Context(), cm))
	before := cm.DeepCopy()
	_, err := f.reconciler(f.client).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.ledger)})
	require.NoError(t, err)
	f.replacement(t)
	require.NoError(t, f.client.Get(t.Context(), cmKey, cm))
	require.Equal(t, before.UID, cm.UID)
	require.Equal(t, before.Data, cm.Data, "unresolved matching credentials must not be treated as explicit removal")
	condition := meta.FindStatusCondition(f.latestLedger(t).Status.Conditions, "AuthKeysPending")
	require.NotNil(t, condition)
	require.Equal(t, string(authKeysPendingNoneDistributed), condition.Reason)
}

func TestStorageRecoveryDependenciesFailureDoesNotPreventRecreate(t *testing.T) {
	t.Parallel()
	for _, failRestore := range []bool{false, true} {
		t.Run(strconvBool(failRestore), func(t *testing.T) {
			t.Parallel()
			f := newStorageRecoveryDependenciesFixture(t)
			_, snapshot := f.journal(t)
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: f.ledger.Namespace, Name: clusterSecretName(f.ledger.Name)}}
			require.NoError(t, f.client.Delete(t.Context(), secret))
			projectionFailure := errors.New("auth projection unavailable once")
			restoreFailure := errors.New("saved StatefulSet create unavailable once")
			projectionAttempts, restoreAttempts := 0, 0
			c := interceptor.NewClient(f.client, interceptor.Funcs{
				Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					if _, ok := obj.(*corev1.ConfigMap); ok && obj.GetName() == authKeysConfigMapName(f.ledger.Name) {
						projectionAttempts++
						if projectionAttempts == 1 {
							return projectionFailure
						}
					}

					return c.Update(ctx, obj, opts...)
				},
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if _, ok := obj.(*appsv1.StatefulSet); ok {
						restoreAttempts++
						if failRestore && restoreAttempts == 1 {
							return restoreFailure
						}
					}

					return c.Create(ctx, obj, opts...)
				},
			})
			// Force a projection write instead of relying on no-op updates.
			cred := &ledgerv1alpha1.Credentials{}
			require.NoError(t, f.client.Get(t.Context(), client.ObjectKey{Name: "recovery-credential"}, cred))
			cred.Spec.Scopes = []string{"ledger:read"}
			require.NoError(t, f.client.Update(t.Context(), cred))
			r := f.reconciler(c)
			r.Clientset = storageRecoveryTypedClient(f, false)
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.ledger)}
			_, err := r.Reconcile(t.Context(), request)
			require.ErrorIs(t, err, projectionFailure)
			require.Equal(t, 1, restoreAttempts, "dependency errors must not prevent the restore attempt")
			if failRestore {
				require.ErrorIs(t, err, restoreFailure, "join the independent and recovery failures")
			} else {
				require.Equal(t, snapshot.StatefulSet.Spec, f.replacement(t).Spec)
			}
			require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(secret), secret), "secret maintenance must still run after auth projection failure")
			requireStorageRecoveryProtection(t, f, r.Clientset.(*kubefake.Clientset), true)
			_, err = r.Reconcile(t.Context(), request)
			require.NoError(t, err)
			require.Equal(t, 2, projectionAttempts)
			wantRestoreAttempts := 1
			if failRestore {
				wantRestoreAttempts = 2
			}
			require.Equal(t, wantRestoreAttempts, restoreAttempts)
			require.Equal(t, snapshot.StatefulSet.Spec, f.replacement(t).Spec)
			cm := &corev1.ConfigMap{}
			require.NoError(t, f.client.Get(t.Context(), client.ObjectKey{Namespace: f.ledger.Namespace, Name: authKeysConfigMapName(f.ledger.Name)}, cm))
			require.Contains(t, cm.Data["auth-keys.json"], "ledger:read")
		})
	}
}

func strconvBool(value bool) string {
	if value {
		return "restore fails once"
	}

	return "restore succeeds immediately"
}

func TestStorageRecoveryAuthConfigUsesSavedEnv(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		env     []corev1.EnvVar
		enabled bool
		issuer  string
		issuers []string
		wantErr bool
	}{
		{name: "explicit enabled", env: []corev1.EnvVar{{Name: "AUTH_ENABLED", Value: "true"}}, enabled: true},
		{name: "key file auto enables", env: []corev1.EnvVar{{Name: "AUTH_ED25519_KEYS", Value: "/auth-keys/auth-keys.json"}}, enabled: true},
		{name: "disabled", env: []corev1.EnvVar{{Name: "AUTH_ENABLED", Value: "false"}}},
		{name: "issuer", env: []corev1.EnvVar{{Name: "AUTH_ISSUER", Value: "https://issuer.example"}}, issuer: "https://issuer.example"},
		{name: "issuers", env: []corev1.EnvVar{{Name: "AUTH_ISSUERS", Value: "https://one.example,https://two.example"}}, issuers: []string{"https://one.example", "https://two.example"}},
		{name: "unresolvable auth", env: []corev1.EnvVar{{Name: "AUTH_ENABLED", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}}}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sts := &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{
				{Name: "sidecar", Env: []corev1.EnvVar{{Name: "AUTH_ENABLED", Value: "true"}}},
				{Name: "ledger", Env: tt.env},
			}}}}}
			auth, err := storageRecoveryAuthConfig(sts)
			if tt.wantErr {
				require.ErrorContains(t, err, "cannot resolve saved AUTH_ENABLED")

				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.enabled, *auth.Enabled)
			require.Equal(t, tt.issuer, auth.Issuer)
			require.Equal(t, tt.issuers, auth.Issuers)
		})
	}
}

// The typed and controller-runtime fakes represent separate clients. Keep the
// recovery reader's retained identities valid and change only the protection
// client's observation. A protection fence error must not stop STS restoration.
func TestStorageRecoveryProtectionIdentityFailureStillAttemptsRestore(t *testing.T) {
	t.Parallel()
	for _, resource := range []string{"persistentvolumeclaims", "persistentvolumes"} {
		for _, failRestore := range []bool{false, true} {
			t.Run(resource+"/"+strconvBool(failRestore), func(t *testing.T) {
				t.Parallel()
				f := newStorageRecoveryDependenciesFixture(t)
				_, snapshot := f.journal(t)
				cs := storageRecoveryTypedClient(f, false)
				if resource == "persistentvolumeclaims" {
					pvc, err := cs.CoreV1().PersistentVolumeClaims(f.ledger.Namespace).Get(t.Context(), f.pvcs[0].Name, metav1.GetOptions{})
					require.NoError(t, err)
					pvc.UID = "replacement-pvc"
					_, err = cs.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(t.Context(), pvc, metav1.UpdateOptions{})
					require.NoError(t, err)
				} else {
					pv, err := cs.CoreV1().PersistentVolumes().Get(t.Context(), f.pvs[0].Name, metav1.GetOptions{})
					require.NoError(t, err)
					pv.UID = "replacement-pv"
					_, err = cs.CoreV1().PersistentVolumes().Update(t.Context(), pv, metav1.UpdateOptions{})
					require.NoError(t, err)
				}
				patches := 0
				cs.PrependReactor("patch", "*", func(clienttesting.Action) (bool, runtime.Object, error) {
					patches++

					return false, nil, nil
				})
				restoreFailure := errors.New("restore unavailable")
				restoreAttempts := 0
				c := interceptor.NewClient(f.client, interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if _, ok := obj.(*appsv1.StatefulSet); ok {
						restoreAttempts++
						if failRestore {
							return restoreFailure
						}
					}

					return c.Create(ctx, obj, opts...)
				}})
				r := f.reconciler(c)
				r.Clientset = cs
				_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.ledger)})
				require.ErrorContains(t, err, "reconciling storage recovery volume protection: retained")
				require.Equal(t, 1, restoreAttempts, "the dependency fence must not short-circuit the restore")
				if failRestore {
					require.ErrorIs(t, err, restoreFailure, "join the protection and restore failures")
				} else {
					require.Equal(t, snapshot.StatefulSet.Spec, f.replacement(t).Spec)
				}
				require.Zero(t, patches, "reject the first changed pair before either label write")
				requireStorageRecoveryProtection(t, f, cs, false)
			})
		}
	}
}

func TestStorageRecoveryProtectionPatchConflictStillRestoresThenConverges(t *testing.T) {
	t.Parallel()
	f := newStorageRecoveryDependenciesFixture(t)
	_, snapshot := f.journal(t)
	cs := storageRecoveryTypedClient(f, false)
	attempts := 0
	conflict := apierrors.NewConflict(corev1.Resource("persistentvolumeclaims"), f.pvcs[0].Name, errors.New("concurrent volume write"))
	cs.PrependReactor("patch", "persistentvolumeclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
		attempts++
		if attempts == 1 {
			return true, nil, conflict
		}

		return false, nil, nil
	})
	r := f.reconciler(f.client)
	r.Clientset = cs
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.ledger)}
	_, err := r.Reconcile(t.Context(), request)
	require.ErrorIs(t, err, conflict)
	require.Equal(t, 1, attempts)
	requireStorageRecoveryProtection(t, f, cs, false)
	require.Equal(t, snapshot.StatefulSet.Spec, f.replacement(t).Spec, "a label patch conflict must not prevent restoration")
	_, err = r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, 1+len(f.pvcs), attempts, "one failed patch, then one successful patch per retained PVC")
	requireStorageRecoveryProtection(t, f, cs, true)
	require.Equal(t, snapshot.StatefulSet.Spec, f.replacement(t).Spec)
}
