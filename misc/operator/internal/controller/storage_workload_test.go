package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestStorageDeclarationDriftKeepsWorkloadReconciliation(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name        string
		old         func(*ledgerv1alpha1.Cluster)
		edit        func(*ledgerv1alpha1.Cluster)
		unsupported bool
	}{
		{name: "default class made explicit", edit: func(c *ledgerv1alpha1.Cluster) { c.Spec.Persistence.Data.StorageClass = "gp3" }},
		{name: "explicit class omitted", old: func(c *ledgerv1alpha1.Cluster) { c.Spec.Persistence.Data.StorageClass = "gp3" }, edit: func(c *ledgerv1alpha1.Cluster) { c.Spec.Persistence.Data.StorageClass = "" }},
		{name: "different class", old: func(c *ledgerv1alpha1.Cluster) { c.Spec.Persistence.Data.StorageClass = "gp3" }, edit: func(c *ledgerv1alpha1.Cluster) { c.Spec.Persistence.Data.StorageClass = "other" }},
		{name: "different access mode", edit: func(c *ledgerv1alpha1.Cluster) { c.Spec.Persistence.Data.AccessMode = string(corev1.ReadWriteMany) }},
		{name: "cleared volume profile", old: func(c *ledgerv1alpha1.Cluster) { c.Spec.Persistence.Data.VolumeAttributesClassName = "profile" }, edit: func(c *ledgerv1alpha1.Cluster) { c.Spec.Persistence.Data.VolumeAttributesClassName = "" }, unsupported: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			scheme := runtime.NewScheme()
			require.NoError(t, clientgoscheme.AddToScheme(scheme))
			require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
			old := newMinimalCluster()
			old.UID = "cluster"
			applyDefaults(old)
			if tt.old != nil {
				tt.old(old)
			}
			hash := computePodSpecHash(&old.Spec, false)
			sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
				Name: resourceName(old.Name), Namespace: old.Namespace, UID: "source", CreationTimestamp: metav1.Now(), Generation: 1,
			}, Spec: buildStatefulSetSpec(old, hash, nil, "disabled"), Status: appsv1.StatefulSetStatus{ObservedGeneration: 1, UpdatedReplicas: 3, ReadyReplicas: 3}}
			require.NoError(t, controllerutil.SetControllerReference(old, sts, scheme))
			claims := sts.Spec.DeepCopy().VolumeClaimTemplates
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts, old).Build()
			r := &ClusterReconciler{Client: c, Scheme: scheme}
			desired := old.DeepCopy()
			tt.edit(desired)
			desired.Spec.PodAnnotations = map[string]string{"workload-change": "applied"}
			_, err := r.reconcileStatefulSetWithExec(t.Context(), desired, computeSpecHash(&desired.Spec), nil, nil)
			require.NoError(t, err)
			var got appsv1.StatefulSet
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(sts), &got))
			require.Equal(t, sts.UID, got.UID)
			require.Equal(t, claims, got.Spec.VolumeClaimTemplates)
			require.Equal(t, "applied", got.Spec.Template.Annotations["workload-change"])
			condition := meta.FindStatusCondition(desired.Status.Conditions, "ClaimTemplatesReady")
			require.NotNil(t, condition)
			if tt.unsupported {
				require.Equal(t, metav1.ConditionFalse, condition.Status)
				require.Equal(t, "UnsupportedChange", condition.Reason)
			} else {
				// Template alignment retains immutable class/access modes. Live
				// PVC migration diagnostics belong to the storage conditions.
				require.Equal(t, metav1.ConditionTrue, condition.Status)
				require.Equal(t, "Ready", condition.Reason)
			}
			requireNoClaimTemplateRecovery(t, c, old)
		})
	}
}

func requireReleasedHashWorkloadRollout(t *testing.T, prior, desired *ledgerv1alpha1.Cluster, oldTLSMode string) *appsv1.StatefulSet {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	oldHash := computePodSpecHash(&prior.Spec, false)
	source := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: resourceName(prior.Name), Namespace: prior.Namespace, UID: "released-source", Generation: 1, CreationTimestamp: metav1.Now(),
	}, Spec: buildStatefulSetSpec(prior, oldHash, nil, oldTLSMode),
		Status: appsv1.StatefulSetStatus{ObservedGeneration: 1, ReadyReplicas: *prior.Spec.Replicas, UpdatedReplicas: *prior.Spec.Replicas}}
	require.NoError(t, controllerutil.SetControllerReference(prior, source, scheme))
	require.Empty(t, source.Annotations[annotationPodConfigHash], "released operators did not write canonical metadata")
	require.Empty(t, desired.Annotations[annotationLegacySpecHashProof])
	require.NoError(t, validateRCUpgrade(desired.Spec.Image.Tag, source))
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(desired, source).WithObjects(desired, source).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	hash := computeSpecHash(&desired.Spec)
	_, err := r.reconcileStatefulSet(t.Context(), desired, hash, nil)
	require.NoError(t, err, "a changed admitted template must roll without requiring historical proof")
	var got appsv1.StatefulSet
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(source), &got))
	require.Equal(t, source.UID, got.UID)
	require.Equal(t, source.Spec.VolumeClaimTemplates, got.Spec.VolumeClaimTemplates)
	require.Equal(t, hash, got.Annotations[annotationPodConfigHash])
	require.Equal(t, hash, got.Spec.Template.Annotations[annotationSpecHash])
	require.NotEqual(t, oldHash, got.Spec.Template.Annotations[annotationSpecHash])
	requireNoClaimTemplateRecovery(t, c, desired)

	return &got
}

func TestStorageReleasedHashRCUpgradeExplicitTag(t *testing.T) {
	t.Parallel()
	prior := newMinimalCluster()
	prior.UID = "cluster"
	prior.Spec.Image.Repository = "ghcr.io/formancehq/ledger"
	prior.Spec.Image.Tag = "v3.0.0-rc.1"
	applyDefaults(prior)
	desired := prior.DeepCopy()
	desired.Spec.Image.Tag = "v3.0.0-rc.2"
	got := requireReleasedHashWorkloadRollout(t, prior, desired, tlsModeDisabled)
	require.Equal(t, "ghcr.io/formancehq/ledger:v3.0.0-rc.2", got.Spec.Template.Spec.Containers[0].Image)
}

func TestStorageReleasedHashRCUpgradeEnvironmentTag(t *testing.T) {
	// The chart supplies the default tag through the Operator environment.
	// Keep this test serial because applyDefaults reads process-wide settings.
	t.Setenv("LEDGER_IMAGE_REPOSITORY", "ghcr.io/formancehq/ledger")
	t.Setenv("LEDGER_IMAGE_TAG", "v3.0.0-rc.1")
	declared := newMinimalCluster()
	declared.UID = "cluster"
	require.Empty(t, declared.Spec.Image.Tag)
	prior := declared.DeepCopy()
	applyDefaults(prior)
	require.Equal(t, "v3.0.0-rc.1", prior.Spec.Image.Tag)
	t.Setenv("LEDGER_IMAGE_TAG", "v3.0.0-rc.2")
	desired := declared.DeepCopy()
	applyDefaults(desired)
	require.Equal(t, "v3.0.0-rc.2", desired.Spec.Image.Tag)
	got := requireReleasedHashWorkloadRollout(t, prior, desired, tlsModeDisabled)
	require.Equal(t, "ghcr.io/formancehq/ledger:v3.0.0-rc.2", got.Spec.Template.Spec.Containers[0].Image)
	require.Empty(t, declared.Spec.Image.Tag, "the CR continues to omit the image tag")
}

func TestStorageReleasedHashWorkloadChangesWithoutProof(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"configuration", "Pod annotation", "enable TLS", "disable TLS"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			prior := newMinimalCluster()
			prior.UID = "cluster"
			prior.Spec.Image.Tag = "v3.0.0-rc.1"
			oldTLSMode := tlsModeDisabled
			if change == "disable TLS" {
				prior.Spec.TLS = &ledgerv1alpha1.TLSConfig{Enabled: true, SecretName: "existing-certs", CASecretKey: "custom-ca.pem"}
				oldTLSMode = tlsModeRequired
			}
			applyDefaults(prior)
			desired := prior.DeepCopy()
			switch change {
			case "configuration":
				desired.Spec.HttpPort = 9001
			case "Pod annotation":
				desired.Spec.PodAnnotations = map[string]string{"rollout": "requested"}
			case "enable TLS":
				desired.Spec.TLS = &ledgerv1alpha1.TLSConfig{Enabled: true, SecretName: "existing-certs", CASecretKey: "custom-ca.pem"}
			case "disable TLS":
				desired.Spec.TLS = nil
			}
			got := requireReleasedHashWorkloadRollout(t, prior, desired, oldTLSMode)
			switch change {
			case "configuration":
				require.Contains(t, got.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "HTTP_PORT", Value: "9001"})
			case "Pod annotation":
				require.Equal(t, "requested", got.Spec.Template.Annotations["rollout"])
			default:
				requireTLSCertificates(t, got, tlsModeOptional)
			}
		})
	}
}

func TestStorageReleasedHashServiceAccountChangeStillRequiresProof(t *testing.T) {
	t.Parallel()
	for _, storageEdit := range []bool{false, true} {
		t.Run(fmt.Sprintf("storage=%t", storageEdit), func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			f.ledger.Spec.Persistence.Data.Size = resource.MustParse("100Gi")
			oldHash := computePodSpecHash(&f.ledger.Spec, false)
			f.source.CreationTimestamp = metav1.Now()
			f.source.Spec = buildStatefulSetSpec(f.ledger, oldHash, nil, tlsModeDisabled)
			require.NoError(t, f.client.Update(t.Context(), f.source))
			before := f.source.DeepCopy()
			f.ledger.Spec.ServiceAccount.Annotations = map[string]string{"eks.amazonaws.com/role-arn": "arn:aws:iam::111111111111:role/replacement"}
			if storageEdit {
				f.ledger.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
			}
			_, err := f.reconciler(f.client).reconcileStatefulSet(t.Context(), f.ledger, computeSpecHash(&f.ledger.Spec), nil)
			require.Error(t, err)
			condition := meta.FindStatusCondition(f.ledger.Status.Conditions, "ClaimTemplatesReady")
			require.NotNil(t, condition)
			require.Equal(t, "HashMigrationRequired", condition.Reason, "equal Pod templates cannot establish historical admission inputs")
			var got appsv1.StatefulSet
			require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &got))
			require.Equal(t, before, &got)
			f.requireStorageUnchanged(t)
		})
	}
}

func TestStorageLegacyHashAdmissionFailureIsVisible(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	ledger := newMinimalCluster()
	ledger.UID = "cluster"
	applyDefaults(ledger)
	prior := ledger.Spec.DeepCopy()
	prior.Persistence.Data.Size = resource.MustParse("150Gi")
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: resourceName(ledger.Name), Namespace: ledger.Namespace, UID: "source", CreationTimestamp: metav1.Now(),
	}, Spec: buildStatefulSetSpec(ledger, computePodSpecHash(prior, false), nil, "disabled")}
	installLegacySpecHashProof(t, ledger, sts, prior)
	require.NoError(t, controllerutil.SetControllerReference(ledger, sts, scheme))
	writes := 0
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts).WithInterceptorFuncs(interceptor.Funcs{
		Update: func(_ context.Context, _ client.WithWatch, _ client.Object, opts ...client.UpdateOption) error {
			options := &client.UpdateOptions{}
			options.ApplyOptions(opts)
			require.Equal(t, []string{metav1.DryRunAll}, options.DryRun)
			writes++

			return errors.New("webhook does not support dry run")
		},
	}).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	_, err := r.reconcileStatefulSetWithExec(t.Context(), ledger, computeSpecHash(&ledger.Spec), nil, nil)
	require.ErrorContains(t, err, "checking Pod template compatibility")
	require.Equal(t, 1, writes)
	condition := meta.FindStatusCondition(ledger.Status.Conditions, "ClaimTemplatesReady")
	require.NotNil(t, condition)
	require.Equal(t, "AdmissionCheckFailed", condition.Reason)
	var got appsv1.StatefulSet
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(sts), &got))
	require.Equal(t, sts.Spec.Template, got.Spec.Template)
}

func TestStoragePodTemplateAdmissionFailureIsVisible(t *testing.T) {
	t.Parallel()
	for _, explicitClass := range []bool{false, true} {
		name := "ready source start gate"
		if explicitClass {
			name = "modern hash compatibility"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			scheme := runtime.NewScheme()
			require.NoError(t, clientgoscheme.AddToScheme(scheme))
			require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
			ledger := newMinimalCluster()
			ledger.UID = "cluster"
			applyDefaults(ledger)
			hash := computeSpecHash(&ledger.Spec)
			sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
				Name: resourceName(ledger.Name), Namespace: ledger.Namespace, UID: "source", Generation: 1, CreationTimestamp: metav1.Now(),
				Annotations: map[string]string{annotationPodConfigHash: hash, annotationPodConfigSpecHash: hash},
			}, Spec: buildStatefulSetSpec(ledger, hash, nil, tlsModeDisabled), Status: appsv1.StatefulSetStatus{ObservedGeneration: 1, ReadyReplicas: 3, UpdatedReplicas: 3}}
			require.NoError(t, controllerutil.SetControllerReference(ledger, sts, scheme))
			ledger.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
			if explicitClass {
				ledger.Spec.Persistence.Data.StorageClass = "gp3"
			}
			admissionErr := errors.New("webhook unavailable")
			previews := 0
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts, ledger).WithInterceptorFuncs(interceptor.Funcs{
				Update: func(_ context.Context, _ client.WithWatch, _ client.Object, opts ...client.UpdateOption) error {
					var options client.UpdateOptions
					options.ApplyOptions(opts)
					require.Equal(t, []string{metav1.DryRunAll}, options.DryRun)
					previews++

					return admissionErr
				},
			}).Build()
			r := &ClusterReconciler{Client: c, Scheme: scheme}
			_, err := r.reconcileStatefulSet(t.Context(), ledger, computeSpecHash(&ledger.Spec), nil)
			require.ErrorIs(t, err, admissionErr)
			require.Equal(t, 1, previews)
			condition := meta.FindStatusCondition(ledger.Status.Conditions, "ClaimTemplatesReady")
			require.NotNil(t, condition, "report the failure on the original Cluster, not the effective TLS copy")
			require.Equal(t, metav1.ConditionFalse, condition.Status)
			require.Equal(t, "AdmissionCheckFailed", condition.Reason)
			require.Contains(t, condition.Message, admissionErr.Error())
			var got appsv1.StatefulSet
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(sts), &got))
			require.Equal(t, sts.Spec, got.Spec)
			require.True(t, got.DeletionTimestamp.IsZero())
			requireNoClaimTemplateRecovery(t, c, ledger)
		})
	}
}

func TestStorageTemplateComparisonIncludesRemovedWorkloadFields(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	ledger := newMinimalCluster()
	ledger.UID = "cluster"
	applyDefaults(ledger)
	hash := computeSpecHash(&ledger.Spec)
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: resourceName(ledger.Name), Namespace: ledger.Namespace, UID: "source", Generation: 1, CreationTimestamp: metav1.Now(),
		Annotations: map[string]string{annotationPodConfigHash: hash, annotationPodConfigSpecHash: hash},
	}, Spec: buildStatefulSetSpec(ledger, hash, nil, tlsModeDisabled), Status: appsv1.StatefulSetStatus{ObservedGeneration: 1, ReadyReplicas: 3, UpdatedReplicas: 3}}
	sts.Spec.Template.Spec.NodeSelector = map[string]string{"old-workload-setting": "remove"}
	require.NoError(t, controllerutil.SetControllerReference(ledger, sts, scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts, ledger).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	ledger.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
	_, err := r.reconcileStatefulSet(t.Context(), ledger, computeSpecHash(&ledger.Spec), nil)
	require.NoError(t, err)
	var got appsv1.StatefulSet
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(sts), &got))
	require.Empty(t, got.Spec.Template.Spec.NodeSelector)
	require.True(t, got.DeletionTimestamp.IsZero())
	requireNoClaimTemplateRecovery(t, c, ledger)
	condition := meta.FindStatusCondition(ledger.Status.Conditions, "ClaimTemplatesReady")
	require.NotNil(t, condition)
	require.Equal(t, "WaitingForRollout", condition.Reason)
}

func TestStorageDeclarationDriftPreservesInstalledPodHash(t *testing.T) {
	t.Parallel()
	for _, modern := range []bool{false, true} {
		name := "released hash"
		if modern {
			name = "modern metadata hash"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, field := range []string{"storageClass", "accessMode"} {
				t.Run(field, func(t *testing.T) {
					t.Parallel()
					scheme := runtime.NewScheme()
					require.NoError(t, clientgoscheme.AddToScheme(scheme))
					ledger := newMinimalCluster()
					applyDefaults(ledger)
					ledger.Spec.Persistence.Data.Size = resource.MustParse("100Gi")
					hash := computePodSpecHash(&ledger.Spec, modern)
					sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "source", Namespace: ledger.Namespace}, Spec: buildStatefulSetSpec(ledger, hash, nil, tlsModeDisabled)}
					if modern {
						sts.Annotations = map[string]string{annotationPodConfigHash: hash, annotationPodConfigSpecHash: hash}
					}
					if field == "storageClass" {
						ledger.Spec.Persistence.Data.StorageClass = "gp3"
					} else {
						ledger.Spec.Persistence.Data.AccessMode = string(corev1.ReadWriteMany)
					}
					desiredHash := computeSpecHash(&ledger.Spec)
					if modern {
						require.Equal(t, hash, desiredHash, "claim declarations are excluded from the canonical hash")
					} else {
						require.NotEqual(t, hash, desiredHash, "exercise the released full-spec hash")
					}
					desired := buildStatefulSetSpec(ledger, desiredHash, nil, tlsModeDisabled).Template
					previews := 0
					c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts).WithInterceptorFuncs(interceptor.Funcs{
						Update: func(callCtx context.Context, inner client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
							var options client.UpdateOptions
							options.ApplyOptions(opts)
							require.Equal(t, []string{metav1.DryRunAll}, options.DryRun)
							previews++

							return inner.Update(callCtx, obj, opts...)
						},
					}).Build()
					before := sts.DeepCopy()
					r := &ClusterReconciler{Client: c}
					resolved, err := r.resolveStorageSpecHash(t.Context(), ledger, sts, desired, desiredHash)
					require.NoError(t, err)
					require.Equal(t, 0, previews, "exact hash reconstruction proves nonstorage inputs unchanged")
					require.Equal(t, before.Spec.Template, resolved)
					require.Equal(t, before, sts, "hash resolution must not mutate its source")
				})
			}
		})
	}
}

func TestStorageAdmissionFailureReportsOnOriginalTLSCluster(t *testing.T) {
	t.Parallel()
	c, cluster := tlsDisableFixture(t)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cluster), cluster))
	cluster.Spec.TLS = nil
	require.NoError(t, c.Update(t.Context(), cluster))
	admissionErr := errors.New("TLS webhook does not support dry run")
	wrapped := interceptor.NewClient(c, interceptor.Funcs{
		Update: func(_ context.Context, _ client.WithWatch, _ client.Object, opts ...client.UpdateOption) error {
			var options client.UpdateOptions
			options.ApplyOptions(opts)
			require.Equal(t, []string{metav1.DryRunAll}, options.DryRun)

			return admissionErr
		},
	})
	var source appsv1.StatefulSet
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: cluster.Namespace, Name: resourceName(cluster.Name)}, &source))
	prior := cluster.Spec.DeepCopy()
	prior.Persistence.Data.Size = resource.MustParse("150Gi")
	oldHash := computePodSpecHash(prior, false)
	source.UID = "tls-source"
	delete(source.Annotations, annotationPodConfigHash)
	delete(source.Annotations, annotationPodConfigSpecHash)
	source.Spec.Template.Annotations[annotationSpecHash] = oldHash
	installLegacySpecHashProof(t, cluster, &source, prior)
	require.NoError(t, c.Update(t.Context(), &source))
	r := &ClusterReconciler{Client: wrapped, Scheme: c.Scheme()}
	_, err := r.reconcileStatefulSet(t.Context(), cluster, computeSpecHash(&cluster.Spec), nil)
	require.ErrorIs(t, err, admissionErr)
	require.Nil(t, cluster.Spec.TLS, "the desired spec remains separate from retained TLS configuration")
	condition := meta.FindStatusCondition(cluster.Status.Conditions, "ClaimTemplatesReady")
	require.NotNil(t, condition, "the outer Reconcile must be able to persist the condition on the original Cluster")
	require.Equal(t, "AdmissionCheckFailed", condition.Reason)
	require.Contains(t, condition.Message, admissionErr.Error())
	var sts appsv1.StatefulSet
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: cluster.Namespace, Name: resourceName(cluster.Name)}, &sts))
	require.Equal(t, oldHash, sts.Spec.Template.Annotations[annotationSpecHash])
	requireTLSCertificates(t, &sts, tlsModeRequired)
}

// ServiceAccount annotations affect Pod admission (for example, IRSA), even
// though they do not appear in the StatefulSet's PodTemplateSpec.
func TestStorageServiceAccountAnnotationsRollPods(t *testing.T) {
	t.Parallel()
	for _, source := range []string{"modern", "released", "released stale claims"} {
		for _, edit := range []string{"add", "replace", "remove"} {
			for _, storageEdit := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/storage=%t", source, edit, storageEdit), func(t *testing.T) {
					t.Parallel()
					scheme := runtime.NewScheme()
					require.NoError(t, clientgoscheme.AddToScheme(scheme))
					require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
					ledger := newMinimalCluster()
					ledger.UID = "cluster"
					applyDefaults(ledger)
					ledger.Spec.Persistence.Data.Size = resource.MustParse("100Gi")
					if edit != "add" {
						ledger.Spec.ServiceAccount.Annotations = map[string]string{"eks.amazonaws.com/role-arn": "arn:aws:iam::111111111111:role/previous"}
					}
					prior := ledger.Spec.DeepCopy()
					if source == "released stale claims" {
						prior.Persistence.Data.Size = resource.MustParse("150Gi")
					}
					hash := computeSpecHash(prior)
					if source != "modern" {
						hash = computePodSpecHash(prior, false)
					}
					sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
						Name: resourceName(ledger.Name), Namespace: ledger.Namespace, UID: "source", Generation: 1, CreationTimestamp: metav1.Now(),
					}, Spec: buildStatefulSetSpec(ledger, hash, nil, tlsModeDisabled), Status: appsv1.StatefulSetStatus{ObservedGeneration: 1, ReadyReplicas: 3, UpdatedReplicas: 3}}
					if source == "modern" {
						sts.Annotations = map[string]string{annotationPodConfigHash: hash, annotationPodConfigSpecHash: hash}
					}
					require.NoError(t, controllerutil.SetControllerReference(ledger, sts, scheme))
					c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts, ledger).Build()
					r := &ClusterReconciler{Client: c, Scheme: scheme}
					if source != "modern" {
						installLegacySpecHashProof(t, ledger, sts, prior)
					}
					ledger.Spec.ServiceAccount.Annotations = map[string]string{"eks.amazonaws.com/role-arn": "arn:aws:iam::111111111111:role/replacement"}
					if edit == "remove" {
						ledger.Spec.ServiceAccount.Annotations = nil
					}
					if storageEdit {
						ledger.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
					}
					desiredHash := computeSpecHash(&ledger.Spec)
					require.NotEqual(t, hash, desiredHash)
					_, err := r.reconcileStatefulSet(t.Context(), ledger, desiredHash, nil)
					require.NoError(t, err)
					var got appsv1.StatefulSet
					require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(sts), &got))
					require.Equal(t, desiredHash, got.Spec.Template.Annotations[annotationSpecHash], "the old hash must not hide a change to Pod admission inputs")
					require.Equal(t, sts.UID, got.UID)
					require.Equal(t, sts.Spec.VolumeClaimTemplates, got.Spec.VolumeClaimTemplates)
					requireNoClaimTemplateRecovery(t, c, ledger)
				})
			}
		}
	}
}

func TestStoragePreparationFailureFencesNormalWorkloadWrites(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"before journal", "intent rejected", "intent response lost", "journal rejected", "journal response lost", "pointer response lost", "fence unavailable"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			f.ledger.Spec.Persistence.DeletionProtection = new(false)
			hash := computeSpecHash(&f.ledger.Spec)
			f.source.CreationTimestamp = metav1.Now()
			f.source.Spec = buildStatefulSetSpec(f.ledger, hash, nil, tlsModeDisabled)
			recordPodConfigHash(f.source, hash)
			require.NoError(t, f.client.Update(t.Context(), f.source))
			original := f.source.DeepCopy()
			f.ledger.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
			f.ledger.Spec.Replicas = new(int32(5))
			f.ledger.Spec.Persistence.DeletionProtection = new(true)
			require.NoError(t, f.client.Update(t.Context(), f.ledger))
			injected := errors.New("preparation response lost")
			attempts, normalWrites := 0, 0
			preparationFailed := false
			wrapped := interceptor.NewClient(f.client, interceptor.Funcs{
				Create: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					var options client.CreateOptions
					options.ApplyOptions(opts)
					if _, ok := obj.(*appsv1.StatefulSet); ok && len(options.DryRun) != 0 && (failure == "before journal" || failure == "fence unavailable") {
						preparationFailed = true

						return injected
					}
					if _, ok := obj.(*corev1.ConfigMap); ok && strings.HasPrefix(obj.GetName(), resourceName(f.ledger.Name)+"-storage-recovery-") {
						attempts++
						if failure != "journal rejected" {
							if err := inner.Create(ctx, obj, opts...); err != nil {
								return err
							}
							if failure == "pointer response lost" {
								return nil
							}
						}

						return injected
					}

					return inner.Create(ctx, obj, opts...)
				},
				Update: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					options := &client.UpdateOptions{}
					options.ApplyOptions(opts)
					if _, ok := obj.(*appsv1.StatefulSet); ok && len(options.DryRun) == 0 {
						normalWrites++
					}

					return inner.Update(ctx, obj, opts...)
				},
				SubResourcePatch: func(ctx context.Context, inner client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
					pointer := obj.(*ledgerv1alpha1.Cluster).Status.ClaimTemplateRecovery
					if sub == "status" && pointer != nil && pointer.ConfigMapUID == "" && failure == "intent rejected" {
						return injected
					}
					err := inner.SubResource(sub).Patch(ctx, obj, patch, opts...)
					if err == nil && sub == "status" && pointer != nil && ((failure == "intent response lost" && pointer.ConfigMapUID == "") || (failure == "pointer response lost" && pointer.ConfigMapUID != "")) {
						return injected
					}

					return err
				},
				Get: func(ctx context.Context, inner client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if failure == "fence unavailable" && preparationFailed {
						return errors.New("API fence unavailable")
					}

					return inner.Get(ctx, key, obj, opts...)
				},
			})
			r := f.reconciler(wrapped)
			protectedObjects := make([]runtime.Object, 0, len(f.pvcs)+len(f.pvs))
			for _, pvc := range f.pvcs {
				protectedObjects = append(protectedObjects, pvc.DeepCopy())
			}
			for _, pv := range f.pvs {
				protectedObjects = append(protectedObjects, pv.DeepCopy())
			}
			r.Clientset = kubefake.NewClientset(protectedObjects...)
			_, err := r.reconcileStatefulSet(t.Context(), f.ledger, computeSpecHash(&f.ledger.Spec), nil)
			require.ErrorIs(t, err, injected)
			expectedAttempts := 1
			if failure == "before journal" || failure == "fence unavailable" || failure == "intent rejected" || failure == "intent response lost" {
				expectedAttempts = 0
			}
			require.Equal(t, expectedAttempts, attempts)
			var got appsv1.StatefulSet
			require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &got))
			if failure == "before journal" {
				require.EqualValues(t, 5, *got.Spec.Replicas, "a proven pre-journal failure must not block safe scale-up")
				require.Equal(t, 1, normalWrites)
				requireNoClaimTemplateRecovery(t, f.client, f.ledger)
			} else {
				require.Equal(t, 0, normalWrites, "a durable or uncertain operation fences mutable workload reconciliation")
				require.Equal(t, original.Spec, got.Spec)
				if failure == "journal rejected" {
					require.NotNil(t, f.latestLedger(t).Status.ClaimTemplateRecovery, "the failed journal attempt retains its durable intent")
					requireNoClaimTemplateRecoveryJournals(t, f.client, f.ledger)
				}
			}
			for _, pvc := range f.pvcs {
				claim, getErr := r.Clientset.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(t.Context(), pvc.Name, metav1.GetOptions{})
				require.NoError(t, getErr)
				pv, getErr := r.Clientset.CoreV1().PersistentVolumes().Get(t.Context(), pvc.Spec.VolumeName, metav1.GetOptions{})
				require.NoError(t, getErr)
				if failure == "before journal" {
					require.Equal(t, labelDeletionProtectionValue, claim.Labels[labelDeletionProtection])
					require.Equal(t, labelDeletionProtectionValue, pv.Labels[labelDeletionProtection])
				} else {
					require.Empty(t, claim.Labels[labelDeletionProtection], "normal protection must not run while recovery owns the workload")
					require.Empty(t, pv.Labels[labelDeletionProtection])
				}
			}
		})
	}
}

func TestStoragePreparationFenceUsesAPIReader(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"journal", "pointer", "read failure", "replaced Cluster", "missing APIReader"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			switch state {
			case "journal":
				require.NoError(t, f.client.Create(t.Context(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: resourceName(f.ledger.Name) + "-storage-recovery-inert-fixture", Namespace: f.ledger.Namespace}}))
			case "pointer":
				f.ledger.Status.ClaimTemplateRecovery = &ledgerv1alpha1.ClaimTemplateRecoveryStatus{ConfigMapName: resourceName(f.ledger.Name) + "-storage-recovery-inert-fixture", ConfigMapUID: "committed-journal"}
				require.NoError(t, f.client.Status().Update(t.Context(), f.ledger))
			case "replaced Cluster":
				latest := f.ledger.DeepCopy()
				latest.UID = "replacement-cluster"
				require.NoError(t, f.client.Update(t.Context(), latest))
			}
			// The stale manager cache knows neither the journal nor the pointer.
			stale := f.ledger.DeepCopy()
			stale.Status.ClaimTemplateRecovery = nil
			cached := fake.NewClientBuilder().WithScheme(f.scheme).WithObjects(stale).Build()
			r := &ClusterReconciler{Client: cached, APIReader: f.client, Scheme: f.scheme}
			if state == "missing APIReader" {
				r.APIReader = nil
			}
			if state == "read failure" {
				r.APIReader = interceptor.NewClient(f.client, interceptor.Funcs{Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
					return errors.New("API fence unavailable")
				}})
			}
			safe, err := r.claimTemplatePreparationUncommitted(t.Context(), f.ledger)
			if state == "journal" {
				require.True(t, safe, "standalone journals have no authority to block normal writes")
			} else {
				require.False(t, safe, "the stale cache must never authorize normal writes")
			}
			switch state {
			case "read failure":
				require.ErrorContains(t, err, "API fence unavailable")
			case "replaced Cluster":
				require.ErrorContains(t, err, "cluster identity changed")
			case "missing APIReader":
				require.ErrorContains(t, err, "uncached API reader is required")
			default:
				require.NoError(t, err)
			}
		})
	}
}

func TestStoragePreparationScheduledWaitContinuesOnlyFencedMutableWork(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"retry sooner than protection", "protection sooner than retry", "mutable write fails", "fence unavailable", "different Cluster UID", "pointer appeared"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			f := newClaimRecoveryFixture(t)
			f.ledger.Spec.Persistence.DeletionProtection = new(false)
			canonical := computeSpecHash(&f.ledger.Spec)
			f.source.CreationTimestamp = metav1.Now()
			f.source.Spec = buildStatefulSetSpec(f.ledger, canonical, nil, tlsModeDisabled)
			recordPodConfigHash(f.source, canonical)
			require.NoError(t, f.client.Update(t.Context(), f.source))
			f.ledger.Spec.Persistence.Data.Size = resource.MustParse("200Gi")
			f.ledger.Spec.Replicas = new(int32(5))
			f.ledger.Spec.Persistence.DeletionProtection = new(true)
			require.NoError(t, f.client.Update(t.Context(), f.ledger))
			denied := errors.New("journal quota rejected")
			mutableErr := errors.New("mutable StatefulSet update rejected")
			intentWrites, journalWrites, mutableWrites := 0, 0, 0
			waitingPass := false
			wrapped := interceptor.NewClient(f.client, interceptor.Funcs{
				Create: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if cm, ok := obj.(*corev1.ConfigMap); ok && strings.HasPrefix(cm.Name, resourceName(f.ledger.Name)+"-storage-recovery-") {
						journalWrites++

						return denied
					}

					return inner.Create(ctx, obj, opts...)
				},
				SubResourcePatch: func(ctx context.Context, inner client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
					intentWrites++

					return inner.SubResource(sub).Patch(ctx, obj, patch, opts...)
				},
				Update: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					options := (&client.UpdateOptions{}).ApplyOptions(opts)
					if _, ok := obj.(*appsv1.StatefulSet); ok && len(options.DryRun) == 0 {
						mutableWrites++
						if waitingPass && scenario == "mutable write fails" {
							return mutableErr
						}
					}

					return inner.Update(ctx, obj, opts...)
				},
			})
			r := f.reconciler(wrapped)
			objects := make([]runtime.Object, 0, len(f.pvcs)+len(f.pvs))
			for _, pvc := range f.pvcs {
				objects = append(objects, pvc.DeepCopy())
			}
			for _, pv := range f.pvs {
				objects = append(objects, pv.DeepCopy())
			}
			r.Clientset = kubefake.NewClientset(objects...)
			_, err := r.reconcileStatefulSet(t.Context(), f.ledger, canonical, nil)
			require.ErrorIs(t, err, denied)
			require.Equal(t, 1, journalWrites)
			require.Zero(t, mutableWrites, "an attempted durable write still fences its failing pass")
			handled, _, err := r.resumeClaimTemplateRecovery(t.Context(), f.latestLedger(t))
			require.True(t, handled)
			require.NoError(t, err)
			latest := f.latestLedger(t)
			conditions := append([]metav1.Condition(nil), latest.Status.Conditions...)
			beforeIntents := intentWrites
			waitWindow := 5 * time.Second
			if scenario == "protection sooner than retry" {
				waitWindow = 30 * time.Second
			}
			r.claimTemplateRetries.mu.Lock()
			key := client.ObjectKeyFromObject(latest)
			state, exists := r.claimTemplateRetries.entries[key]
			require.True(t, exists)
			state.retryAt = time.Now().Add(waitWindow)
			r.claimTemplateRetries.entries[key] = state
			r.claimTemplateRetries.mu.Unlock()
			fenceErr := errors.New("uncached Cluster read unavailable")
			if scenario == "fence unavailable" || scenario == "different Cluster UID" || scenario == "pointer appeared" {
				clusterReads := 0
				r.APIReader = interceptor.NewClient(f.client, interceptor.Funcs{
					Get: func(ctx context.Context, inner client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if err := inner.Get(ctx, key, obj, opts...); err != nil {
							return err
						}
						if cluster, ok := obj.(*ledgerv1alpha1.Cluster); ok {
							clusterReads++
							if clusterReads == 1 {
								return nil // Initial resume observes no operation; race the caller fence.
							}
							switch scenario {
							case "fence unavailable":
								return fenceErr
							case "different Cluster UID":
								cluster.UID = "replacement-cluster"
							case "pointer appeared":
								cluster.Status.ClaimTemplateRecovery = &ledgerv1alpha1.ClaimTemplateRecoveryStatus{SourceUID: "another-operation"}
							}
						}

						return nil
					},
				})
			}
			waitingPass = true
			result, err := r.reconcileStatefulSet(t.Context(), latest, canonical, nil)
			blocked := scenario == "fence unavailable" || scenario == "different Cluster UID" || scenario == "pointer appeared"
			switch {
			case scenario == "mutable write fails":
				require.ErrorIs(t, err, mutableErr, "a scheduled wait cannot hide a real mutable-work error")
			case blocked:
				require.Error(t, err)
				require.Zero(t, mutableWrites, "a rate hold cannot replace direct API authorization")
			default:
				require.NoError(t, err, "a scheduled hold is not a preparation failure")
				require.Positive(t, result.RequeueAfter)
				if scenario == "retry sooner than protection" {
					require.LessOrEqual(t, result.RequeueAfter, waitWindow)
				} else {
					require.Equal(t, volumeBindRequeueInterval, result.RequeueAfter)
				}
				var live appsv1.StatefulSet
				require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.source), &live))
				require.EqualValues(t, 5, *live.Spec.Replicas, "journal denial does not park safe scale-up")
				require.Equal(t, f.source.Spec.VolumeClaimTemplates, live.Spec.VolumeClaimTemplates)
				require.Equal(t, f.source.Spec.Template, live.Spec.Template)
				for _, pvc := range f.pvcs {
					claim, getErr := r.Clientset.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(t.Context(), pvc.Name, metav1.GetOptions{})
					require.NoError(t, getErr)
					pv, getErr := r.Clientset.CoreV1().PersistentVolumes().Get(t.Context(), pvc.Spec.VolumeName, metav1.GetOptions{})
					require.NoError(t, getErr)
					require.Equal(t, labelDeletionProtectionValue, claim.Labels[labelDeletionProtection])
					require.Equal(t, labelDeletionProtectionValue, pv.Labels[labelDeletionProtection])
				}
			}
			require.Equal(t, conditions, latest.Status.Conditions, "scheduled waits must not toggle Preparing or failure conditions")
			require.Equal(t, beforeIntents, intentWrites)
			require.Equal(t, 1, journalWrites)
			r.claimTemplateRetries.mu.Lock()
			after := r.claimTemplateRetries.entries[key]
			r.claimTemplateRetries.mu.Unlock()
			require.Equal(t, state, after, "holding cannot increment failures or slide the deadline")
			requireNoClaimTemplateRecovery(t, f.client, latest)
		})
	}
}
