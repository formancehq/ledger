package controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

// ClusterReconciler reconciles a Cluster object.
type ClusterReconciler struct {
	client.Client

	Scheme    *runtime.Scheme
	Config    *rest.Config
	Clientset kubernetes.Interface
	Recorder  record.EventRecorder
}

// +kubebuilder:rbac:groups=ledger.formance.com,resources=clusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ledger.formance.com,resources=clusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ledger.formance.com,resources=clusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=ledger.formance.com,resources=credentials,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=delete;get;list;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=externaldns.k8s.io,resources=dnsendpoints,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=validatingadmissionpolicybindings,verbs=get;list;watch

// Reconcile handles the reconciliation loop for Cluster resources.
func (r *ClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the Cluster CR
	ledger := &ledgerv1alpha1.Cluster{}
	if err := r.Get(ctx, req.NamespacedName, ledger); err != nil {
		return ctrl.Result{}, ignoreNotFound(err)
	}

	// Handle deletion — owned resources are garbage-collected via owner references.
	if !ledger.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Clear the persisted Phase before stepping through reconcile so a
	// previously parked "Error" does not survive once the user fixes the
	// underlying issue. Steps that detect an error (validateSpec, drift
	// guard) re-set Phase=Error before returning; updateStatus recomputes
	// from StatefulSet readiness when Phase stays empty.
	ledger.Status.Phase = ""

	// Apply hardcoded defaults (fills remaining zero-value fields).
	applyDefaults(ledger)

	// Resolve endpoints early so they are always set in status, even if
	// later reconciliation steps fail (e.g. StatefulSet scale-down).
	ledger.Status.Endpoints = resolveEndpoints(ledger)

	// Always persist status (endpoints, conditions) at the end, even on error.
	defer func() {
		if statusErr := r.updateStatus(ctx, ledger); statusErr != nil {
			logger.Error(statusErr, "failed to update status")
		}
	}()

	// Validate spec — report errors via status condition instead of retrying.
	if err := validateSpec(ledger); err != nil {
		meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
			Type:               "ConfigValid",
			Status:             metav1.ConditionFalse,
			Reason:             "ValidationFailed",
			Message:            err.Error(),
			ObservedGeneration: ledger.Generation,
		})
		ledger.Status.Phase = "Error"

		return ctrl.Result{}, nil // Don't requeue; wait for spec change.
	}

	// Clear any previous validation failure.
	meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
		Type:               "ConfigValid",
		Status:             metav1.ConditionTrue,
		Reason:             "Valid",
		ObservedGeneration: ledger.Generation,
	})

	// Prune optional Services whose spec marks them disabled BEFORE running
	// the drift guard: an edit that both disables a Service and tweaks
	// additionalLabels would otherwise abort on drift detected on the
	// primary Service / StatefulSet, leaving the optional Service stranded.
	if err := r.pruneDisabledOptionalServices(ctx, ledger); err != nil {
		return ctrl.Result{}, fmt.Errorf("pruning disabled optional services: %w", err)
	}

	// Reject spec.additionalLabels changes that would mutate the immutable
	// selector of an existing Service / StatefulSet. The reconcile is skipped
	// (no requeue) and the user gets a SelectorImmutable=false condition
	// pointing at the offending objects.
	if err := r.validateSelectorImmutability(ctx, ledger); err != nil {
		if errors.Is(err, errSelectorDrift) {
			meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
				Type:               "SelectorImmutable",
				Status:             metav1.ConditionFalse,
				Reason:             "SelectorDrift",
				Message:            err.Error(),
				ObservedGeneration: ledger.Generation,
			})
			ledger.Status.Phase = "Error"

			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, fmt.Errorf("validating selector immutability: %w", err)
	}
	meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
		Type:               "SelectorImmutable",
		Status:             metav1.ConditionTrue,
		Reason:             "Stable",
		ObservedGeneration: ledger.Generation,
	})

	// Warn when hostPath volumes are used without node scheduling constraints.
	if hasHostPathVolume(ledger) && len(ledger.Spec.NodeSelector) == 0 && ledger.Spec.Affinity == nil {
		meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
			Type:               "HostPathSchedulingWarning",
			Status:             metav1.ConditionTrue,
			Reason:             "NoNodeSelector",
			Message:            "hostPath volumes are configured but no nodeSelector or affinity is set; pods may be scheduled on nodes without the expected mount path",
			ObservedGeneration: ledger.Generation,
		})
	} else {
		meta.RemoveStatusCondition(&ledger.Status.Conditions, "HostPathSchedulingWarning")
	}

	// Warn when a ledger opts into deletion protection but no cluster-scoped
	// protection policy is installed. The operator still stamps the label, but
	// with no policy selecting it the volumes are not actually protected — surface
	// that instead of silently no-op'ing. We detect "installed" by probing the
	// actual ValidatingAdmissionPolicyBinding rather than this release's own Helm
	// flag: in a multi-release setup a sibling release (pvcProtection.enabled=false)
	// still stamps the label and the owning release's cluster-wide binding protects
	// it, so keying off the local flag would falsely report unprotected.
	if r.Clientset != nil {
		inactive := false
		if ledger.Spec.Persistence.DeletionProtectionEnabled() {
			installed, err := r.deletionProtectionPolicyInstalled(ctx)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("checking volume deletion-protection policy: %w", err)
			}
			inactive = !installed
		}

		if inactive {
			meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
				Type:               "DeletionProtectionInactive",
				Status:             metav1.ConditionTrue,
				Reason:             "ClusterPolicyNotInstalled",
				Message:            "spec.persistence.deletionProtection is set but no cluster-scoped volume protection policy is installed (Helm pvcProtection.enabled on no release); volumes are NOT protected",
				ObservedGeneration: ledger.Generation,
			})
			if r.Recorder != nil {
				r.Recorder.Event(ledger, corev1.EventTypeWarning, "DeletionProtectionInactive",
					"deletion protection requested but no cluster policy is installed; volumes are NOT protected")
			}
		} else {
			meta.RemoveStatusCondition(&ledger.Status.Conditions, "DeletionProtectionInactive")
		}
	}

	// Compute spec hash for rolling updates
	specHash := computeSpecHash(&ledger.Spec)

	// Reconcile sub-resources in order
	reconcilers := []struct {
		name string
		fn   func(context.Context, *ledgerv1alpha1.Cluster) error
	}{
		{"ServiceAccount", r.reconcileServiceAccount},
		{"HeadlessService", r.reconcileHeadlessService},
		{"Service", r.reconcileService},
		{"GrpcService", r.reconcileGrpcService},
		{"Ingress", r.reconcileIngress},
		{"IngressGrpc", r.reconcileIngressGrpc},
		{"DNSEndpoint", r.reconcileDNSEndpoint},
		{"NetworkPolicy", r.reconcileNetworkPolicy},
	}

	for _, rec := range reconcilers {
		if err := rec.fn(ctx, ledger); err != nil {
			logger.Error(err, "failed to reconcile", "resource", rec.name)

			return ctrl.Result{}, fmt.Errorf("reconciling %s: %w", rec.name, err)
		}
	}

	// Reconcile auth keys from Credentials (before StatefulSet).
	agents, err := r.reconcileAuthKeys(ctx, ledger)
	if err != nil {
		logger.Error(err, "failed to reconcile auth keys")

		return ctrl.Result{}, fmt.Errorf("reconciling AuthKeys: %w", err)
	}

	// Reconcile cluster secret only when TLS will be at least partially
	// active during this pass. The secret is a static bearer token; it must
	// never travel in plaintext. The state machine ensures the secret
	// appears at the same time the StatefulSet moves to optional during a
	// TLS toggle, and disappears symmetrically when TLS is turned off.
	existingSTSForTLS, err := r.fetchExistingStatefulSet(ctx, ledger)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("fetching StatefulSet for TLS state: %w", err)
	}
	targetTLSForSecret := computeTargetTLSMode(
		desiredTLSMode(ledger),
		currentTLSModeFromStatefulSet(existingSTSForTLS),
		rolloutConverged(existingSTSForTLS),
	)

	if shouldInjectClusterSecret(targetTLSForSecret) {
		if err := r.reconcileClusterSecret(ctx, ledger); err != nil {
			logger.Error(err, "failed to reconcile cluster secret")

			return ctrl.Result{}, fmt.Errorf("reconciling ClusterSecret: %w", err)
		}
	} else {
		if err := r.deleteClusterSecret(ctx, ledger); err != nil {
			logger.Error(err, "failed to delete cluster secret")

			return ctrl.Result{}, fmt.Errorf("deleting ClusterSecret: %w", err)
		}
	}

	// StatefulSet needs the specHash and agent info
	result, err := r.reconcileStatefulSet(ctx, ledger, specHash, agents)
	if err != nil {
		logger.Error(err, "failed to reconcile StatefulSet")

		return ctrl.Result{}, fmt.Errorf("reconciling StatefulSet: %w", err)
	}

	return result, nil
}

// deletionProtectionPolicyInstalled reports whether the cluster-scoped volume
// deletion-protection policy is installed, by probing for the fixed-name PVC
// ValidatingAdmissionPolicyBinding. This reflects actual cluster state regardless
// of which operator release owns the singleton, so it stays correct in the
// documented multi-release setup where a sibling release (pvcProtection.enabled=false)
// relies on the owning release's binding to protect its volumes.
func (r *ClusterReconciler) deletionProtectionPolicyInstalled(ctx context.Context) (bool, error) {
	_, err := r.Clientset.AdmissionregistrationV1().
		ValidatingAdmissionPolicyBindings().
		Get(ctx, volumeProtectionPVCBindingName, metav1.GetOptions{})
	// Not installed (binding absent) — or the cluster predates ValidatingAdmissionPolicy
	// (GA in Kubernetes >= 1.30): the resource type is unregistered there, so the API
	// server answers the GET with a 404, which IsNotFound also matches (unknown reason +
	// code 404). Either way the policy is not acting on any volume, so report not-installed
	// rather than erroring the reconcile — deletionProtection now defaults on, so this
	// probe runs on every reconcile.
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("getting ValidatingAdmissionPolicyBinding %s: %w", volumeProtectionPVCBindingName, err)
	}

	return true, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ledgerv1alpha1.Cluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&networkingv1.Ingress{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Watches(&ledgerv1alpha1.Credentials{}, handler.EnqueueRequestsFromMapFunc(r.credentialsToClusters)).
		Complete(r)
}

func (r *ClusterReconciler) updateStatus(ctx context.Context, ledger *ledgerv1alpha1.Cluster) error {
	// Re-fetch the latest version to avoid conflict on status update.
	latest := &ledgerv1alpha1.Cluster{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      ledger.Name,
		Namespace: ledger.Namespace,
	}, latest); err != nil {
		return err
	}

	// ledger was fetched fresh at the top of Reconcile and this reconciler is the
	// sole writer of Cluster status conditions, so ledger.Status.Conditions is
	// the authoritative desired set after this pass — including conditions removed
	// during reconcile (e.g. DeletionProtectionInactive once the cluster policy is
	// installed or protection is disabled). Assign it onto the freshly-fetched latest
	// so those removals are persisted; an additive SetStatusCondition merge would
	// leave stale conditions in .status.conditions forever.
	latest.Status.Conditions = ledger.Status.Conditions

	// Preserve the phase set during reconciliation (e.g. "Degraded" from
	// validation failure) before we try to recompute from StatefulSet state.
	reconciledPhase := ledger.Status.Phase

	// Get the StatefulSet to read ready replicas
	sts := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      resourceName(ledger.Name),
		Namespace: ledger.Namespace,
	}, sts)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// If the reconciler already set a phase (e.g. "Degraded" from
			// validation), keep it; otherwise default to "Pending".
			if reconciledPhase != "" {
				latest.Status.Phase = reconciledPhase
			} else {
				latest.Status.Phase = "Pending"
			}
			latest.Status.ReadyReplicas = 0
		} else {
			return err
		}
	} else {
		latest.Status.ReadyReplicas = sts.Status.ReadyReplicas

		replicas := int32(3)
		if latest.Spec.Replicas != nil {
			replicas = *latest.Spec.Replicas
		}

		switch {
		// An "Error" phase is set when reconciliation hit a hard stop
		// (invalid spec, selector drift). Keep it visible — recomputing
		// from StatefulSet readiness would mask the blocked state for
		// users and automation that key off .status.phase.
		case reconciledPhase == "Error":
			latest.Status.Phase = "Error"
		case sts.Status.ReadyReplicas == replicas:
			latest.Status.Phase = "Running"
		case sts.Status.ReadyReplicas > 0:
			latest.Status.Phase = "Degraded"
		default:
			latest.Status.Phase = "Pending"
		}
	}

	latest.Status.ObservedGeneration = latest.Generation
	latest.Status.Endpoints = ledger.Status.Endpoints
	latest.Status.TLSMigrationPhase = ledger.Status.TLSMigrationPhase

	// Set condition
	condition := metav1.Condition{
		Type:               "Ready",
		ObservedGeneration: latest.Generation,
		LastTransitionTime: metav1.Now(),
	}
	if latest.Status.Phase == "Running" {
		condition.Status = metav1.ConditionTrue
		condition.Reason = "AllReplicasReady"
		condition.Message = "All replicas are ready"
	} else {
		condition.Status = metav1.ConditionFalse
		condition.Reason = "ReplicasNotReady"
		condition.Message = fmt.Sprintf("%d/%d replicas ready", latest.Status.ReadyReplicas, *latest.Spec.Replicas)
	}
	meta.SetStatusCondition(&latest.Status.Conditions, condition)

	return r.Status().Update(ctx, latest)
}

// resolveEndpoints computes the external or internal endpoints for a Cluster.
func resolveEndpoints(ledger *ledgerv1alpha1.Cluster) *ledgerv1alpha1.EndpointsStatus {
	// gRPC endpoint
	var grpcHost string
	if ledger.Spec.IngressGrpc != nil && ledger.Spec.IngressGrpc.Enabled && len(ledger.Spec.IngressGrpc.Hosts) > 0 {
		grpcHost = ledger.Spec.IngressGrpc.Hosts[0].Host
	}

	// HTTP endpoint
	var httpHost string
	var httpTLS bool
	if ledger.Spec.Ingress != nil && ledger.Spec.Ingress.Enabled && len(ledger.Spec.Ingress.Hosts) > 0 {
		httpHost = ledger.Spec.Ingress.Hosts[0].Host
		httpTLS = len(ledger.Spec.Ingress.TLS) > 0
	}

	external := grpcHost != "" || httpHost != ""

	grpcEndpoint := fmt.Sprintf("%s.%s.svc.cluster.local:8888", resourceName(ledger.Name), ledger.Namespace)
	if grpcHost != "" {
		grpcEndpoint = grpcHost + ":443"
	}

	httpEndpoint := fmt.Sprintf("http://%s.%s.svc.cluster.local:9000", resourceName(ledger.Name), ledger.Namespace)
	if httpHost != "" {
		scheme := "http"
		if httpTLS {
			scheme = "https"
		}
		httpEndpoint = fmt.Sprintf("%s://%s", scheme, httpHost)
	}

	return &ledgerv1alpha1.EndpointsStatus{
		GRPC:     grpcEndpoint,
		HTTP:     httpEndpoint,
		External: external,
	}
}

// deleteIfExists deletes a resource if it exists, ignoring not-found errors.
func (r *ClusterReconciler) deleteIfExists(ctx context.Context, obj client.Object) error {
	err := r.Get(ctx, types.NamespacedName{
		Name:      obj.GetName(),
		Namespace: obj.GetNamespace(),
	}, obj)
	if err != nil {
		return ignoreNotFound(err)
	}

	return r.Delete(ctx, obj)
}

func (r *ClusterReconciler) deleteUnstructuredIfExists(ctx context.Context, obj *unstructured.Unstructured) error {
	err := r.Get(ctx, types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}, obj)
	if err != nil {
		return ignoreNotFound(err)
	}

	return r.Delete(ctx, obj)
}

// ignoreNotFound returns nil on NotFound and NoKindMatch errors.
// NoKindMatch occurs when optional CRDs (DNSEndpoint)
// are not installed in the cluster.
func ignoreNotFound(err error) error {
	if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
		return nil
	}

	return err
}

// applyDefaults fills in zero-value fields with sensible defaults.
func applyDefaults(ledger *ledgerv1alpha1.Cluster) {
	if ledger.Spec.Image.Repository == "" {
		if repo := os.Getenv("LEDGER_IMAGE_REPOSITORY"); repo != "" {
			ledger.Spec.Image.Repository = repo
		} else {
			ledger.Spec.Image.Repository = "ghcr.io/formancehq/ledger"
		}
	}
	if ledger.Spec.Image.Tag == "" {
		if tag := os.Getenv("LEDGER_IMAGE_TAG"); tag != "" {
			ledger.Spec.Image.Tag = tag
		} else {
			ledger.Spec.Image.Tag = "latest"
		}
	}
	if ledger.Spec.Image.PullPolicy == "" {
		ledger.Spec.Image.PullPolicy = corev1.PullIfNotPresent
	}
	if ledger.Spec.Replicas == nil {
		replicas := int32(3)
		ledger.Spec.Replicas = &replicas
	}
	if ledger.Spec.BindAddr == "" {
		ledger.Spec.BindAddr = "0.0.0.0:7777"
	}
	if ledger.Spec.GrpcPort == 0 {
		ledger.Spec.GrpcPort = 8888
	}
	if ledger.Spec.HttpPort == 0 {
		ledger.Spec.HttpPort = 9000
	}
	if ledger.Spec.WalDir == "" {
		ledger.Spec.WalDir = "/data/raft"
	}
	if ledger.Spec.DataDir == "" {
		ledger.Spec.DataDir = "/data/app"
	}
	if ledger.Spec.ClusterID == "" {
		ledger.Spec.ClusterID = "default"
	}
	if ledger.Spec.ServiceAccount.Create == nil {
		create := true
		ledger.Spec.ServiceAccount.Create = &create
	}
	if ledger.Spec.Service.Type == "" {
		ledger.Spec.Service.Type = corev1.ServiceTypeClusterIP
	}
}

// validateSpec checks the Cluster spec for configuration errors that would
// cause reconciliation to fail. Errors are surfaced via status conditions
// rather than silently failing in operator logs.
func validateSpec(ledger *ledgerv1alpha1.Cluster) error {
	// The headless Service name is the tightest DNS-1035 label the operator
	// derives from the CR name (resourcePrefix + name + "-headless"), capped at
	// 63 chars. Reject over-long names at admission with a clear message instead
	// of letting reconciliation fail later on an invalid Service name (EN-1319).
	if derived := headlessServiceName(ledger.Name); len(derived) > dns1035LabelMaxLength {
		return fmt.Errorf(
			"name %q is too long: derived Service name %q is %d chars, exceeds the %d-char DNS-1035 limit",
			ledger.Name, derived, len(derived), dns1035LabelMaxLength,
		)
	}

	if ledger.Spec.Replicas != nil && *ledger.Spec.Replicas%2 == 0 {
		return fmt.Errorf("replicas must be odd for Raft consensus, got %d", *ledger.Spec.Replicas)
	}

	if err := validateClusterConfig(&ledger.Spec); err != nil {
		return err
	}

	// Validate hostPath / PVC mutual exclusion for each volume.
	volumes := []struct {
		name string
		spec *ledgerv1alpha1.VolumeSpec
	}{
		{"persistence.wal", &ledger.Spec.Persistence.WAL},
		{"persistence.data", &ledger.Spec.Persistence.Data},
		{"persistence.coldCache", &ledger.Spec.Persistence.ColdCache},
	}
	for _, v := range volumes {
		if err := validateVolumeSpec(v.name, v.spec); err != nil {
			return err
		}
	}

	return nil
}

// validateClusterConfig validates cluster-wide cache and bloom parameters that
// flow to the server via env vars. Defaults are server-side (loadBloomConfig);
// this only rejects values that would either be silently ignored or that the
// server would refuse.
func validateClusterConfig(spec *ledgerv1alpha1.ClusterSpec) error {
	if spec.Cache != nil && spec.Cache.RotationThreshold != nil && *spec.Cache.RotationThreshold <= 0 {
		return fmt.Errorf("cache.rotationThreshold must be > 0, got %d", *spec.Cache.RotationThreshold)
	}
	if spec.Bloom == nil {
		return nil
	}
	bloomFilters := []struct {
		name string
		spec *ledgerv1alpha1.BloomFilterConfig
	}{
		{"volumes", spec.Bloom.Volumes},
		{"metadata", spec.Bloom.Metadata},
		{"references", spec.Bloom.References},
		{"ledgers", spec.Bloom.Ledgers},
		{"boundaries", spec.Bloom.Boundaries},
		{"transactions", spec.Bloom.Transactions},
		{"sinkConfigs", spec.Bloom.SinkConfigs},
		{"numscriptVersions", spec.Bloom.NumscriptVersions},
		{"numscriptContents", spec.Bloom.NumscriptContents},
		{"ledgerMetadata", spec.Bloom.LedgerMetadata},
		{"preparedQueries", spec.Bloom.PreparedQueries},
	}
	for _, b := range bloomFilters {
		if b.spec == nil {
			continue
		}
		if b.spec.ExpectedKeys != nil && *b.spec.ExpectedKeys < 0 {
			return fmt.Errorf("bloom.%s.expectedKeys must be >= 0, got %d (0 disables)", b.name, *b.spec.ExpectedKeys)
		}
		if b.spec.FPRate == "" {
			continue
		}
		f, err := strconv.ParseFloat(b.spec.FPRate, 64)
		if err != nil {
			return fmt.Errorf("bloom.%s.fpRate %q is not a valid float: %w", b.name, b.spec.FPRate, err)
		}
		if f <= 0 || f >= 1 {
			return fmt.Errorf("bloom.%s.fpRate must be in (0,1), got %v", b.name, f)
		}
	}

	return nil
}

// validateVolumeSpec checks that hostPath and PVC fields are mutually exclusive.
func validateVolumeSpec(field string, spec *ledgerv1alpha1.VolumeSpec) error {
	if spec.HostPath == nil {
		return nil
	}
	if spec.HostPath.Path == "" {
		return fmt.Errorf("%s.hostPath.path must not be empty", field)
	}
	if spec.StorageClass != "" {
		return fmt.Errorf("%s: storageClass and hostPath are mutually exclusive", field)
	}
	if spec.VolumeAttributesClassName != "" {
		return fmt.Errorf("%s: volumeAttributesClassName and hostPath are mutually exclusive", field)
	}

	return nil
}

// hasHostPathVolume returns true if any volume uses hostPath.
func hasHostPathVolume(ledger *ledgerv1alpha1.Cluster) bool {
	return ledger.Spec.Persistence.WAL.HostPath != nil ||
		ledger.Spec.Persistence.Data.HostPath != nil ||
		ledger.Spec.Persistence.ColdCache.HostPath != nil
}
