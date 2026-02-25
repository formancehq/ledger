package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

// LedgerReconciler reconciles a Ledger object.
type LedgerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgers/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=traefik.io,resources=ingressroutes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=elbv2.k8s.aws,resources=targetgroupbindings,verbs=get;list;watch;create;update;patch;delete

// Reconcile handles the reconciliation loop for Ledger resources.
func (r *LedgerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the Ledger CR
	ledger := &ledgerv1alpha1.Ledger{}
	if err := r.Get(ctx, req.NamespacedName, ledger); err != nil {
		return ctrl.Result{}, ignoreNotFound(err)
	}

	// Apply defaults
	applyDefaults(ledger)

	// Validate spec — report errors via status condition instead of retrying.
	if err := validateSpec(ledger); err != nil {
		meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
			Type:               "ConfigValid",
			Status:             metav1.ConditionFalse,
			Reason:             "ValidationFailed",
			Message:            err.Error(),
			ObservedGeneration: ledger.Generation,
		})
		ledger.Status.Phase = "Degraded"
		_ = r.Status().Update(ctx, ledger)
		return ctrl.Result{}, nil // Don't requeue; wait for spec change.
	}

	// Clear any previous validation failure.
	meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
		Type:               "ConfigValid",
		Status:             metav1.ConditionTrue,
		Reason:             "Valid",
		ObservedGeneration: ledger.Generation,
	})

	// Compute spec hash for rolling updates
	specHash := computeSpecHash(&ledger.Spec)

	// Reconcile sub-resources in order
	reconcilers := []struct {
		name string
		fn   func(context.Context, *ledgerv1alpha1.Ledger) error
	}{
		{"ServiceAccount", r.reconcileServiceAccount},
		{"HeadlessService", r.reconcileHeadlessService},
		{"Service", r.reconcileService},
		{"GrpcService", r.reconcileGrpcService},
		{"Ingress", r.reconcileIngress},
		{"IngressGrpc", r.reconcileIngressGrpc},
		{"IngressRouteGrpc", r.reconcileIngressRouteGrpc},
		{"PDB", r.reconcilePDB},
		{"ServiceMonitor", r.reconcileServiceMonitor},
		{"TargetGroupBinding", r.reconcileTargetGroupBinding},
	}

	for _, rec := range reconcilers {
		if err := rec.fn(ctx, ledger); err != nil {
			logger.Error(err, "failed to reconcile", "resource", rec.name)
			return ctrl.Result{}, fmt.Errorf("reconciling %s: %w", rec.name, err)
		}
	}

	// StatefulSet needs the specHash
	if err := r.reconcileStatefulSet(ctx, ledger, specHash); err != nil {
		logger.Error(err, "failed to reconcile StatefulSet")
		return ctrl.Result{}, fmt.Errorf("reconciling StatefulSet: %w", err)
	}

	// Update status
	if err := r.updateStatus(ctx, ledger); err != nil {
		logger.Error(err, "failed to update status")
		return ctrl.Result{}, fmt.Errorf("updating status: %w", err)
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *LedgerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ledgerv1alpha1.Ledger{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&networkingv1.Ingress{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Complete(r)
}

func (r *LedgerReconciler) updateStatus(ctx context.Context, ledger *ledgerv1alpha1.Ledger) error {
	// Re-fetch the latest version to avoid conflict on status update.
	latest := &ledgerv1alpha1.Ledger{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      ledger.Name,
		Namespace: ledger.Namespace,
	}, latest); err != nil {
		return err
	}

	// Get the StatefulSet to read ready replicas
	sts := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      ledger.Name,
		Namespace: ledger.Namespace,
	}, sts)
	if err != nil {
		if apierrors.IsNotFound(err) {
			latest.Status.Phase = "Pending"
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
		case sts.Status.ReadyReplicas == replicas:
			latest.Status.Phase = "Running"
		case sts.Status.ReadyReplicas > 0:
			latest.Status.Phase = "Degraded"
		default:
			latest.Status.Phase = "Pending"
		}
	}

	latest.Status.ObservedGeneration = latest.Generation

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

// deleteIfExists deletes a resource if it exists, ignoring not-found errors.
func (r *LedgerReconciler) deleteIfExists(ctx context.Context, obj client.Object) error {
	err := r.Get(ctx, types.NamespacedName{
		Name:      obj.GetName(),
		Namespace: obj.GetNamespace(),
	}, obj)
	if err != nil {
		return ignoreNotFound(err)
	}
	return r.Delete(ctx, obj)
}

// ignoreNotFound returns nil on NotFound errors.
func ignoreNotFound(err error) error {
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// applyDefaults fills in zero-value fields with sensible defaults.
func applyDefaults(ledger *ledgerv1alpha1.Ledger) {
	if ledger.Spec.Image.Repository == "" {
		ledger.Spec.Image.Repository = "ghcr.io/formancehq/ledger-v3-poc"
	}
	if ledger.Spec.Image.Tag == "" {
		ledger.Spec.Image.Tag = "latest"
	}
	if ledger.Spec.Image.PullPolicy == "" {
		ledger.Spec.Image.PullPolicy = corev1.PullIfNotPresent
	}
	if ledger.Spec.Replicas == nil {
		replicas := int32(3)
		ledger.Spec.Replicas = &replicas
	}
	if ledger.Spec.Config.BindAddr == "" {
		ledger.Spec.Config.BindAddr = "0.0.0.0:7777"
	}
	if ledger.Spec.Config.GrpcPort == 0 {
		ledger.Spec.Config.GrpcPort = 8888
	}
	if ledger.Spec.Config.HttpPort == 0 {
		ledger.Spec.Config.HttpPort = 9000
	}
	if ledger.Spec.Config.WalDir == "" {
		ledger.Spec.Config.WalDir = "/data/raft"
	}
	if ledger.Spec.Config.DataDir == "" {
		ledger.Spec.Config.DataDir = "/data/app"
	}
	if ledger.Spec.Config.ClusterID == "" {
		ledger.Spec.Config.ClusterID = "default"
	}
	if ledger.Spec.Service.Type == "" {
		ledger.Spec.Service.Type = corev1.ServiceTypeClusterIP
	}

	applyIngressHostDefaults(ingressHosts(ledger.Spec.Ingress))
	applyIngressHostDefaults(ingressGrpcHosts(ledger.Spec.IngressGrpc))
}

func applyIngressHostDefaults(hosts []ledgerv1alpha1.IngressHost, enabled bool) {
	if !enabled {
		return
	}
	for i := range hosts {
		if len(hosts[i].Paths) == 0 {
			hosts[i].Paths = defaultIngressPaths()
		}
	}
}

func ingressHosts(spec *ledgerv1alpha1.IngressSpec) ([]ledgerv1alpha1.IngressHost, bool) {
	if spec == nil {
		return nil, false
	}
	return spec.Hosts, spec.Enabled
}

func ingressGrpcHosts(spec *ledgerv1alpha1.IngressGrpcSpec) ([]ledgerv1alpha1.IngressHost, bool) {
	if spec == nil {
		return nil, false
	}
	return spec.Hosts, spec.Enabled
}

func defaultIngressPaths() []ledgerv1alpha1.IngressPath {
	return []ledgerv1alpha1.IngressPath{{Path: "/", PathType: "Prefix"}}
}

// validateSpec checks the Ledger spec for configuration errors that would
// cause reconciliation to fail. Errors are surfaced via status conditions
// rather than silently failing in operator logs.
func validateSpec(ledger *ledgerv1alpha1.Ledger) error {
	if ledger.Spec.Replicas != nil && *ledger.Spec.Replicas%2 == 0 {
		return fmt.Errorf("replicas must be odd for Raft consensus, got %d", *ledger.Spec.Replicas)
	}
	if hosts, enabled := ingressHosts(ledger.Spec.Ingress); enabled {
		if err := validateIngressHosts("ingress", hosts); err != nil {
			return err
		}
	}
	if hosts, enabled := ingressGrpcHosts(ledger.Spec.IngressGrpc); enabled {
		if err := validateIngressHosts("ingressGrpc", hosts); err != nil {
			return err
		}
	}
	return nil
}

func validateIngressHosts(field string, hosts []ledgerv1alpha1.IngressHost) error {
	if len(hosts) == 0 {
		return fmt.Errorf("%s is enabled but has no hosts configured", field)
	}
	for i, h := range hosts {
		if h.Host == "" {
			return fmt.Errorf("%s.hosts[%d].host must not be empty", field, i)
		}
	}
	return nil
}
