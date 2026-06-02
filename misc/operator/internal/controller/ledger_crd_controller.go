package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

const (
	ledgerFinalizer       = "ledger.formance.com/finalizer"
	ledgerServiceGRPCPort = 8888
	ledgerContainer       = "ledger"
	ledgerExecTimeout     = 5 * time.Second
	ledgerRequeueDelay    = 15 * time.Second

	conditionEndpointResolved = "EndpointResolved"
	conditionLedgerSynced     = "LedgerSynced"
	conditionSpecDrifted      = "SpecDrifted"
)

var ledgerServiceGVR = schema.GroupVersionResource{
	Group:    "ledger.formance.com",
	Version:  "v1alpha1",
	Resource: "ledgerservices",
}

// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgers/finalizers,verbs=update
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgerservices,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create

// LedgerReconciler reconciles a Ledger object.
type LedgerReconciler struct {
	client.Client

	Scheme    *runtime.Scheme
	Dynamic   dynamic.Interface
	Config    *rest.Config
	Clientset kubernetes.Interface
}

func (r *LedgerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, retErr error) {
	log := ctrl.LoggerFrom(ctx)

	var ledger ledgerv1alpha1.Ledger
	if err := r.Get(ctx, req.NamespacedName, &ledger); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Handle deletion before setting up the deferred status update,
	// because deletion modifies metadata (finalizer removal), not status.
	if !ledger.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &ledger)
	}

	// Always persist status at the end, even on error.
	defer func() {
		ledger.Status.ObservedGeneration = ledger.Generation
		if statusErr := r.Status().Update(ctx, &ledger); statusErr != nil {
			log.Error(statusErr, "failed to update status")
			if retErr == nil {
				retErr = statusErr
			}
		}
	}()

	// Ensure finalizer.
	if !controllerutil.ContainsFinalizer(&ledger, ledgerFinalizer) {
		controllerutil.AddFinalizer(&ledger, ledgerFinalizer)
		if err := r.Update(ctx, &ledger); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{Requeue: true}, nil
	}

	// If already ready, check for spec drift only.
	if ledger.Status.Phase == ledgerv1alpha1.LedgerPhaseReady {
		return r.reconcileReady(ctx, &ledger)
	}

	// Resolve LedgerService endpoint.
	grpcPort, err := r.resolveEndpoint(ctx, &ledger)
	if err != nil {
		ledger.Status.Phase = ledgerv1alpha1.LedgerPhasePending
		ledger.Status.Message = fmt.Sprintf("waiting for LedgerService: %v", err)

		return ctrl.Result{RequeueAfter: ledgerRequeueDelay}, nil
	}

	// Create ledger via ledgerctl exec.
	pod0 := ledger.Spec.ServiceRef + "-0"
	args, err := r.buildCreateArgs(ctx, &ledger)
	if err != nil {
		meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
			Type:               conditionLedgerSynced,
			Status:             metav1.ConditionFalse,
			Reason:             "InvalidSpec",
			Message:            err.Error(),
			ObservedGeneration: ledger.Generation,
		})
		ledger.Status.Phase = ledgerv1alpha1.LedgerPhasePending
		ledger.Status.Message = fmt.Sprintf("invalid spec: %v", err)

		return ctrl.Result{RequeueAfter: ledgerRequeueDelay}, nil
	}

	execCtx, cancel := context.WithTimeout(ctx, ledgerExecTimeout)
	defer cancel()

	log.Info("creating ledger", "name", ledger.Spec.Name, "mode", ledger.Spec.Mode)
	if err := r.ledgerctlExec(execCtx, ledger.Namespace, pod0, grpcPort, args...); err != nil {
		if !isAlreadyExists(err) {
			meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
				Type:               conditionLedgerSynced,
				Status:             metav1.ConditionFalse,
				Reason:             "CreateFailed",
				Message:            err.Error(),
				ObservedGeneration: ledger.Generation,
			})
			ledger.Status.Phase = ledgerv1alpha1.LedgerPhasePending
			ledger.Status.Message = fmt.Sprintf("waiting: %v", err)

			return ctrl.Result{RequeueAfter: ledgerRequeueDelay}, nil
		}
	}

	// Success.
	mode := ledger.Spec.Mode
	if mode == "" {
		mode = "normal"
	}

	meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
		Type:               conditionLedgerSynced,
		Status:             metav1.ConditionTrue,
		Reason:             "Created",
		ObservedGeneration: ledger.Generation,
	})
	ledger.Status.Phase = ledgerv1alpha1.LedgerPhaseReady
	ledger.Status.Mode = mode
	ledger.Status.Message = ""
	ledger.Status.AppliedSpecHash = computeLedgerSpecHash(&ledger.Spec)

	log.Info("ledger ready", "name", ledger.Spec.Name, "mode", mode)

	return ctrl.Result{}, nil
}

// reconcileReady handles reconciliation for a Ledger that is already in Ready phase.
// It detects spec drift (ledgers are immutable) and handles mirror→normal promotion.
func (r *LedgerReconciler) reconcileReady(ctx context.Context, ledger *ledgerv1alpha1.Ledger) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	// Handle promotion: mirror → normal.
	if ledger.Status.Mode == "mirror" && ledger.Spec.Mode == "normal" {
		return r.reconcilePromotion(ctx, ledger)
	}

	// Detect spec drift.
	currentHash := computeLedgerSpecHash(&ledger.Spec)
	if ledger.Status.AppliedSpecHash != "" && currentHash != ledger.Status.AppliedSpecHash {
		meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
			Type:               conditionSpecDrifted,
			Status:             metav1.ConditionTrue,
			Reason:             "LedgerImmutable",
			Message:            "Ledger spec was modified but ledgers are immutable after creation. Delete and recreate to apply changes.",
			ObservedGeneration: ledger.Generation,
		})
		log.Info("spec drift detected on immutable ledger", "name", ledger.Spec.Name)
	} else {
		meta.RemoveStatusCondition(&ledger.Status.Conditions, conditionSpecDrifted)
	}

	return ctrl.Result{}, nil
}

// reconcilePromotion handles mirror→normal promotion.
func (r *LedgerReconciler) reconcilePromotion(ctx context.Context, ledger *ledgerv1alpha1.Ledger) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	grpcPort, err := r.resolveEndpoint(ctx, ledger)
	if err != nil {
		ledger.Status.Message = fmt.Sprintf("waiting for LedgerService for promotion: %v", err)

		return ctrl.Result{RequeueAfter: ledgerRequeueDelay}, nil
	}

	pod0 := ledger.Spec.ServiceRef + "-0"

	execCtx, cancel := context.WithTimeout(ctx, ledgerExecTimeout)
	defer cancel()

	log.Info("promoting mirror ledger to normal", "name", ledger.Spec.Name)
	if err := r.ledgerctlExec(execCtx, ledger.Namespace, pod0, grpcPort,
		"ledgers", "promote", ledger.Spec.Name, "--yes"); err != nil {
		meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
			Type:               conditionLedgerSynced,
			Status:             metav1.ConditionFalse,
			Reason:             "PromotionFailed",
			Message:            err.Error(),
			ObservedGeneration: ledger.Generation,
		})
		ledger.Status.Message = fmt.Sprintf("promotion failed: %v", err)

		return ctrl.Result{RequeueAfter: ledgerRequeueDelay}, nil
	}

	meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
		Type:               conditionLedgerSynced,
		Status:             metav1.ConditionTrue,
		Reason:             "Promoted",
		ObservedGeneration: ledger.Generation,
	})
	ledger.Status.Mode = "normal"
	ledger.Status.Message = ""
	ledger.Status.AppliedSpecHash = computeLedgerSpecHash(&ledger.Spec)

	log.Info("ledger promoted", "name", ledger.Spec.Name)

	return ctrl.Result{}, nil
}

// reconcileDelete handles ledger deletion with best-effort cleanup.
func (r *LedgerReconciler) reconcileDelete(ctx context.Context, ledger *ledgerv1alpha1.Ledger) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	if !controllerutil.ContainsFinalizer(ledger, ledgerFinalizer) {
		return ctrl.Result{}, nil
	}

	grpcPort, err := r.resolveGRPCPort(ctx, ledger.Namespace, ledger.Spec.ServiceRef)
	if err != nil {
		log.Error(err, "failed to resolve gRPC endpoint for deletion, continuing cleanup")
	} else {
		pod0 := ledger.Spec.ServiceRef + "-0"

		execCtx, cancel := context.WithTimeout(ctx, ledgerExecTimeout)
		defer cancel()

		log.Info("deleting ledger", "name", ledger.Spec.Name)
		if err := r.ledgerctlExec(execCtx, ledger.Namespace, pod0, grpcPort,
			"ledgers", "delete", ledger.Spec.Name, "--yes"); err != nil {
			if !isLedgerNotFound(err) {
				log.Error(err, "failed to delete ledger (best-effort)")
			}
		}
	}

	controllerutil.RemoveFinalizer(ledger, ledgerFinalizer)
	if err := r.Update(ctx, ledger); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// resolveEndpoint resolves the gRPC port and verifies the LedgerService is Running.
// Sets the EndpointResolved condition accordingly.
func (r *LedgerReconciler) resolveEndpoint(ctx context.Context, ledger *ledgerv1alpha1.Ledger) (int32, error) {
	ls, err := r.Dynamic.Resource(ledgerServiceGVR).Namespace(ledger.Namespace).Get(
		ctx, ledger.Spec.ServiceRef, metav1.GetOptions{})
	if err != nil {
		meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
			Type:               conditionEndpointResolved,
			Status:             metav1.ConditionFalse,
			Reason:             "LedgerServiceNotFound",
			Message:            fmt.Sprintf("LedgerService %q not found: %v", ledger.Spec.ServiceRef, err),
			ObservedGeneration: ledger.Generation,
		})

		return 0, fmt.Errorf("get LedgerService %q: %w", ledger.Spec.ServiceRef, err)
	}

	phase, _, _ := nestedFieldNoCopy(ls.Object, "status", "phase")
	if phaseStr, ok := phase.(string); !ok || phaseStr != "Running" {
		meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
			Type:               conditionEndpointResolved,
			Status:             metav1.ConditionFalse,
			Reason:             "LedgerServiceNotReady",
			Message:            fmt.Sprintf("LedgerService %q is not Running (phase: %v)", ledger.Spec.ServiceRef, phase),
			ObservedGeneration: ledger.Generation,
		})

		return 0, fmt.Errorf("LedgerService %q is not Running", ledger.Spec.ServiceRef)
	}

	port, found, err := unstructuredNestedInt64(ls.Object, "spec", "config", "grpcPort")
	if err != nil || !found || port == 0 {
		port = ledgerServiceGRPCPort
	}

	meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
		Type:               conditionEndpointResolved,
		Status:             metav1.ConditionTrue,
		Reason:             "Resolved",
		ObservedGeneration: ledger.Generation,
	})

	return int32(port), nil
}

// buildCreateArgs constructs the ledgerctl ledgers create arguments.
func (r *LedgerReconciler) buildCreateArgs(ctx context.Context, ledger *ledgerv1alpha1.Ledger) ([]string, error) {
	args := []string{"ledgers", "create", "--name", ledger.Spec.Name}

	mode := ledger.Spec.Mode
	if mode == "" {
		mode = "normal"
	}

	if mode == "mirror" {
		args = append(args, "--mode", "mirror")

		src := ledger.Spec.MirrorSource
		if src == nil {
			return nil, errors.New("mirrorSource is required when mode is mirror")
		}

		if src.LedgerName != "" {
			args = append(args, "--mirror-ledger-name", src.LedgerName)
		}

		if src.BatchSize != nil {
			args = append(args, "--mirror-batch-size", strconv.Itoa(int(*src.BatchSize)))
		}

		switch {
		case src.HTTP != nil:
			args = append(args, "--mirror-source-type", "http")
			args = append(args, "--mirror-base-url", src.HTTP.BaseURL)

			if src.HTTP.OAuth2 != nil {
				oauth2 := src.HTTP.OAuth2
				args = append(args, "--mirror-oauth2-client-id", oauth2.ClientID)
				args = append(args, "--mirror-oauth2-token-endpoint", oauth2.TokenEndpoint)

				secret, err := r.readSecretKey(ctx, ledger.Namespace,
					oauth2.ClientSecretFrom.Name, oauth2.ClientSecretFrom.Key)
				if err != nil {
					return nil, fmt.Errorf("reading OAuth2 client secret: %w", err)
				}

				args = append(args, "--mirror-oauth2-client-secret", secret)

				for _, scope := range oauth2.Scopes {
					args = append(args, "--mirror-oauth2-scopes", scope)
				}
			}

		case src.Postgres != nil:
			args = append(args, "--mirror-source-type", "postgres")

			dsn, err := r.readSecretKey(ctx, ledger.Namespace,
				src.Postgres.DSNFrom.Name, src.Postgres.DSNFrom.Key)
			if err != nil {
				return nil, fmt.Errorf("reading Postgres DSN secret: %w", err)
			}

			args = append(args, "--mirror-dsn", dsn)

		default:
			return nil, errors.New("mirrorSource must specify either http or postgres")
		}
	}

	return args, nil
}

// readSecretKey reads a single key from a Kubernetes Secret.
func (r *LedgerReconciler) readSecretKey(ctx context.Context, namespace, name, key string) (string, error) {
	secret, err := r.Clientset.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get secret %s/%s: %w", namespace, name, err)
	}

	data, ok := secret.Data[key]
	if !ok {
		return "", fmt.Errorf("key %q not found in secret %s/%s", key, namespace, name)
	}

	return string(data), nil
}

// ledgerctlExec runs a ledgerctl command inside pod-0 of the LedgerService StatefulSet.
func (r *LedgerReconciler) ledgerctlExec(ctx context.Context, namespace, pod string, grpcPort int32, args ...string) error {
	cmd := ledgerctlCommand(grpcPort, args...)

	_, err := podExec(ctx, r.Config, r.Clientset, namespace, pod, ledgerContainer, cmd)
	if err != nil {
		return fmt.Errorf("ledgerctl %s: %w", args[0], err)
	}

	return nil
}

// resolveGRPCPort reads the gRPC port from the LedgerService spec (defaults to 8888).
func (r *LedgerReconciler) resolveGRPCPort(ctx context.Context, namespace, serviceRef string) (int32, error) {
	ls, err := r.Dynamic.Resource(ledgerServiceGVR).Namespace(namespace).Get(ctx, serviceRef, metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("get LedgerService %q: %w", serviceRef, err)
	}

	port, found, err := unstructuredNestedInt64(ls.Object, "spec", "config", "grpcPort")
	if err != nil || !found || port == 0 {
		return ledgerServiceGRPCPort, nil
	}

	return int32(port), nil
}

func (r *LedgerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ledgerv1alpha1.Ledger{}).
		Named("ledger-crd").
		Complete(r)
}

// computeLedgerSpecHash returns a SHA-256 hash of the Ledger spec.
func computeLedgerSpecHash(spec *ledgerv1alpha1.LedgerCRDSpec) string {
	data, _ := json.Marshal(spec) //nolint:errchkjson // spec is always serializable

	return fmt.Sprintf("%x", sha256.Sum256(data))
}

// isAlreadyExists checks if the error output indicates the ledger already exists.
func isAlreadyExists(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "already exists")
}

// isLedgerNotFound checks if the error output indicates the ledger was not found.
func isLedgerNotFound(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "not found")
}

// unstructuredNestedInt64 extracts an int64 from an unstructured object.
func unstructuredNestedInt64(obj map[string]any, fields ...string) (int64, bool, error) {
	val, found, err := nestedFieldNoCopy(obj, fields...)
	if !found || err != nil {
		return 0, found, err
	}

	switch v := val.(type) {
	case int64:
		return v, true, nil
	case float64:
		return int64(v), true, nil
	default:
		return 0, false, fmt.Errorf("unexpected type %T for %v", val, fields)
	}
}

// nestedFieldNoCopy traverses a map by the given path.
func nestedFieldNoCopy(obj map[string]any, fields ...string) (any, bool, error) {
	var val any = obj
	for _, field := range fields {
		m, ok := val.(map[string]any)
		if !ok {
			return nil, false, nil
		}

		val, ok = m[field]
		if !ok {
			return nil, false, nil
		}
	}

	return val, true, nil
}
