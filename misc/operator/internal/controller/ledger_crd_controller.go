package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
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
	ledgerFinalizer    = "ledger.formance.com/finalizer"
	clusterGRPCPort    = 8888
	ledgerContainer    = "ledger"
	ledgerExecTimeout  = 5 * time.Second
	ledgerRequeueDelay = 15 * time.Second

	conditionEndpointResolved = "EndpointResolved"
	conditionLedgerSynced     = "LedgerSynced"
	conditionSpecDrifted      = "SpecDrifted"
	conditionIndexesSynced    = "IndexesSynced"
)

var clusterGVR = schema.GroupVersionResource{
	Group:    "ledger.formance.com",
	Version:  "v1alpha1",
	Resource: "clusters",
}

// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ledger.formance.com,resources=ledgers/finalizers,verbs=update
// +kubebuilder:rbac:groups=ledger.formance.com,resources=clusters,verbs=get;list;watch
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

	// Resolve Cluster endpoint.
	grpcPort, err := r.resolveEndpoint(ctx, &ledger)
	if err != nil {
		ledger.Status.Phase = ledgerv1alpha1.LedgerPhasePending
		ledger.Status.Message = fmt.Sprintf("waiting for Cluster: %v", err)

		return ctrl.Result{RequeueAfter: ledgerRequeueDelay}, nil
	}

	// Create ledger via ledgerctl exec.
	pod0 := podName(ledger.Spec.ServiceRef, 0)
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
	if err := r.ledgerctlExec(execCtx, ledger.Namespace, ledger.Spec.ServiceRef, pod0, grpcPort, args...); err != nil {
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

	// Reconcile declarative indexes now that the ledger exists.
	return r.handleIndexReconcile(ctx, &ledger, grpcPort, ctrl.Result{}), nil
}

// reconcileReady handles reconciliation for a Ledger that is already in Ready phase.
// It detects spec drift (ledgers are immutable) and handles mirror→normal promotion.
func (r *LedgerReconciler) reconcileReady(ctx context.Context, ledger *ledgerv1alpha1.Ledger) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	// Handle promotion: mirror → normal.
	if ledger.Status.Mode == "mirror" && ledger.Spec.Mode == "normal" {
		return r.reconcilePromotion(ctx, ledger)
	}

	// Detect spec drift. The hash excludes indexes, so a mismatch means an
	// immutable field (name/serviceRef/mode/mirrorSource) changed.
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

		// Do NOT run index reconciliation while an immutable field has drifted:
		// spec.name / spec.serviceRef may now point at a different ledger or
		// cluster, so creating/dropping indexes here would mutate the wrong
		// target. Hold until the drift is resolved by delete + recreate.
		return ctrl.Result{}, nil
	}

	meta.RemoveStatusCondition(&ledger.Status.Conditions, conditionSpecDrifted)

	// Reconcile declarative indexes (mutable, independent of ledger immutability).
	if ledger.Spec.Indexes == nil {
		meta.RemoveStatusCondition(&ledger.Status.Conditions, conditionIndexesSynced)

		return ctrl.Result{}, nil
	}

	grpcPort, err := r.resolveEndpoint(ctx, ledger)
	if err != nil {
		// Index reconciliation did not run for this generation. Mark
		// IndexesSynced=False so a prior True is not left stale (consumers such
		// as `kubectl wait` / Chainsaw must not see the new desired set as
		// synced while no create/drop actually ran).
		meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
			Type:               conditionIndexesSynced,
			Status:             metav1.ConditionFalse,
			Reason:             "EndpointUnavailable",
			Message:            fmt.Sprintf("waiting for Cluster before reconciling indexes: %v", err),
			ObservedGeneration: ledger.Generation,
		})
		ledger.Status.Message = fmt.Sprintf("waiting for Cluster for index reconcile: %v", err)

		return ctrl.Result{RequeueAfter: ledgerRequeueDelay}, nil
	}

	return r.handleIndexReconcile(ctx, ledger, grpcPort, ctrl.Result{}), nil
}

// reconcilePromotion handles mirror→normal promotion.
func (r *LedgerReconciler) reconcilePromotion(ctx context.Context, ledger *ledgerv1alpha1.Ledger) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	grpcPort, err := r.resolveEndpoint(ctx, ledger)
	if err != nil {
		ledger.Status.Message = fmt.Sprintf("waiting for Cluster for promotion: %v", err)

		return ctrl.Result{RequeueAfter: ledgerRequeueDelay}, nil
	}

	pod0 := podName(ledger.Spec.ServiceRef, 0)

	execCtx, cancel := context.WithTimeout(ctx, ledgerExecTimeout)
	defer cancel()

	log.Info("promoting mirror ledger to normal", "name", ledger.Spec.Name)
	if err := r.ledgerctlExec(execCtx, ledger.Namespace, ledger.Spec.ServiceRef, pod0, grpcPort,
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

	// Requeue so the next pass (now Ready + normal) reconciles declarative
	// indexes for the promoted ledger.
	return ctrl.Result{Requeue: true}, nil
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
		pod0 := podName(ledger.Spec.ServiceRef, 0)

		execCtx, cancel := context.WithTimeout(ctx, ledgerExecTimeout)
		defer cancel()

		log.Info("deleting ledger", "name", ledger.Spec.Name)
		if err := r.ledgerctlExec(execCtx, ledger.Namespace, ledger.Spec.ServiceRef, pod0, grpcPort,
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

// resolveEndpoint resolves the gRPC port and verifies the Cluster is Running.
// Sets the EndpointResolved condition accordingly.
func (r *LedgerReconciler) resolveEndpoint(ctx context.Context, ledger *ledgerv1alpha1.Ledger) (int32, error) {
	ls, err := r.Dynamic.Resource(clusterGVR).Namespace(ledger.Namespace).Get(
		ctx, ledger.Spec.ServiceRef, metav1.GetOptions{})
	if err != nil {
		meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
			Type:               conditionEndpointResolved,
			Status:             metav1.ConditionFalse,
			Reason:             "ClusterNotFound",
			Message:            fmt.Sprintf("Cluster %q not found: %v", ledger.Spec.ServiceRef, err),
			ObservedGeneration: ledger.Generation,
		})

		return 0, fmt.Errorf("get Cluster %q: %w", ledger.Spec.ServiceRef, err)
	}

	phase, _, _ := nestedFieldNoCopy(ls.Object, "status", "phase")
	if phaseStr, ok := phase.(string); !ok || phaseStr != "Running" {
		meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
			Type:               conditionEndpointResolved,
			Status:             metav1.ConditionFalse,
			Reason:             "ClusterNotReady",
			Message:            fmt.Sprintf("Cluster %q is not Running (phase: %v)", ledger.Spec.ServiceRef, phase),
			ObservedGeneration: ledger.Generation,
		})

		return 0, fmt.Errorf("cluster %q is not Running", ledger.Spec.ServiceRef)
	}

	port, found, err := unstructuredNestedInt64(ls.Object, "spec", "config", "grpcPort")
	if err != nil || !found || port == 0 {
		port = clusterGRPCPort
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

		// CEL rewrite rules apply to the shared v2 translator, so they are
		// independent of the source transport (HTTP or Postgres). Each rule is
		// passed as a repeatable JSON object via --mirror-rewrite-rule (ledgerctl
		// runs over k8s exec with an arg list, so a JSON arg composes where a file
		// path would not).
		for _, rule := range src.RewriteRules {
			encoded, err := json.Marshal(rule)
			if err != nil {
				return nil, fmt.Errorf("encoding rewrite rule: %w", err)
			}

			args = append(args, "--mirror-rewrite-rule", string(encoded))
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
			pgArgs, err := r.buildPostgresMirrorArgs(ctx, ledger.Namespace, src.Postgres)
			if err != nil {
				return nil, err
			}

			args = append(args, pgArgs...)

		default:
			return nil, errors.New("mirrorSource must specify either http or postgres")
		}
	}

	return args, nil
}

// buildPostgresMirrorArgs assembles ledgerctl flags for a Postgres mirror
// source: explicit Host/Port/User/Database/SSLMode fields are joined into a
// DSN, then either the static password (looked up from a Secret) is inlined,
// or AWS RDS IAM auth is signalled via --mirror-aws-iam-region (and the DSN
// stays passwordless; the ledger pod mints SigV4 tokens per connection).
// Exactly one of PasswordFrom or AWSIAMAuth must be set.
func (r *LedgerReconciler) buildPostgresMirrorArgs(ctx context.Context, namespace string, pg *ledgerv1alpha1.PostgresMirrorSource) ([]string, error) {
	if pg.PasswordFrom == nil && pg.AWSIAMAuth == nil {
		return nil, errors.New("mirrorSource.postgres: one of passwordFrom or awsIamAuth must be set")
	}

	if pg.PasswordFrom != nil && pg.AWSIAMAuth != nil {
		return nil, errors.New("mirrorSource.postgres: passwordFrom and awsIamAuth are mutually exclusive")
	}

	port := int32(5432)
	if pg.Port != 0 {
		port = pg.Port
	}

	sslmode := pg.SSLMode
	if sslmode == "" {
		sslmode = "require"
	}

	userInfo := url.User(pg.User)

	if pg.PasswordFrom != nil {
		password, err := r.readSecretKey(ctx, namespace, pg.PasswordFrom.Name, pg.PasswordFrom.Key)
		if err != nil {
			return nil, fmt.Errorf("reading Postgres password secret: %w", err)
		}

		userInfo = url.UserPassword(pg.User, password)
	}

	dsn := url.URL{
		Scheme:   "postgres",
		User:     userInfo,
		Host:     fmt.Sprintf("%s:%d", pg.Host, port),
		Path:     "/" + pg.Database,
		RawQuery: url.Values{"sslmode": []string{sslmode}}.Encode(),
	}

	args := []string{
		"--mirror-source-type", "postgres",
		"--mirror-dsn", dsn.String(),
	}

	if pg.AWSIAMAuth != nil {
		args = append(args, "--mirror-aws-iam-region", pg.AWSIAMAuth.Region)

		if pg.AWSIAMAuth.AssumeRoleArn != "" {
			args = append(args, "--mirror-aws-iam-assume-role-arn", pg.AWSIAMAuth.AssumeRoleArn)
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

// ledgerctlExec runs a ledgerctl command inside pod-0 of the Cluster
// StatefulSet. It resolves the TLS_MODE from the StatefulSet env so that
// ledgerctl negotiates the same transport (plaintext or TLS) as the running
// gRPC server expects — a mismatch surfaces as "error reading server preface".
// The server address dialed is the pod's own headless DNS so the SNI matches
// the server certificate's SANs.
func (r *LedgerReconciler) ledgerctlExec(ctx context.Context, namespace, serviceName, pod string, grpcPort int32, args ...string) error {
	_, err := r.ledgerctlExecOutput(ctx, namespace, serviceName, pod, grpcPort, args...)

	return err
}

// ledgerctlExecOutput runs a ledgerctl command like ledgerctlExec but returns
// its captured stdout. When the command is invoked with --json, pterm output
// (spinners, messages) is routed to stderr by the CLI, so stdout carries only
// the JSON payload — safe to parse.
func (r *LedgerReconciler) ledgerctlExecOutput(ctx context.Context, namespace, serviceName, pod string, grpcPort int32, args ...string) (string, error) {
	tlsMode, err := fetchTLSMode(ctx, r.Client, namespace, resourceName(serviceName))
	if err != nil {
		return "", fmt.Errorf("resolving TLS mode for Cluster %q: %w", serviceName, err)
	}

	serverAddr := podSelfServerAddr(headlessServiceName(serviceName), grpcPort)
	cmd := ledgerctlCommand(serverAddr, tlsMode, args...)

	res, err := podExec(ctx, r.Config, r.Clientset, namespace, pod, ledgerContainer, cmd)
	if err != nil {
		return "", fmt.Errorf("ledgerctl %s: %w", args[0], err)
	}

	return res.Stdout, nil
}

// resolveGRPCPort reads the gRPC port from the Cluster spec (defaults to 8888).
func (r *LedgerReconciler) resolveGRPCPort(ctx context.Context, namespace, serviceRef string) (int32, error) {
	ls, err := r.Dynamic.Resource(clusterGVR).Namespace(namespace).Get(ctx, serviceRef, metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("get Cluster %q: %w", serviceRef, err)
	}

	port, found, err := unstructuredNestedInt64(ls.Object, "spec", "config", "grpcPort")
	if err != nil || !found || port == 0 {
		return clusterGRPCPort, nil
	}

	return int32(port), nil
}

func (r *LedgerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ledgerv1alpha1.Ledger{}).
		Named("ledger-crd").
		Complete(r)
}

// computeLedgerSpecHash returns a SHA-256 hash of the Ledger spec used to
// detect drift on immutable ledgers. The indexes field is excluded because it
// is mutable and reconciled continuously — an index-only edit must not trip the
// SpecDrifted condition.
func computeLedgerSpecHash(spec *ledgerv1alpha1.LedgerCRDSpec) string {
	forHash := *spec
	forHash.Indexes = nil
	data, _ := json.Marshal(&forHash) //nolint:errchkjson // spec is always serializable

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
