package controller

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

const (
	ledgerFinalizer       = "ledger.formance.com/finalizer"
	bucketServiceApply    = "/ledger.BucketService/Apply"
	ledgerServiceGRPCPort = 8888
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

// LedgerReconciler reconciles a Ledger object.
type LedgerReconciler struct {
	client.Client

	Scheme  *runtime.Scheme
	Dynamic dynamic.Interface
}

func (r *LedgerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	var ledger ledgerv1alpha1.Ledger
	if err := r.Get(ctx, req.NamespacedName, &ledger); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Handle deletion.
	if !ledger.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(&ledger, ledgerFinalizer) {
			return ctrl.Result{}, nil
		}

		// Resolve gRPC endpoint and delete ledger (best-effort).
		endpoint, err := r.resolveGRPCEndpoint(ctx, ledger.Namespace, ledger.Spec.ServiceRef)
		if err != nil {
			log.Error(err, "failed to resolve gRPC endpoint for deletion, continuing cleanup")
		} else {
			log.Info("deleting ledger", "name", ledger.Spec.Name, "endpoint", endpoint)
			if delErr := deleteLedger(ctx, endpoint, ledger.Spec.Name); delErr != nil {
				log.Error(delErr, "failed to delete ledger (best-effort)")
			}
		}

		controllerutil.RemoveFinalizer(&ledger, ledgerFinalizer)
		if err := r.Update(ctx, &ledger); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{}, nil
	}

	// Ensure finalizer.
	if !controllerutil.ContainsFinalizer(&ledger, ledgerFinalizer) {
		controllerutil.AddFinalizer(&ledger, ledgerFinalizer)
		if err := r.Update(ctx, &ledger); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{Requeue: true}, nil
	}

	// If already ready, nothing to do.
	if ledger.Status.Phase == ledgerv1alpha1.LedgerPhaseReady {
		return ctrl.Result{}, nil
	}

	// Resolve gRPC endpoint.
	endpoint, err := r.resolveGRPCEndpoint(ctx, ledger.Namespace, ledger.Spec.ServiceRef)
	if err != nil {
		return r.setLedgerFailed(ctx, &ledger, fmt.Sprintf("failed to resolve gRPC endpoint: %v", err))
	}

	// Create ledger via gRPC.
	log.Info("creating ledger", "name", ledger.Spec.Name, "endpoint", endpoint)
	if err := createLedger(ctx, endpoint, ledger.Spec.Name); err != nil {
		return r.setLedgerFailed(ctx, &ledger, fmt.Sprintf("failed to create ledger: %v", err))
	}

	ledger.Status.Phase = ledgerv1alpha1.LedgerPhaseReady
	ledger.Status.Message = ""
	if err := r.Status().Update(ctx, &ledger); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("ledger ready", "name", ledger.Spec.Name)

	return ctrl.Result{}, nil
}

func (r *LedgerReconciler) resolveGRPCEndpoint(ctx context.Context, namespace, serviceRef string) (string, error) {
	ls, err := r.Dynamic.Resource(ledgerServiceGVR).Namespace(namespace).Get(ctx, serviceRef, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get LedgerService %q: %w", serviceRef, err)
	}

	// Use the LedgerService's internal DNS name.
	_ = ls // We only need the name to construct the endpoint.

	return fmt.Sprintf("%s.%s.svc.cluster.local:%d", serviceRef, namespace, ledgerServiceGRPCPort), nil
}

func (r *LedgerReconciler) setLedgerFailed(ctx context.Context, ledger *ledgerv1alpha1.Ledger, message string) (ctrl.Result, error) {
	ledger.Status.Phase = ledgerv1alpha1.LedgerPhaseFailed
	ledger.Status.Message = message
	if err := r.Status().Update(ctx, ledger); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *LedgerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ledgerv1alpha1.Ledger{}).
		Named("ledger-crd").
		Complete(r)
}

// gRPC ledger management via raw protobuf encoding.

func createLedger(ctx context.Context, endpoint, name string) error {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", endpoint, err)
	}
	defer conn.Close() //nolint:errcheck // best-effort

	req := encodeApplyRequest(encodeCreateLedgerRequest(name))
	var resp []byte

	if err := conn.Invoke(ctx, bucketServiceApply, &req, &resp, grpc.ForceCodec(rawCodec{})); err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.AlreadyExists {
			return nil
		}

		return fmt.Errorf("create ledger %q: %w", name, err)
	}

	return nil
}

func deleteLedger(ctx context.Context, endpoint, name string) error {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", endpoint, err)
	}
	defer conn.Close() //nolint:errcheck // best-effort

	req := encodeApplyRequest(encodeDeleteLedgerRequest(name))
	var resp []byte

	if err := conn.Invoke(ctx, bucketServiceApply, &req, &resp, grpc.ForceCodec(rawCodec{})); err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			return nil
		}

		return fmt.Errorf("delete ledger %q: %w", name, err)
	}

	return nil
}

// rawCodec passes raw byte slices without marshaling, used with grpc.ForceCodec.
type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.(*[]byte)
	if !ok {
		return nil, fmt.Errorf("rawCodec: expected *[]byte, got %T", v)
	}

	return *b, nil
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("rawCodec: expected *[]byte, got %T", v)
	}

	*b = data

	return nil
}

func (rawCodec) Name() string { return "proto" }

// Protobuf encoding helpers for BucketService/Apply messages.
// Proto schema (from misc/proto/bucket.proto):
//
//	ApplyRequest  { repeated Request requests = 1; }
//	Request       { oneof action { CreateLedgerRequest create_ledger = 3; DeleteLedgerRequest delete_ledger = 4; } }
//	CreateLedgerRequest { string name = 1; }
//	DeleteLedgerRequest { string name = 1; }
func encodeCreateLedgerRequest(name string) []byte {
	// CreateLedgerRequest: field 1 = name (string)
	var inner []byte
	inner = protowire.AppendTag(inner, 1, protowire.BytesType)
	inner = protowire.AppendString(inner, name)

	// Request: field 3 = create_ledger (CreateLedgerRequest)
	var req []byte
	req = protowire.AppendTag(req, 3, protowire.BytesType)
	req = protowire.AppendBytes(req, inner)

	return req
}

func encodeDeleteLedgerRequest(name string) []byte {
	// DeleteLedgerRequest: field 1 = name (string)
	var inner []byte
	inner = protowire.AppendTag(inner, 1, protowire.BytesType)
	inner = protowire.AppendString(inner, name)

	// Request: field 4 = delete_ledger (DeleteLedgerRequest)
	var req []byte
	req = protowire.AppendTag(req, 4, protowire.BytesType)
	req = protowire.AppendBytes(req, inner)

	return req
}

func encodeApplyRequest(requests ...[]byte) []byte {
	// ApplyRequest: field 1 = requests (repeated Request)
	var msg []byte
	for _, r := range requests {
		msg = protowire.AppendTag(msg, 1, protowire.BytesType)
		msg = protowire.AppendBytes(msg, r)
	}

	return msg
}
