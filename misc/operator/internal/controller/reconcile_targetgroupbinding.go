package controller

import (
	"context"
	"encoding/json"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

func (r *LedgerReconciler) reconcileTargetGroupBinding(ctx context.Context, ledger *ledgerv1alpha1.Ledger) error {
	name := ledger.Name + "-grpc"
	gvk := schema.GroupVersionKind{
		Group:   "elbv2.k8s.aws",
		Version: "v1beta1",
		Kind:    "TargetGroupBinding",
	}

	enabled := ledger.Spec.IngressGrpc != nil &&
		ledger.Spec.IngressGrpc.Enabled &&
		ledger.Spec.IngressGrpc.TargetGroupBinding != nil &&
		ledger.Spec.IngressGrpc.TargetGroupBinding.Enabled

	if !enabled {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(gvk)
		obj.SetName(name)
		obj.SetNamespace(ledger.Namespace)
		return r.deleteUnstructuredIfExists(ctx, obj)
	}

	tgb := ledger.Spec.IngressGrpc.TargetGroupBinding

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetName(name)
	obj.SetNamespace(ledger.Namespace)

	// Fetch existing to merge
	_ = r.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: ledger.Namespace}, obj) //nolint:errcheck // ignore not-found

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
		obj.SetLabels(commonLabels(ledger))

		grpcSvcName := ledger.Name + "-grpc"
		grpcPort := serviceGrpcPort(ledger)

		// Preserve existing spec to avoid overwriting immutable fields
		// (ipAddressType, vpcID) set by the AWS Load Balancer Controller webhook.
		existingSpec, _, _ := unstructured.NestedMap(obj.Object, "spec") //nolint:errcheck
		if existingSpec == nil {
			existingSpec = map[string]interface{}{}
		}

		existingSpec["serviceRef"] = map[string]interface{}{
			"name": grpcSvcName,
			"port": int64(grpcPort),
		}
		existingSpec["targetGroupARN"] = tgb.TargetGroupARN

		if tgb.TargetType != "" {
			existingSpec["targetType"] = tgb.TargetType
		}

		if tgb.Networking != nil && tgb.Networking.Raw != nil {
			var networking interface{}
			if err := json.Unmarshal(tgb.Networking.Raw, &networking); err == nil {
				existingSpec["networking"] = networking
			}
		}

		if err := unstructured.SetNestedField(obj.Object, existingSpec, "spec"); err != nil {
			return err
		}
		return controllerutil.SetControllerReference(ledger, obj, r.Scheme)
	})
	return err
}
