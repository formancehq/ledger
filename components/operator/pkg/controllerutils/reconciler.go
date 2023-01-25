package controllerutils

import (
	"context"
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"

	apisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	"github.com/go-logr/logr"
	pkgError "github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type Mutator[T apisv1beta2.Object] interface {
	SetupWithBuilder(mgr ctrl.Manager, builder *ctrl.Builder) error
	Mutate(ctx context.Context, t T) (*ctrl.Result, error)
}

// Reconciler reconciles a Stack object
type Reconciler[T apisv1beta2.Object] struct {
	client.Client
	Scheme  *runtime.Scheme
	Mutator Mutator[T]
}

func (r *Reconciler[T]) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {

	log.FromContext(ctx).Info("Starting reconciliation")
	defer func() {
		log.FromContext(ctx).Info("Reconciliation terminated")
	}()

	var t T
	t = reflect.New(reflect.TypeOf(t).Elem()).Interface().(T)
	if err := r.Get(ctx, req.NamespacedName, t); err != nil {
		if errors.IsNotFound(err) {
			log.FromContext(ctx).Info("Object not found, skip")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, pkgError.Wrap(err, "Reading target")
	}
	actual := t.DeepCopyObject().(T)
	updated := t.DeepCopyObject().(T)

	log.FromContext(ctx).Info("Call mutator")
	var (
		result         *ctrl.Result
		reconcileError error
	)
	func() {
		defer func() {
			if reconcileError != nil {
				apisv1beta2.SetError(updated, reconcileError)
			}
		}()
		defer func() {
			if e := recover(); e != nil {
				reconcileError = fmt.Errorf("%s", e)
				log.FromContext(ctx).Error(reconcileError, "reconciling")
				debug.PrintStack()
			}
		}()

		result, reconcileError = r.Mutator.Mutate(ctx, updated)
		if reconcileError != nil {
			log.FromContext(ctx).Error(reconcileError, "Reconciling")
		} else {
			apisv1beta2.RemoveCondition(updated, apisv1beta2.ConditionTypeError)
		}
	}()

	if updated.IsDirty(actual) {
		log.FromContext(ctx).Info("Object dirty, updating it")
		if patchErr := r.Update(ctx, updated); patchErr != nil {
			log.FromContext(ctx).Error(patchErr, "Updating object")
			return ctrl.Result{}, patchErr
		}
	} else {
		if updated.GetStatus().IsDirty(actual) {
			log.FromContext(ctx).Info("Object dirty, updating status")
			if patchErr := r.Status().Update(ctx, updated); patchErr != nil {
				log.FromContext(ctx).Error(patchErr, "Updating status")
				return ctrl.Result{}, patchErr
			}
		}
	}

	if result != nil {
		return *result, nil
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler[T]) SetupWithManager(mgr ctrl.Manager) error {
	var t T
	t = reflect.New(reflect.TypeOf(t).Elem()).Interface().(T)

	gvk, err := apiutil.GVKForObject(t, r.Scheme)
	if err != nil {
		return err
	}

	builder := ctrl.NewControllerManagedBy(mgr).
		For(t).
		WithLogConstructor(func(req *reconcile.Request) logr.Logger {
			log := mgr.GetLogger().WithValues(
				"controller", strings.ToLower(gvk.Kind),
				"controllerGroup", gvk.Group,
				"controllerKind", gvk.Kind,
			)

			lowerCamelCaseKind := strings.ToLower(gvk.Kind[:1]) + gvk.Kind[1:]

			if req != nil {
				log = log.WithValues(
					lowerCamelCaseKind, klog.KRef(req.Namespace, req.Name),
					"namespace", req.Namespace, "name", req.Name,
				)
			}

			return log
		})
	if err := r.Mutator.SetupWithBuilder(mgr, builder); err != nil {
		return err
	}

	return builder.Complete(r)
}

func NewReconciler[T apisv1beta2.Object](client client.Client, scheme *runtime.Scheme, mutator Mutator[T]) *Reconciler[T] {
	return &Reconciler[T]{
		Client:  client,
		Scheme:  scheme,
		Mutator: mutator,
	}
}
