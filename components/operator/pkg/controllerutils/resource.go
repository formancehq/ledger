package controllerutils

import (
	"context"
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const ReloaderAnnotationKey = "reloader.stakater.com/auto"

type ObjectMutator[T any] func(t T) error

func WithController[T client.Object](owner client.Object, scheme *runtime.Scheme) ObjectMutator[T] {
	return func(t T) error {
		if !metav1.IsControlledBy(t, owner) {
			return controllerutil.SetControllerReference(owner, t, scheme)
		}
		return nil
	}
}

func WithAnnotations[T client.Object](newAnnotations map[string]string) ObjectMutator[T] {
	return func(t T) error {
		annotations := t.GetAnnotations()
		if annotations == nil {
			annotations = make(map[string]string)
		}
		for k, v := range newAnnotations {
			annotations[k] = v
		}
		t.SetAnnotations(annotations)
		return nil
	}
}

func WithReloaderAnnotations[T client.Object]() ObjectMutator[T] {
	return WithAnnotations[T](map[string]string{
		ReloaderAnnotationKey: "true",
	})
}

func CreateOrUpdate[T client.Object](ctx context.Context, client client.Client,
	key types.NamespacedName, mutators ...ObjectMutator[T]) (T, controllerutil.OperationResult, error) {
	var ret T
	ret = reflect.New(reflect.TypeOf(ret).Elem()).Interface().(T)
	ret.SetNamespace(key.Namespace)
	ret.SetName(key.Name)
	operationResult, err := controllerutil.CreateOrUpdate(ctx, client, ret, func() error {
		labels := ret.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels["stack"] = "true"
		ret.SetLabels(labels)
		for _, mutate := range mutators {
			if err := mutate(ret); err != nil {
				return err
			}
		}
		return nil
	})
	return ret, operationResult, err
}
