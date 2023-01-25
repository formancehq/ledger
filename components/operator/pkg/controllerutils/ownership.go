package controllerutils

import (
	"context"

	pkgError "github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func DefineOwner(ctx context.Context, client client.Client, scheme *runtime.Scheme, ob client.Object, ownerKey types.NamespacedName, ownerType client.Object) error {
	if err := client.Get(ctx, ownerKey, ownerType); err != nil {
		return pkgError.Wrap(err, "Retrieving object")
	}

	if err := controllerutil.SetOwnerReference(ownerType, ob, scheme); err != nil {
		return pkgError.Wrap(err, "Setting owner reference to object")
	}

	references := ob.GetOwnerReferences()
	for ind, ref := range references {
		if ref.UID == ownerType.GetUID() {
			ref.BlockOwnerDeletion = pointer.Bool(true)
			references[ind] = ref
		}
	}
	ob.SetOwnerReferences(references)
	return nil
}

func OwnerReference(o client.Object) metav1.OwnerReference {
	groupVersionKinds, _, err := scheme.Scheme.ObjectKinds(o)
	if err != nil {
		panic(err)
	}
	groupVersionKind := groupVersionKinds[0]
	return metav1.OwnerReference{
		Kind:               groupVersionKind.Kind,
		APIVersion:         groupVersionKind.GroupVersion().String(),
		Name:               o.GetName(),
		UID:                o.GetUID(),
		BlockOwnerDeletion: pointer.Bool(true),
		Controller:         pointer.Bool(true),
	}
}
