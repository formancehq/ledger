package v1beta2

import (
	"fmt"
	"reflect"

	"github.com/formancehq/operator/pkg/typeutils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func authServerChanges(previous, actual client.Object, reference string) bool {
	expectedOwnerRef := fmt.Sprintf("%s-%s", actual.GetNamespace(), reference)
	ownerFromSource := typeutils.First(previous.GetOwnerReferences(), func(ref metav1.OwnerReference) bool {
		return ref.Name == expectedOwnerRef
	})
	ownerFromActual := typeutils.First(actual.GetOwnerReferences(), func(ref metav1.OwnerReference) bool {
		return ref.Name == expectedOwnerRef
	})
	return !reflect.DeepEqual(ownerFromSource, ownerFromActual)
}
