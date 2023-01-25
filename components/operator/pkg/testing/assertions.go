package testing

import (
	apisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	"github.com/formancehq/operator/pkg/typeutils"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func ConditionStatus(object apisv1beta2.Object, conditionType string) func() v1.ConditionStatus {
	return func() v1.ConditionStatus {
		c := GetCondition(object, conditionType)()
		if c == nil {
			return v1.ConditionUnknown
		}
		return c.Status
	}
}

func GetCondition(object apisv1beta2.Object, conditionType string) func() *apisv1beta2.Condition {
	return func() *apisv1beta2.Condition {
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(object), object)
		if err != nil {
			return nil
		}
		return typeutils.First(*object.GetConditions(), func(t apisv1beta2.Condition) bool {
			return t.Type == conditionType
		})
	}
}

func NotFound(object client.Object) func() bool {
	return func() bool {
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(object), object)
		switch {
		case errors.IsNotFound(err):
			return true
		case err != nil:
			panic(err)
		default:
			return false
		}
	}
}

func Exists(object client.Object) func() bool {
	return func() bool {
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(object), object)
		switch {
		case errors.IsNotFound(err):
			return false
		case err == nil:
			return true
		default:
			panic(err)
		}
	}
}
