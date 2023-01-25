package controllerutils

import (
	apisv1beta2 "github.com/formancehq/operator/pkg/apis/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ConditionTypeSecretReady = "SecretReady"
)

func SetSecretReady(object apisv1beta2.Object, msg ...string) {
	apisv1beta2.SetCondition(object, ConditionTypeSecretReady, metav1.ConditionTrue, msg...)
}

func SetSecretError(object apisv1beta2.Object, msg ...string) {
	apisv1beta2.SetCondition(object, ConditionTypeSecretReady, metav1.ConditionFalse, msg...)
}
