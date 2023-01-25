package controllerutils

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"
)

func DefaultLiveness() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/_healthcheck",
				Port: intstr.IntOrString{
					IntVal: 8080,
				},
				Scheme: "HTTP",
			},
		},
		InitialDelaySeconds:           1,
		TimeoutSeconds:                30,
		PeriodSeconds:                 2,
		SuccessThreshold:              1,
		FailureThreshold:              10,
		TerminationGracePeriodSeconds: pointer.Int64(10),
	}
}
