package controllerutils

import (
	corev1 "k8s.io/api/core/v1"
)

func ImagePullPolicy(o interface {
	GetVersion() string
}) corev1.PullPolicy {
	version := o.GetVersion()
	imagePullPolicy := corev1.PullIfNotPresent
	if version == "latest" {
		imagePullPolicy = corev1.PullAlways
	}
	return imagePullPolicy
}
