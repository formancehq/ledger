package controllerutils

import (
	v1 "k8s.io/api/core/v1"
)

func SinglePort(name string, port int32) []v1.ContainerPort {
	return []v1.ContainerPort{{
		Name:          name,
		ContainerPort: port,
	}}
}
