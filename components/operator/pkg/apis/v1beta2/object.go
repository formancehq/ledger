package v1beta2

import (
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// +kubebuilder:object:generate=false
type Dirty interface {
	IsDirty(t Object) bool
}

// +kubebuilder:object:generate=false
type Object interface {
	client.Object
	Dirty
	GetStatus() Dirty
	GetConditions() *Conditions
}
