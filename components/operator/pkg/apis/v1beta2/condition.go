package v1beta2

import (
	"github.com/formancehq/operator/pkg/typeutils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Condition struct {
	// type of condition in CamelCase or in foo.example.com/CamelCase.
	// ---
	// Many .condition.type values are consistent across resources like Available, but because arbitrary conditions can be
	// useful (see .node.status.conditions), the ability to deconflict is important.
	// The regex it matches is (dns1123SubdomainFmt/)?(qualifiedNameFmt)
	// +required
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^([a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*/)?(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])$`
	// +kubebuilder:validation:MaxLength=316
	Type string `json:"type" protobuf:"bytes,1,opt,name=type"`
	// status of the condition, one of True, False, Unknown.
	// +required
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=True;False;Unknown
	Status metav1.ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status"`
	// observedGeneration represents the .metadata.generation that the condition was set based upon.
	// For instance, if .metadata.generation is currently 12, but the .status.conditions[x].observedGeneration is 9, the condition is out of date
	// with respect to the current state of the instance.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=0
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,3,opt,name=observedGeneration"`
	// lastTransitionTime is the last time the condition transitioned from one status to another.
	// This should be when the underlying condition changed.  If that is not known, then using the time when the API field changed is acceptable.
	// +required
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Format=date-time
	LastTransitionTime metav1.Time `json:"lastTransitionTime" protobuf:"bytes,4,opt,name=lastTransitionTime"`
	// message is a human readable message indicating details about the transition.
	// This may be an empty string.
	// +required
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:MaxLength=32768
	Message string `json:"message,omitempty" protobuf:"bytes,6,opt,name=message"`
}

func SetCondition(object Object, conditionType string, status metav1.ConditionStatus, msg ...string) {
	if len(msg) > 1 {
		panic("Only one message can be passed")
	}
	object.GetConditions().Set(Condition{
		Type:               conditionType,
		Status:             status,
		ObservedGeneration: object.GetGeneration(),
		LastTransitionTime: metav1.Now(),
		Message: func() string {
			if len(msg) > 0 {
				return msg[0]
			}
			return ""
		}(),
	})
}

func RemoveCondition(object Object, conditionType string) {
	object.GetConditions().Remove(conditionType)
}

type Conditions []Condition

func (conditions *Conditions) Set(condition Condition) {
	if conditions == nil {
		*conditions = Conditions{}
	}
	for i, c := range *conditions {
		if c.Type == condition.Type {
			(*conditions)[i] = condition
			return
		}
	}
	*conditions = append(*conditions, condition)
}

func (c *Conditions) Remove(t string) {
	*c = typeutils.Filter(*c, func(c Condition) bool {
		return c.Type != t
	})
}

const (
	ConditionTypeReady       = "Ready"
	ConditionTypeProgressing = "Progressing"
	ConditionTypeError       = "Error"
)

func SetReady(object Object, msg ...string) {
	object.GetConditions().Remove(ConditionTypeProgressing)
	SetCondition(object, ConditionTypeReady, metav1.ConditionTrue, msg...)
}

func RemoveReadyCondition(object Object) {
	object.GetConditions().Remove(ConditionTypeReady)
}

func SetProgressing(object Object, msg ...string) {
	object.GetConditions().Remove(ConditionTypeReady)
	SetCondition(object, ConditionTypeProgressing, metav1.ConditionTrue, msg...)
}

func SetError(object Object, err error) {
	SetCondition(object, ConditionTypeError, metav1.ConditionTrue, err.Error())
}
