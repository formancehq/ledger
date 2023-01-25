package v1beta2

import (
	"github.com/formancehq/operator/pkg/typeutils"
)

type Status struct {
	// +patchMergeKey=type
	// +patchStrategy=merge
	Conditions Conditions `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

func (in *Status) GetConditions() []Condition {
	return in.Conditions
}

func (in *Status) IsDirty(reference Object) bool {
	conditionsChanged := len(in.Conditions) != len(*reference.GetConditions())
	if !conditionsChanged {
		for _, condition := range *reference.GetConditions() {
			v := typeutils.First(in.Conditions, func(c Condition) bool {
				return c.Type == condition.Type
			})
			if v == nil {
				conditionsChanged = true
				break
			}
			if (*v).Status != condition.Status {
				conditionsChanged = true
				break
			}
			if (*v).ObservedGeneration != condition.ObservedGeneration {
				conditionsChanged = true
				break
			}
		}
	}
	return conditionsChanged
}

func (in *Status) GetCondition(conditionType string) *Condition {
	if in != nil {
		for _, condition := range in.Conditions {
			if condition.Type == conditionType {
				return &condition
			}
		}
	}
	return nil
}

func (in *Status) SetCondition(condition Condition) {
	for i, c := range in.Conditions {
		if c.Type == condition.Type {
			in.Conditions[i] = condition
			return
		}
	}
	in.Conditions = append(in.Conditions, condition)
}

func (in *Status) RemoveCondition(v string) {
	in.Conditions = typeutils.Filter(in.Conditions, func(stack Condition) bool {
		return stack.Type != v
	})
}

type ReplicationStatus struct {
	Status `json:",inline"`
	// +optional
	Replicas int32 `json:"replicas"`
	// +optional
	Selector string `json:"selector"`
}
