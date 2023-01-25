package v1beta2

import (
	autoscallingv2 "k8s.io/api/autoscaling/v2"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Scalable struct {
	// +optional
	// +kubebuilder:default:=1
	Replicas *int32 `json:"replicas,omitempty"`
	// minReplicas is the lower limit for the number of replicas to which the autoscaler
	// can scale down.  It defaults to 1 pod.  minReplicas is allowed to be 0 if the
	// alpha feature gate HPAScaleToZero is enabled and at least one Object or External
	// metric is configured.  Scaling is active as long as at least one metric value is
	// available.
	// +optional
	MinReplicas *int32 `json:"minReplicas,omitempty" protobuf:"varint,2,opt,name=minReplicas"`
	// upper limit for the number of pods that can be set by the autoscaler; cannot be smaller than MinReplicas.
	// If not specified, the default will be 10
	// +optional
	// +kubebuilder:default:=10
	MaxReplicas int32 `json:"maxReplicas,omitempty" protobuf:"varint,3,opt,name=maxReplicas"`
	// metrics contains the specifications for which to use to calculate the
	// desired replica count (the maximum replica count across all metrics will
	// be used).  The desired replica count is calculated multiplying the
	// ratio between the target value and the current value by the current
	// number of pods.  Ergo, metrics used must decrease as the pod count is
	// increased, and vice-versa.  See the individual metric source types for
	// more information about how each type of metric must respond.
	// +optional
	Metrics []autoscallingv2.MetricSpec `json:"metrics,omitempty" protobuf:"bytes,4,rep,name=metrics"`
}

func (s Scalable) GetReplicas() *int32 {
	if s.Replicas != nil {
		return s.Replicas
	}
	replicas := int32(1)
	return &replicas
}

func (s Scalable) GetHPASpec(object client.Object) autoscallingv2.HorizontalPodAutoscalerSpec {
	gvk := object.GetObjectKind().GroupVersionKind()
	return autoscallingv2.HorizontalPodAutoscalerSpec{
		ScaleTargetRef: autoscallingv2.CrossVersionObjectReference{
			Kind:       gvk.Kind,
			Name:       object.GetName(),
			APIVersion: gvk.GroupVersion().String(),
		},
		MinReplicas: pointer.Int32(1),
		MaxReplicas: s.MaxReplicas,
		Metrics:     s.Metrics,
	}
}

func (s Scalable) WithReplicas(replicas *int32) Scalable {
	s.Replicas = replicas
	return s
}
