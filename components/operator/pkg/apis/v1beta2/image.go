package v1beta2

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

type ImageHolder struct {
	// +optional
	Image string `json:"image,omitempty"`

	// ImagePullSecrets is an optional list of references to secrets in the same namespace to use for pulling any of the images used by this PodSpec.
	// If specified, these secrets will be passed to individual puller implementations for them to use.
	// More info: https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,15,rep,name=imagePullSecrets"`
}

func (h *ImageHolder) GetImage(component string) string {
	if h.Image == "" {
		return fmt.Sprintf("ghcr.io/formancehq/%s:latest", component)
	}
	return h.Image
}
