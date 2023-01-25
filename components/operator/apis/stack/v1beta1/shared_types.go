package v1beta1

type ScalingSpec struct {
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// +optional
	MinReplica int `json:"minReplica,omitempty"`
	// +optional
	MaxReplica int `json:"maxReplica,omitempty"`
	// +optional
	CpuLimit int `json:"cpuLimit,omitempty"`
}

type DatabaseSpec struct {
	// +optional
	Url string `json:"url,omitempty"`
	// +optional
	Type string `json:"type,omitempty"`
}

type IngressConfig struct {
	// +optional
	Enabled *bool `json:"enabled"`
	// +optional
	Annotations map[string]string `json:"annotations"`
	// +optional
	Host string `json:"host"`
}
