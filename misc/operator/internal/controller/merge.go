package controller

import (
	corev1 "k8s.io/api/core/v1"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

// applyDefaultsFromRef merges fields from a LedgerDefaultsSpec into a LedgerServiceSpec.
// LedgerService-level non-zero/non-nil values always take precedence.
func applyDefaultsFromRef(spec *ledgerv1alpha1.LedgerServiceSpec, defaults *ledgerv1alpha1.LedgerDefaultsSpec) {
	mergeImageSpec(&spec.Image, &defaults.Image)
	mergeServiceAccountSpec(&spec.ServiceAccount, &defaults.ServiceAccount)
	mergeDefaultsConfig(&spec.Config, &defaults.Config)
	mergeResources(&spec.Resources, &defaults.Resources)

	// Slices: LedgerService non-nil replaces entirely.
	if spec.ImagePullSecrets == nil {
		spec.ImagePullSecrets = defaults.ImagePullSecrets
	}
	if spec.Tolerations == nil {
		spec.Tolerations = defaults.Tolerations
	}

	// Pointer fields: LedgerService non-nil wins (whole-block replacement).
	if spec.LivenessProbe == nil {
		spec.LivenessProbe = defaults.LivenessProbe
	}
	if spec.ReadinessProbe == nil {
		spec.ReadinessProbe = defaults.ReadinessProbe
	}
	if spec.PodSecurityContext == nil {
		spec.PodSecurityContext = defaults.PodSecurityContext
	}
	if spec.SecurityContext == nil {
		spec.SecurityContext = defaults.SecurityContext
	}
	if spec.Affinity == nil {
		spec.Affinity = defaults.Affinity
	}
	if spec.PodAntiAffinity == nil {
		spec.PodAntiAffinity = defaults.PodAntiAffinity
	}
	if spec.PodDisruptionBudget == nil {
		spec.PodDisruptionBudget = defaults.PodDisruptionBudget
	}
	if spec.ServiceMonitor == nil {
		spec.ServiceMonitor = defaults.ServiceMonitor
	}

	// Maps: LedgerService non-nil replaces.
	if spec.NodeSelector == nil {
		spec.NodeSelector = defaults.NodeSelector
	}
}

// mergeImageSpec merges default image values into spec where spec fields are zero.
func mergeImageSpec(spec *ledgerv1alpha1.ImageSpec, defaults *ledgerv1alpha1.ImageSpec) {
	if spec.Repository == "" {
		spec.Repository = defaults.Repository
	}
	if spec.Tag == "" {
		spec.Tag = defaults.Tag
	}
	if spec.PullPolicy == "" {
		spec.PullPolicy = defaults.PullPolicy
	}
}

// mergeServiceAccountSpec merges default service account values into spec.
func mergeServiceAccountSpec(spec *ledgerv1alpha1.ServiceAccountSpec, defaults *ledgerv1alpha1.ServiceAccountSpec) {
	if spec.Create == nil {
		spec.Create = defaults.Create
	}
	if spec.Name == "" {
		spec.Name = defaults.Name
	}
	if spec.Annotations == nil {
		spec.Annotations = defaults.Annotations
	}
}

// mergeDefaultsConfig merges the shared config subset from LedgerDefaultsConfig
// into LedgerServiceConfig. Only pointer fields that are nil in spec get the default value.
func mergeDefaultsConfig(spec *ledgerv1alpha1.LedgerServiceConfig, defaults *ledgerv1alpha1.LedgerDefaultsConfig) {
	if spec.Pebble == nil {
		spec.Pebble = defaults.Pebble
	}
	if spec.Raft == nil {
		spec.Raft = defaults.Raft
	}
	if spec.Health == nil {
		spec.Health = defaults.Health
	}
	if spec.ColdStorage == nil {
		spec.ColdStorage = defaults.ColdStorage
	}
	if spec.TLS == nil {
		spec.TLS = defaults.TLS
	}
	if spec.ResponseSigning == nil {
		spec.ResponseSigning = defaults.ResponseSigning
	}
	if spec.Monitoring == nil {
		spec.Monitoring = defaults.Monitoring
	}
}

// mergeResources merges default resource requirements into spec.
// Each resource list (Requests, Limits) is replaced as a whole if nil in spec.
func mergeResources(spec *corev1.ResourceRequirements, defaults *corev1.ResourceRequirements) {
	if spec.Requests == nil {
		spec.Requests = defaults.Requests
	}
	if spec.Limits == nil {
		spec.Limits = defaults.Limits
	}
}
