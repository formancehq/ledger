package sdk

import (
	"fmt"
	"strings"
)

const (
	TargetCapabilityOrganization = "target.organization"
	TargetCapabilityStack        = "target.stack"
)

// TargetKind identifies the target coordinates required by a command.
type TargetKind string

const (
	TargetNone         TargetKind = "none"
	TargetOrganization TargetKind = "organization"
	TargetStack        TargetKind = "stack"
	TargetApplication  TargetKind = "application"
)

// TargetRequirement declares the target coordinates required by a command.
// Kind is always explicit, including TargetNone for commands without a target.
type TargetRequirement struct {
	Kind TargetKind `json:"kind"`
}

// TargetSelection carries host-validated target IDs to a product command.
type TargetSelection struct {
	OrganizationID string `json:"organizationId"`
	StackID        string `json:"stackId"`
}

// TargetRef identifies a parent target supplied to a discovery provider.
type TargetRef struct {
	Kind string
	ID   string
}

// TargetDescriptor is one discovered target. ResourceURI is supplied only for
// targets whose resource endpoint must later be resolved by the host.
type TargetDescriptor struct {
	Kind        string
	ID          string
	ParentID    string
	Name        string
	State       string
	ResourceURI string
}

// TargetDiscoveryRequest bounds one target discovery operation.
type TargetDiscoveryRequest struct {
	Capability string
	Parent     *TargetRef
}

// TargetDiscoveryResult contains the typed targets returned by one discovery
// operation.
type TargetDiscoveryResult struct {
	Targets []TargetDescriptor
}

// TargetOperationPolicy pairs one bounded discovery operation with its
// independently selected authentication requirements.
type TargetOperationPolicy struct {
	Operation OperationPolicy
	Auth      []AuthRequirement
}

// TargetProviderMetadata identifies a target provider and the bounded
// operations it may request through a TargetHost.
type TargetProviderMetadata struct {
	Name         string
	Version      string
	Capabilities []string
	Operations   []TargetOperationPolicy
}

// Clone returns a deep copy of mutable target-provider metadata fields.
func (metadata TargetProviderMetadata) Clone() TargetProviderMetadata {
	clone := metadata
	clone.Capabilities = cloneStrings(metadata.Capabilities)
	clone.Operations = cloneTargetOperationPolicies(metadata.Operations)
	return clone
}

// ValidateTargetSelection verifies that host-provided IDs satisfy a command's
// target requirement.
func ValidateTargetSelection(requirement TargetRequirement, selection TargetSelection) error {
	kind, err := normalizedTargetKind(requirement)
	if err != nil {
		return err
	}
	switch kind {
	case TargetNone:
		if selection.OrganizationID != "" || selection.StackID != "" {
			return fmt.Errorf("target kind %q does not accept target coordinates", kind)
		}
	case TargetOrganization:
		if selection.OrganizationID == "" || selection.StackID != "" {
			return fmt.Errorf("target kind %q requires only an organization id", kind)
		}
	case TargetStack:
		if selection.OrganizationID == "" || selection.StackID == "" {
			return fmt.Errorf("target kind %q requires organization and stack ids", kind)
		}
	case TargetApplication:
		if selection.OrganizationID != "" || selection.StackID != "" {
			return fmt.Errorf("target kind %q does not accept target coordinates", kind)
		}
	}
	return nil
}

// ValidateTargetProviderMetadata rejects target-provider metadata that could
// exceed the host's declared discovery policy.
func ValidateTargetProviderMetadata(metadata TargetProviderMetadata) error {
	if strings.TrimSpace(metadata.Name) == "" {
		return fmt.Errorf("target provider name is required")
	}
	if strings.TrimSpace(metadata.Version) == "" {
		return fmt.Errorf("target provider version is required")
	}
	if err := validateTargetCapabilities(metadata.Capabilities); err != nil {
		return fmt.Errorf("target provider capabilities: %w", err)
	}
	if err := validateTargetOperationPolicies(metadata.Operations); err != nil {
		return fmt.Errorf("target provider operations: %w", err)
	}
	return nil
}

func normalizedTargetKind(requirement TargetRequirement) (TargetKind, error) {
	switch requirement.Kind {
	case TargetNone:
		return TargetNone, nil
	case TargetOrganization, TargetStack, TargetApplication:
		return requirement.Kind, nil
	default:
		return "", fmt.Errorf("unsupported target kind %q", requirement.Kind)
	}
}

func validateTargetCapabilities(capabilities []string) error {
	if len(capabilities) == 0 {
		return fmt.Errorf("requires at least one capability")
	}
	seen := make(map[string]struct{}, len(capabilities))
	for _, capability := range capabilities {
		if capability != TargetCapabilityOrganization && capability != TargetCapabilityStack {
			return fmt.Errorf("has unsupported capability %q", capability)
		}
		if _, exists := seen[capability]; exists {
			return fmt.Errorf("repeats capability %q", capability)
		}
		seen[capability] = struct{}{}
	}
	return nil
}

func validateTargetOperationPolicies(policies []TargetOperationPolicy) error {
	operations := make(map[string]struct{}, len(policies))
	for index, policy := range policies {
		operation := policy.Operation
		if err := validateOperationPolicy(operation); err != nil {
			return fmt.Errorf("operation %d: %w", index, err)
		}
		if _, exists := operations[operation.ID]; exists {
			return fmt.Errorf("repeats operation %q", operation.ID)
		}
		operations[operation.ID] = struct{}{}
		for _, requirement := range policy.Auth {
			if err := validateAuthRequirement(requirement); err != nil {
				return fmt.Errorf("operation %q: %w", operation.ID, err)
			}
		}
	}
	return nil
}

func cloneStrings(values []string) []string {
	if values == nil {
		return nil
	}
	clone := make([]string, len(values))
	copy(clone, values)
	return clone
}

func cloneTargetOperationPolicies(policies []TargetOperationPolicy) []TargetOperationPolicy {
	if policies == nil {
		return nil
	}
	clone := make([]TargetOperationPolicy, len(policies))
	for index, policy := range policies {
		clone[index] = policy
		clone[index].Operation = cloneOperationPolicy(policy.Operation)
		if policy.Auth != nil {
			clone[index].Auth = make([]AuthRequirement, len(policy.Auth))
			copy(clone[index].Auth, policy.Auth)
		}
	}
	return clone
}

func cloneOperationPolicy(policy OperationPolicy) OperationPolicy {
	clone := policy
	clone.Scopes = cloneStrings(policy.Scopes)
	if policy.HTTP != nil {
		http := *policy.HTTP
		if policy.HTTP.GeneratedClient != nil {
			generated := *policy.HTTP.GeneratedClient
			generated.RequestContentTypes = cloneStrings(policy.HTTP.GeneratedClient.RequestContentTypes)
			generated.RequestHeaders = cloneStrings(policy.HTTP.GeneratedClient.RequestHeaders)
			http.GeneratedClient = &generated
		}
		clone.HTTP = &http
	}
	if policy.GRPC != nil {
		grpc := *policy.GRPC
		if policy.GRPC.GeneratedClient != nil {
			generated := *policy.GRPC.GeneratedClient
			grpc.GeneratedClient = &generated
		}
		clone.GRPC = &grpc
	}
	return clone
}
