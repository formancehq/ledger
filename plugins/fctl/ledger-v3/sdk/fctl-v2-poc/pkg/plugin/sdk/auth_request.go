package sdk

import (
	"errors"
	"slices"
)

// ValidateAuthRequest accepts OIDC-only starts without a credential slot and
// validates every field when the host supplies final client-credentials
// authority. The slot contains authorization metadata only; credential and
// endpoint bytes remain host-owned.
func ValidateAuthRequest(request AuthRequest) error {
	if request.CredentialSlot == nil {
		return nil
	}
	if request.Capability == "" || request.Service == "" || len(request.Operations) == 0 {
		return errors.New("auth request is incomplete")
	}
	for _, operation := range request.Operations {
		if operation == "" {
			return errors.New("auth request is incomplete")
		}
	}
	slot := *request.CredentialSlot
	if len(request.Operations) != 1 ||
		slot.Stage != "" || slot.Profile == "" || slot.Service != request.Service || slot.Capability != request.Capability ||
		slot.Provider == "" || slot.ProviderVersion == "" || slot.ArtifactDigest == "" ||
		slot.FacetProtocolVersion != CurrentAuthProviderFacetProtocolVersion || slot.ProfileCapabilityGeneration == 0 ||
		slot.OrganizationID != request.Target.OrganizationID || slot.StackID != request.Target.StackID ||
		slot.Audience == "" || !ExactScopeSet(slot.Scopes) {
		return errors.New("auth request does not carry an exact credential slot")
	}
	return nil
}

// CloneAuthRequest freezes caller-owned lists before provider code starts.
func CloneAuthRequest(request AuthRequest) AuthRequest {
	request.Operations = slices.Clone(request.Operations)
	if request.CredentialSlot != nil {
		slot := *request.CredentialSlot
		slot.Scopes = slices.Clone(slot.Scopes)
		request.CredentialSlot = &slot
	}
	return request
}
