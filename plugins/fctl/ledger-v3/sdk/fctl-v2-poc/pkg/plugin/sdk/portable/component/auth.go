package component

import (
	pb "github.com/formancehq/fctl-v2-poc/pkg/plugin/protocol/componentbridgev1alpha1"
	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk"
	"github.com/formancehq/fctl-v2-poc/pkg/plugin/sdk/portable"
)

func encodeAuthRequest(value portable.AuthRequest) (*pb.AuthBrokerRequest, error) {
	slot := encodeSlot(value.Slot)
	handle := &pb.CredentialHandle{Opaque: string(value.Handle)}
	request := &pb.AuthBrokerRequest{}
	switch value.Operation {
	case portable.AuthLoad:
		request.Operation = &pb.AuthBrokerRequest_Load{Load: &pb.AuthLoadRequest{Slot: slot}}
	case portable.AuthAuthorizeOIDC:
		request.Operation = &pb.AuthBrokerRequest_AuthorizeOidc{AuthorizeOidc: &pb.AuthorizeOidcRequest{Slot: slot, Intent: &pb.OidcIntent{Issuer: value.Issuer, Scopes: value.Scopes}}}
	case portable.AuthRefresh:
		request.Operation = &pb.AuthBrokerRequest_Refresh{Refresh: &pb.AuthRefreshRequest{Slot: slot, Handle: handle, Scopes: value.Scopes}}
	case portable.AuthExchange:
		request.Operation = &pb.AuthBrokerRequest_Exchange{Exchange: &pb.AuthExchangeRequest{Slot: slot, Handle: handle, Resource: value.Resource, Scopes: value.Scopes}}
	case portable.AuthInvalidate:
		request.Operation = &pb.AuthBrokerRequest_Invalidate{Invalidate: &pb.AuthInvalidateRequest{Slot: slot, Descendants: value.Descendants}}
	case portable.AuthBind:
		request.Operation = &pb.AuthBrokerRequest_Bind{Bind: &pb.AuthBindRequest{Handle: handle, Service: string(value.Service), Operations: value.Operations}}
	case portable.AuthAuthorizeClientCredentials:
		request.Operation = &pb.AuthBrokerRequest_AuthorizeClientCredentials{AuthorizeClientCredentials: &pb.AuthorizeClientCredentialsRequest{Slot: slot, Service: string(value.Service), Scopes: value.Scopes}}
	case portable.AuthBindCredential:
		request.Operation = &pb.AuthBrokerRequest_BindCredential{BindCredential: &pb.BindCredentialRequest{Slot: slot, CredentialHandle: handle, Operations: value.Operations}}
	default:
		return nil, pb.ErrProtocol
	}
	return request, nil
}

func encodeSlot(slot sdk.CredentialSlot) *pb.CredentialSlot {
	return &pb.CredentialSlot{Stage: slot.Stage, Profile: slot.Profile, Service: string(slot.Service), Capability: slot.Capability, Provider: slot.Provider, ProviderVersion: slot.ProviderVersion, ArtifactDigest: slot.ArtifactDigest, FacetProtocolVersion: slot.FacetProtocolVersion, ProfileCapabilityGeneration: slot.ProfileCapabilityGeneration, OrganizationId: slot.OrganizationID, StackId: slot.StackID, Audience: slot.Audience, Scopes: append([]string{}, slot.Scopes...)}
}

func authSlotToSDK(slot *pb.CredentialSlot) sdk.CredentialSlot {
	if slot == nil {
		return sdk.CredentialSlot{}
	}
	return sdk.CredentialSlot{Stage: slot.GetStage(), Profile: slot.GetProfile(), Service: sdk.Service(slot.GetService()), Capability: slot.GetCapability(), Provider: slot.GetProvider(), ProviderVersion: slot.GetProviderVersion(), ArtifactDigest: slot.GetArtifactDigest(), FacetProtocolVersion: slot.GetFacetProtocolVersion(), ProfileCapabilityGeneration: slot.GetProfileCapabilityGeneration(), OrganizationID: slot.GetOrganizationId(), StackID: slot.GetStackId(), Audience: slot.GetAudience(), Scopes: append([]string{}, slot.GetScopes()...)}
}

func decodeAuthResponse(operation portable.AuthOperation, value *pb.AuthBrokerResponse) (portable.AuthResponse, error) {
	var result portable.AuthResponse
	if value == nil {
		return result, pb.ErrProtocol
	}
	lease := func(value *pb.LeaseMetadata) sdk.LeaseMetadata {
		return sdk.LeaseMetadata{ExpiresAtUnix: value.GetExpiresAtUnix(), Refreshable: value.GetRefreshable()}
	}
	switch operation {
	case portable.AuthLoad, portable.AuthAuthorizeOIDC, portable.AuthRefresh, portable.AuthExchange:
		credential := value.GetCredential()
		if credential == nil || credential.GetHandle() == nil || credential.GetLease() == nil {
			return result, pb.ErrProtocol
		}
		switch credential.GetState() {
		case pb.StoreState_STORE_STATE_MISSING:
			result.State = sdk.StoreStateMissing
		case pb.StoreState_STORE_STATE_VALID:
			result.State = sdk.StoreStateValid
		case pb.StoreState_STORE_STATE_EXPIRED:
			result.State = sdk.StoreStateExpired
		case pb.StoreState_STORE_STATE_INVALID:
			result.State = sdk.StoreStateInvalid
		default:
			return result, pb.ErrProtocol
		}
		if operation != portable.AuthLoad && result.State != sdk.StoreStateValid {
			return result, pb.ErrProtocol
		}
		result.Handle = sdk.CredentialHandle(credential.GetHandle().GetOpaque())
		result.Lease = lease(credential.GetLease())
		if result.State == sdk.StoreStateValid && result.Handle == "" {
			return result, pb.ErrProtocol
		}
	case portable.AuthInvalidate:
		if !value.GetInvalidated() {
			return result, pb.ErrProtocol
		}
	case portable.AuthBind:
		if value.GetBinding() == nil || value.GetBinding().GetOpaque() == "" {
			return result, pb.ErrProtocol
		}
		result.Binding = sdk.ServiceBinding(value.GetBinding().GetOpaque())
	case portable.AuthAuthorizeClientCredentials:
		response := value.GetAuthorizeClientCredentials()
		if response == nil || response.GetCredentialHandle() == nil || response.GetCredentialHandle().GetOpaque() == "" || response.GetLease() == nil {
			return result, pb.ErrProtocol
		}
		result.Handle = sdk.CredentialHandle(response.GetCredentialHandle().GetOpaque())
		result.Lease = lease(response.GetLease())
	case portable.AuthBindCredential:
		response := value.GetBindCredential()
		if response == nil || response.GetBinding() == nil || response.GetBinding().GetOpaque() == "" || response.GetLease() == nil {
			return result, pb.ErrProtocol
		}
		result.Binding = sdk.ServiceBinding(response.GetBinding().GetOpaque())
		result.Lease = lease(response.GetLease())
	default:
		return result, pb.ErrProtocol
	}
	return result, nil
}
