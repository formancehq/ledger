package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func processRegisterSigningKey(order *raftcmdpb.RegisterSigningKeyOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	if err := domain.ValidateSigningKeyID(order.GetKeyId()); err != nil {
		return nil, err
	}

	// Parent key ID is optional ("" = root key). Only validate the shape
	// when it's set so registering a root key stays a single-field call.
	if parent := order.GetParentKeyId(); parent != "" {
		if err := domain.ValidateSigningKeyID(parent); err != nil {
			return nil, err
		}
	}

	ctx.Scope.AddSigningKey(order.GetKeyId(), order.GetPublicKey(), order.GetParentKeyId())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_RegisterSigningKey{
			RegisterSigningKey: &commonpb.RegisteredSigningKeyLog{
				KeyId:       order.GetKeyId(),
				PublicKey:   order.GetPublicKey(),
				ParentKeyId: order.GetParentKeyId(),
			},
		},
	}, nil
}

func processRevokeSigningKey(order *raftcmdpb.RevokeSigningKeyOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	s := ctx.Scope
	if err := domain.ValidateSigningKeyID(order.GetKeyId()); err != nil {
		return nil, err
	}

	var cascaded []string

	if order.GetCascade() {
		// BFS to find all descendants for cascade revocation
		queue := []string{order.GetKeyId()}
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			children := s.GetSigningKeyChildren(current)
			cascaded = append(cascaded, children...)
			queue = append(queue, children...)
		}
	}

	// Remove the target key and all descendants (if cascade)
	s.RemoveSigningKey(order.GetKeyId())

	for _, id := range cascaded {
		s.RemoveSigningKey(id)
	}

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_RevokeSigningKey{
			RevokeSigningKey: &commonpb.RevokedSigningKeyLog{
				KeyId:          order.GetKeyId(),
				CascadedKeyIds: cascaded,
			},
		},
	}, nil
}

func processSetSigningConfig(order *raftcmdpb.SetSigningConfigOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	ctx.Scope.SetRequireSignatures(order.GetRequireSignatures())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SetSigningConfig{
			SetSigningConfig: &commonpb.SetSigningConfigLog{
				RequireSignatures: order.GetRequireSignatures(),
			},
		},
	}, nil
}
