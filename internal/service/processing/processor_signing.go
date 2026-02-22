package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processRegisterSigningKey(order *raftcmdpb.RegisterSigningKeyOrder, s Store) (*commonpb.LogPayload, error) {
	s.AddSigningKey(order.KeyId, order.PublicKey, order.ParentKeyId)
	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_RegisterSigningKey{
			RegisterSigningKey: &commonpb.RegisterSigningKeyLog{
				KeyId:       order.KeyId,
				PublicKey:   order.PublicKey,
				ParentKeyId: order.ParentKeyId,
			},
		},
	}, nil
}

func (p *RequestProcessor) processRevokeSigningKey(order *raftcmdpb.RevokeSigningKeyOrder, s Store) (*commonpb.LogPayload, error) {
	var cascaded []string
	if order.Cascade {
		// BFS to find all descendants for cascade revocation
		queue := []string{order.KeyId}
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			children := s.GetSigningKeyChildren(current)
			cascaded = append(cascaded, children...)
			queue = append(queue, children...)
		}
	}

	// Remove the target key and all descendants (if cascade)
	s.RemoveSigningKey(order.KeyId)
	for _, id := range cascaded {
		s.RemoveSigningKey(id)
	}

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_RevokeSigningKey{
			RevokeSigningKey: &commonpb.RevokeSigningKeyLog{
				KeyId:          order.KeyId,
				CascadedKeyIds: cascaded,
			},
		},
	}, nil
}

func (p *RequestProcessor) processSetSigningConfig(order *raftcmdpb.SetSigningConfigOrder, s Store) (*commonpb.LogPayload, error) {
	s.SetRequireSignatures(order.RequireSignatures)
	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SetSigningConfig{
			SetSigningConfig: &commonpb.SetSigningConfigLog{
				RequireSignatures: order.RequireSignatures,
			},
		},
	}, nil
}
