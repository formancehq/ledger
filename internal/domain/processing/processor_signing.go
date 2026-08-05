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
		// BFS over the child relation to collect every descendant, guarded by a
		// visited set.
		//
		// The visited set is what makes the walk TERMINATE, and it is load-bearing
		// rather than defensive: nothing validates that the parent graph stays
		// acyclic. processRegisterSigningKey shape-checks the two key IDs and
		// nothing else — it never verifies that the parent exists — and
		// registration is an upsert, so "register a under b" followed by
		// "register b under a" is accepted end to end. Without the set a cascade
		// revoke of either alternates between them forever, with `cascaded`
		// growing unbounded. That hang would sit in the Raft apply path, so it
		// would wedge every replica at once and, the order being committed,
		// replay on every restart.
		//
		// It also dedups `cascaded`, and hence RevokedSigningKeyLog.cascaded_key_ids:
		// GetSigningKeyChildren appends the key of EVERY pending addition whose
		// parent matches, so registering the same child twice under one parent in
		// one proposal otherwise reports it twice.
		//
		// Seeded with the revoke target so it can never appear in `cascaded` —
		// RemoveSigningKey below already covers it, and an acyclic walk never
		// yielded it either, so the log payload is unchanged for a well-formed
		// graph.
		queue := []string{order.GetKeyId()}
		visited := map[string]struct{}{order.GetKeyId(): {}}

		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]

			for _, child := range s.GetSigningKeyChildren(current) {
				if _, already := visited[child]; already {
					continue
				}

				visited[child] = struct{}{}
				cascaded = append(cascaded, child)
				queue = append(queue, child)
			}
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
