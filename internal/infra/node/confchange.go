package node

import (
	"encoding/json"
	"fmt"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// ConfChangeContext carries peer addresses alongside a Raft ConfChange so that
// all nodes (not just the leader) can learn the new peer's addresses when the
// ConfChange is committed.
type ConfChangeContext struct {
	RaftAddress    string `json:"raftAddress"`
	ServiceAddress string `json:"serviceAddress"`
}

// MarshalConfChangeContext serialises a ConfChangeContext to JSON bytes
// suitable for embedding in ConfChange.Context.
func MarshalConfChangeContext(ctx ConfChangeContext) ([]byte, error) {
	data, err := json.Marshal(ctx)
	if err != nil {
		return nil, fmt.Errorf("marshaling ConfChangeContext: %w", err)
	}

	return data, nil
}

// UnmarshalConfChangeContext deserialises a ConfChangeContext from JSON bytes
// that were stored in ConfChange.Context.
func UnmarshalConfChangeContext(data []byte) (ConfChangeContext, error) {
	var ctx ConfChangeContext

	err := json.Unmarshal(data, &ctx)
	if err != nil {
		return ConfChangeContext{}, fmt.Errorf("unmarshaling ConfChangeContext: %w", err)
	}

	return ctx, nil
}

// unmarshalConfChangeV2 decodes a ConfChange or ConfChangeV2 entry into a
// unified ConfChangeV2. Returns false for entries that are not conf-changes.
func unmarshalConfChangeV2(entry raftpb.Entry) (raftpb.ConfChangeV2, bool, error) {
	var cc raftpb.ConfChangeV2

	switch entry.Type {
	case raftpb.EntryConfChange:
		var ccV1 raftpb.ConfChange

		err := ccV1.Unmarshal(entry.Data)
		if err != nil {
			return cc, false, fmt.Errorf("unmarshaling ConfChange: %w", err)
		}

		cc = ccV1.AsV2()
		// V1->V2 conversion does not copy Context; propagate it manually.
		cc.Context = ccV1.Context
	case raftpb.EntryConfChangeV2:
		err := cc.Unmarshal(entry.Data)
		if err != nil {
			return cc, false, fmt.Errorf("unmarshaling ConfChangeV2: %w", err)
		}
	default:
		return cc, false, nil
	}

	return cc, true, nil
}
