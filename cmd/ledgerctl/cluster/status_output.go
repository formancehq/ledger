package cluster

import (
	"encoding/json"
	"strconv"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// statusOutput preserves numeric IDs understood by older operators while
// representing larger identities exactly as decimal strings in JSON and YAML.
type statusOutput struct{ state *clusterpb.ClusterState }

func statusIdentity(id uint64) json.RawMessage {
	value := strconv.FormatUint(id, 10)
	if id > uint64(^uint32(0)) {
		value = `"` + value + `"`
	}

	return json.RawMessage(value)
}

func (s statusOutput) MarshalJSON() ([]byte, error) {
	data, err := (protojson.MarshalOptions{EmitUnpopulated: true}).Marshal(s.state)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	fields["leader"] = statusIdentity(s.state.LeaderID())
	fields["localNode"] = statusIdentity(s.state.LocalNodeID())
	delete(fields, "leader64")
	delete(fields, "localNode64")
	var peers []map[string]json.RawMessage
	if err := json.Unmarshal(fields["nodes"], &peers); err != nil {
		return nil, err
	}
	for i, peer := range s.state.GetNodes() {
		peers[i]["id"] = statusIdentity(peer.NodeID())
		delete(peers[i], "id64")
	}
	fields["nodes"], err = json.Marshal(peers)
	if err != nil {
		return nil, err
	}

	return json.Marshal(fields)
}
