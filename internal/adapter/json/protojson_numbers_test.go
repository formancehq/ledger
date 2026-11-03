package json_test

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	ledgerjson "github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestMarshalMessageJSONConvertsNested64BitScalars(t *testing.T) {
	t.Parallel()

	msg := &clusterpb.ClusterState{
		Leader:    math.MaxUint64,
		LocalNode: 1,
		Nodes:     []*clusterpb.NodeInfo{{Id: 1}},
	}

	raw, err := ledgerjson.MarshalMessageJSONWithOptions(msg, ledgerjson.MarshalOptions{})
	require.NoError(t, err)

	var object map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &object))
	require.Equal(t, "18446744073709551615", string(object["leader"]))
	require.Equal(t, "1", string(object["localNode"]))

	var nodes []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(object["nodes"], &nodes))
	require.Equal(t, "1", string(nodes[0]["id"]))
}

func TestMarshalMessageJSONPreservesDigitStrings(t *testing.T) {
	t.Parallel()

	msg := &clusterpb.ClusterState{State: "123"}
	raw, err := ledgerjson.MarshalMessageJSONWithOptions(msg, ledgerjson.MarshalOptions{EmitUnpopulated: true})
	require.NoError(t, err)

	var object map[string]any
	require.NoError(t, json.Unmarshal(raw, &object))
	require.Equal(t, "123", object["state"])
}

func TestMarshalMessageJSONUsesPublicProjection(t *testing.T) {
	t.Parallel()

	msg := &commonpb.Transaction{Id: math.MaxUint64}
	raw, err := json.Marshal(msg)
	require.NoError(t, err)

	var object map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &object))
	require.Equal(t, "18446744073709551615", string(object["id"]))
	_, hasProtoField := object["postings"]
	require.True(t, hasProtoField, "the public Transaction projection must be used")
}
