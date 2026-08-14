package cmdutil

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// listedIndex mirrors the struct the Kubernetes operator uses to parse
// `ledgerctl indexes list --json`. Duplicated on purpose: misc/operator is a
// separate Go module with no require/replace on the root module and there is no
// go.work, so this test cannot import the real type — and a package existing
// only to share a test fixture is not wanted either. Keep in sync with
// misc/operator/internal/controller/ledger_indexes.go:186.
//
// WHY THIS TEST EXISTS: marshalJSON prefers a custom MarshalJSON over protojson.
// Adding one to commonpb.Index would reshape this output, and the operator's
// parse would silently yield an empty index set (encoding/json ignores unknown
// fields) or error outright if the enums became numeric. An empty set makes the
// drift reconciler treat every desired index as missing, adopt pre-existing
// indexes as operator-owned, and orphan indexes it should drop. Root
// `go build ./...` never compiles misc/operator, and the operator's own tests
// feed literal JSON rather than real CLI output, so nothing else catches it.
type listedIndex struct {
	ID struct {
		TxBuiltin      string `json:"txBuiltin"`
		AccountBuiltin string `json:"accountBuiltin"`
		LogBuiltin     string `json:"logBuiltin"`
		Metadata       *struct {
			Target string `json:"target"`
			Key    string `json:"key"`
		} `json:"metadata"`
	} `json:"id"`
	Ledger string `json:"ledger"`
}

func TestCLIIndexListShapeMatchesOperatorParser(t *testing.T) {
	t.Parallel()

	src := []*commonpb.Index{
		{Id: &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{
			TxBuiltin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE,
		}}, Ledger: "L"},
		{Id: &commonpb.IndexID{Kind: &commonpb.IndexID_AccountBuiltin{
			AccountBuiltin: commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET,
		}}, Ledger: "L"},
		{Id: &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{
			Metadata: &commonpb.MetadataIndexID{
				Target: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:    "color",
			},
		}}, Ledger: "L"},
	}

	// marshalJSON is the single encoder EncodeStructured uses for --json,
	// --yaml and --result-file alike, so asserting on it covers all three.
	raw, err := marshalJSON(src)
	require.NoError(t, err)

	var got []listedIndex
	require.NoError(t, json.Unmarshal(raw, &got), "operator parser must accept CLI output")
	require.Len(t, got, 3)

	// Enum NAME strings, compared literally by the operator.
	//
	// TX_BUILTIN_INDEX_REFERENCE and TARGET_TYPE_ACCOUNT are both enum value 0,
	// and protoJSONOpts sets EmitUnpopulated, so protojson emits them here. The
	// HTTP surface was silently dropping exactly those fields, which is why the
	// operator kept working while the REST API was wrong (EN-1791).
	require.Equal(t, "TX_BUILTIN_INDEX_REFERENCE", got[0].ID.TxBuiltin)
	require.Equal(t, "ACCT_BUILTIN_INDEX_ASSET", got[1].ID.AccountBuiltin)
	require.NotNil(t, got[2].ID.Metadata)
	require.Equal(t, "TARGET_TYPE_ACCOUNT", got[2].ID.Metadata.Target)
	require.Equal(t, "color", got[2].ID.Metadata.Key)
	require.Equal(t, "L", got[0].Ledger)
}
