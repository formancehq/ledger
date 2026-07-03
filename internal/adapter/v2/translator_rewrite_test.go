package v2

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// dropWorker drops the ":worker:<n>" lock-avoidance segment.
func dropWorkerRewriter(t *testing.T) *AddressRewriter {
	t.Helper()

	r, err := NewAddressRewriter([]*commonpb.AddressRewriteRule{rule(`(:worker:\d+)`, "")})
	require.NoError(t, err)

	return r
}

func TestTranslateBatch_Rewrite_CreatedTransaction(t *testing.T) {
	t.Parallel()

	v2Logs := []V2Log{{
		ID:   1,
		Type: "NEW_TRANSACTION",
		Data: mustMarshal(t, V2NewTransactionData{
			Transaction: V2Transaction{
				ID: 0,
				Postings: []V2Posting{{
					Source:      "world",
					Destination: "payments:acme:worker:001:main",
					Amount:      "100",
					Asset:       "USD/2",
				}},
			},
			AccountMetadata: map[string]map[string]string{
				"payments:acme:worker:001:main": {"kind": "main"},
			},
		}),
	}}

	orders, _, _, err := TranslateBatch("default", v2Logs, 1, 0, dropWorkerRewriter(t))
	require.NoError(t, err)
	require.Len(t, orders, 1)

	ct := orders[0].GetLedgerScoped().GetMirrorIngest().GetEntry().GetCreatedTransaction()
	require.NotNil(t, ct)
	require.Equal(t, "world", ct.GetPostings()[0].GetSource())
	require.Equal(t, "payments:acme:main", ct.GetPostings()[0].GetDestination())

	require.Contains(t, ct.GetAccountMetadata(), "payments:acme:main")
	require.NotContains(t, ct.GetAccountMetadata(), "payments:acme:worker:001:main")
	require.Equal(t, "main",
		ct.GetAccountMetadata()["payments:acme:main"].GetValues()["kind"].GetStringValue())
}

func TestTranslateBatch_Rewrite_RevertedTransaction(t *testing.T) {
	t.Parallel()

	v2Logs := []V2Log{{
		ID:   1,
		Type: "REVERTED_TRANSACTION",
		Data: mustMarshal(t, V2RevertedTransactionData{
			RevertedTransactionID: 0,
			RevertTransaction: V2Transaction{
				ID: 1,
				Postings: []V2Posting{{
					Source:      "payments:acme:worker:001:main",
					Destination: "world",
					Amount:      "100",
					Asset:       "USD/2",
				}},
			},
		}),
	}}

	orders, _, _, err := TranslateBatch("default", v2Logs, 1, 1, dropWorkerRewriter(t))
	require.NoError(t, err)
	require.Len(t, orders, 1)

	rt := orders[0].GetLedgerScoped().GetMirrorIngest().GetEntry().GetRevertedTransaction()
	require.NotNil(t, rt)
	require.Equal(t, "payments:acme:main", rt.GetReversePostings()[0].GetSource())
	require.Equal(t, "world", rt.GetReversePostings()[0].GetDestination())
}

func TestTranslateBatch_Rewrite_MetadataTargets(t *testing.T) {
	t.Parallel()

	v2Logs := []V2Log{
		{
			ID:   1,
			Type: "SET_METADATA",
			Data: mustMarshal(t, V2SetMetadataData{
				TargetType: "ACCOUNT",
				TargetID:   json.RawMessage(`"payments:acme:worker:001:main"`),
				Metadata:   map[string]string{"k": "v"},
			}),
		},
		{
			ID:   2,
			Type: "DELETE_METADATA",
			Data: mustMarshal(t, V2DeleteMetadataData{
				TargetType: "ACCOUNT",
				TargetID:   json.RawMessage(`"payments:acme:worker:002:main"`),
				Key:        "k",
			}),
		},
		{
			ID:   3,
			Type: "SET_METADATA",
			Data: mustMarshal(t, V2SetMetadataData{
				TargetType: "TRANSACTION",
				TargetID:   json.RawMessage(`42`),
				Metadata:   map[string]string{"k": "v"},
			}),
		},
	}

	orders, _, _, err := TranslateBatch("default", v2Logs, 1, 1, dropWorkerRewriter(t))
	require.NoError(t, err)
	require.Len(t, orders, 3)

	saved := orders[0].GetLedgerScoped().GetMirrorIngest().GetEntry().GetSavedMetadata()
	require.Equal(t, "payments:acme:main", saved.GetTarget().GetAccount().GetAddr())

	deleted := orders[1].GetLedgerScoped().GetMirrorIngest().GetEntry().GetDeletedMetadata()
	require.Equal(t, "payments:acme:main", deleted.GetTarget().GetAccount().GetAddr())

	// Transaction targets are untouched.
	tx := orders[2].GetLedgerScoped().GetMirrorIngest().GetEntry().GetSavedMetadata()
	require.Equal(t, uint64(42), tx.GetTarget().GetTransactionId())
}

func TestTranslateBatch_Rewrite_AccountMetadataCollisionMerges(t *testing.T) {
	t.Parallel()

	// worker:001 and worker:002 collapse onto the same account.
	v2Logs := []V2Log{{
		ID:   1,
		Type: "NEW_TRANSACTION",
		Data: mustMarshal(t, V2NewTransactionData{
			Transaction: V2Transaction{ID: 0},
			AccountMetadata: map[string]map[string]string{
				"payments:acme:worker:001:main": {"kind": "main", "shard": "001"},
				"payments:acme:worker:002:main": {"shard": "002"},
			},
		}),
	}}

	orders, _, _, err := TranslateBatch("default", v2Logs, 1, 0, dropWorkerRewriter(t))
	require.NoError(t, err)

	ct := orders[0].GetLedgerScoped().GetMirrorIngest().GetEntry().GetCreatedTransaction()
	merged := ct.GetAccountMetadata()["payments:acme:main"].GetValues()
	require.Equal(t, "main", merged["kind"].GetStringValue())
	// On conflict, the lexicographically-smallest source (worker:001) wins.
	require.Equal(t, "001", merged["shard"].GetStringValue())
}

func TestTranslateBatch_Rewrite_InvalidResultErrors(t *testing.T) {
	t.Parallel()

	r, err := NewAddressRewriter([]*commonpb.AddressRewriteRule{rule(`.+`, "")})
	require.NoError(t, err)

	v2Logs := []V2Log{{
		ID:   1,
		Type: "NEW_TRANSACTION",
		Data: mustMarshal(t, V2NewTransactionData{
			Transaction: V2Transaction{
				ID: 0,
				Postings: []V2Posting{{
					Source:      "world",
					Destination: "payments:acme:main",
					Amount:      "100",
					Asset:       "USD/2",
				}},
			},
		}),
	}}

	_, _, _, err = TranslateBatch("default", v2Logs, 1, 0, r)
	require.Error(t, err)
}
