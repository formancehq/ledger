package commonpb

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpdatedPreparedQueryLogJSONOptionalTargets(t *testing.T) {
	t.Parallel()

	accounts := QueryTarget_QUERY_TARGET_ACCOUNTS
	for _, tc := range []struct {
		name           string
		previousTarget *QueryTarget
		newTarget      *QueryTarget
		want           string
	}{
		{"historical", nil, nil, `{"sequence":1,"payload":{"updatedPreparedQuery":{"ledger":"ledger","name":"query"}}}`},
		{"explicit accounts", &accounts, &accounts, `{"sequence":1,"payload":{"updatedPreparedQuery":{"ledger":"ledger","name":"query","previousTarget":"QUERY_TARGET_ACCOUNTS","newTarget":"QUERY_TARGET_ACCOUNTS"}}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			log := &Log{Sequence: 1, Payload: &LogPayload{Type: &LogPayload_UpdatedPreparedQuery{
				UpdatedPreparedQuery: &UpdatedPreparedQueryLog{
					Ledger: "ledger", Name: "query", PreviousTarget: tc.previousTarget, NewTarget: tc.newTarget,
				},
			}}}
			data, err := json.Marshal(log)
			require.NoError(t, err)
			require.JSONEq(t, tc.want, string(data))
		})
	}
}
