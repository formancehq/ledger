package servicepb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestMetadataIntegers_BulkDecoders(t *testing.T) {
	t.Parallel()
	for _, action := range []string{"CREATE_TRANSACTION", "ADD_METADATA", "REVERT_TRANSACTION"} {
		for _, tc := range []struct {
			token string
			want  any
		}{
			{"9007199254740993", uint64(9007199254740993)},
			{"-9007199254740993", int64(-9007199254740993)},
			{"9223372036854775807", uint64(9223372036854775807)},
			{"-9223372036854775808", int64(-9223372036854775808)},
			{"1.5", nil}, {"9007199254740993.1", nil},
		} {
			t.Run(action+"/"+tc.token, func(t *testing.T) {
				t.Parallel()
				data := fmt.Sprintf(`{"id":1,"targetType":"ACCOUNT","targetId":"users:001","metadata":{"count":%s}}`, tc.token)
				for _, decode := range []func() (*LedgerAction, error){
					func() (*LedgerAction, error) {
						var element BulkElement
						err := json.Unmarshal([]byte(fmt.Sprintf(`{"action":%q,"data":%s}`, action, data)), &element)

						return element.Action, err
					},
					func() (*LedgerAction, error) {
						var element LedgerAction
						err := json.Unmarshal([]byte(fmt.Sprintf(`{"action":%q,"data":%s}`, action, data)), &element)

						return &element, err
					},
				} {
					got, err := decode()
					if tc.want == nil {
						require.Error(t, err)

						continue
					}
					require.NoError(t, err)
					var md map[string]*commonpb.MetadataValue
					switch action {
					case "CREATE_TRANSACTION":
						md = got.GetCreateTransaction().GetMetadata()
					case "ADD_METADATA":
						md = got.GetAddMetadata().GetMetadata()
					case "REVERT_TRANSACTION":
						md = got.GetRevertTransaction().GetMetadata()
					}
					require.Equal(t, tc.want, commonpb.MetadataValueToAny(md["count"]))
				}
			})
		}
	}
}
