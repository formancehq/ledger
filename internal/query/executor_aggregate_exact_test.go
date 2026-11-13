package query_test

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// A prepared aggregate over buckets that each fit in uint256 serves the exact
// ledger-wide total on both the unfiltered and the filtered scan.
func TestExecute_AggregateTotalsAbove2Pow256(t *testing.T) {
	t.Parallel()

	maxVolume := new(uint256.Int).SetAllOne()
	want := new(big.Int).Mul(maxVolume.ToBig(), big.NewInt(2))

	for _, filter := range []*commonpb.QueryFilter{nil, usersPrefixFilter()} {
		t.Run(fmt.Sprintf("filtered=%v", filter != nil), func(t *testing.T) {
			t.Parallel()

			f := newReverseFixture(t, filter, "account:a", "account:b")
			batch := f.store.OpenWriteSession()
			for _, account := range []string{"account:a", "account:b"} {
				_, err := f.attrs.Volume.Set(batch, domain.NewVolumeKey("l", account, "USD/2", "").Bytes(), &raftcmdpb.VolumePair{
					Input:  commonpb.NewUint256(maxVolume),
					Output: commonpb.NewUint256FromUint64(1),
				})
				require.NoError(t, err)
			}
			require.NoError(t, batch.Commit())

			resp, err := f.execute(&servicepb.ExecutePreparedQueryRequest{
				Mode: commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES,
				Parameters: map[string]*commonpb.ParameterValue{
					"prefix": {Value: &commonpb.ParameterValue_StringValue{StringValue: "account:"}},
				},
			})
			require.NoError(t, err)
			volumes := resp.GetAggregate().GetVolumes()
			require.Len(t, volumes, 1)
			input, output, err := volumes[0].Totals()
			require.NoError(t, err)
			require.Zero(t, want.Cmp(input), "got %s", input)
			require.Zero(t, big.NewInt(2).Cmp(output))
		})
	}
}
