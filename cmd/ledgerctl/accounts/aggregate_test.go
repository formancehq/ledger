package accounts

import (
	"strings"
	"testing"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

func TestAggregateVolumesTable(t *testing.T) {
	t.Parallel()

	scale := func(u uint8) *uint8 { return &u }

	vol := func(asset string, input, output uint64) *commonpb.AggregatedVolume {
		return &commonpb.AggregatedVolume{
			Asset:  asset,
			Input:  commonpb.NewUint256FromUint64(input),
			Output: commonpb.NewUint256FromUint64(output),
		}
	}

	t.Run("with --rescale, rows are re-expressed at the requested scale", func(t *testing.T) {
		t.Parallel()

		got, err := aggregateVolumesTable([]*commonpb.AggregatedVolume{vol("USD/3", 69129, 0)}, scale(0))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		want := []string{"USD", "", "69.129", "0.000", "+69.129"}
		if len(got) != 2 || strings.Join(got[1], "|") != strings.Join(want, "|") {
			t.Fatalf("expected header + %v, got %v", want, got)
		}
	})

	t.Run("without --rescale, an invalid asset is rendered raw", func(t *testing.T) {
		t.Parallel()

		got, err := aggregateVolumesTable([]*commonpb.AggregatedVolume{vol("USD/x", 1, 0)}, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(got) != 2 || got[1][0] != "USD/x" {
			t.Fatalf("expected the raw asset row, got %v", got)
		}
	})

	// ParseAssetPrecision alone would read these as precision 0 and render the
	// amount in the wrong unit.
	for _, asset := range []string{"USD/x", "USD/256"} {
		t.Run("with --rescale, invalid asset "+asset+" fails", func(t *testing.T) {
			t.Parallel()

			got, err := aggregateVolumesTable([]*commonpb.AggregatedVolume{
				vol("EUR/2", 250, 100),
				vol(asset, 1, 0),
			}, scale(0))
			if err == nil {
				t.Fatalf("expected an invariant error, got %v", got)
			}

			if !strings.Contains(err.Error(), `"`+asset+`"`) {
				t.Errorf("expected the error to name the offending asset, got %v", err)
			}
		})
	}
}
