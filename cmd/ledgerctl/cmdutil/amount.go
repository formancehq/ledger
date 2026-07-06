package cmdutil

import (
	"math/big"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/formancehq/invariants"
)

// RescaleFlagName is the name of the global flag that re-expresses amounts at a
// chosen scale.
const RescaleFlagName = "rescale"

// RescaleTarget returns the target scale requested via --rescale, or nil when
// the flag was not given. --rescale passed without a value yields a non-nil
// pointer to 0 (rescale to whole units, dropping the precision suffix), which is
// deliberately distinct from the flag being absent (no rescaling at all). The
// scale is a uint8: an asset's precision is a single byte, so pflag rejects any
// value above 255 at parse time.
func RescaleTarget(cmd *cobra.Command) *uint8 {
	if !cmd.Flags().Changed(RescaleFlagName) {
		return nil
	}

	v, _ := cmd.Flags().GetUint8(RescaleFlagName)

	return &v
}

// Rescale re-expresses a raw integer amount, recorded at its asset's precision,
// as an amount at toScale, returning the rendered amount and the asset label
// re-suffixed to toScale (bare currency when toScale is 0).
//
//	1234 "USD/2" @ scale 0 -> "12.34",  "USD"
//	1234 "USD/2" @ scale 2 -> "1234",   "USD/2"
//	1234 "USD/2" @ scale 4 -> "123400", "USD/4"
//
// Amounts that do not parse as an integer are returned unchanged.
func Rescale(amount, asset string, toScale uint8) (string, string) {
	base, precision := invariants.ParseAssetPrecision(asset)

	n, ok := new(big.Int).SetString(amount, 10)
	if !ok {
		return amount, asset
	}

	return RescaleAmount(n, precision, toScale), AssetLabel(base, toScale)
}

// RescaleAmount renders amount — an integer recorded at fromPrecision — expressed
// at toScale. When toScale is coarser than fromPrecision the surplus digits
// become a fractional part (1234 at precision 2, scale 0 → "12.34"); when finer,
// the value is padded with zeros (1234 at precision 2, scale 4 → "123400").
func RescaleAmount(amount *big.Int, fromPrecision, toScale uint8) string {
	switch {
	case fromPrecision > toScale:
		return scaleDown(amount, fromPrecision-toScale)
	case fromPrecision < toScale:
		factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(toScale-fromPrecision)), nil)

		return new(big.Int).Mul(amount, factor).String()
	default:
		return amount.String()
	}
}

// AssetLabel reconstructs an asset string for a bare currency at the given scale:
// scale 0 yields the currency alone, otherwise "CUR/scale". It delegates to
// invariants.FormatAsset so the "CUR/N" grammar has a single implementation
// shared with the server side.
func AssetLabel(base string, scale uint8) string {
	return invariants.FormatAsset(base, scale)
}

// RawVolume holds one (asset, color) bucket's raw integer input/output amounts
// as decimal strings, as carried on the wire. The empty color is the uncolored
// bucket.
type RawVolume struct {
	Asset  string
	Color  string
	Input  string
	Output string
}

// AssetVolumes is the result of aggregating one (base currency, color) bucket
// across precisions. Input, Output and Balance (= Input - Output) are summed and
// expressed at Precision, the highest precision seen in the group; render them at
// a target scale with RescaleAmount(amount, Precision, toScale). Asset is the
// bare currency (no "/precision" suffix).
type AssetVolumes struct {
	Asset     string
	Color     string
	Precision uint8
	Input     *big.Int
	Output    *big.Int
	Balance   *big.Int
}

// AggregateVolumes groups input/output volumes by (base currency, color) and
// sums entries that share a bucket but differ in precision: "USD/4" and "USD/8"
// both collapse to "USD". Colors stay segregated — they are distinct balance
// buckets and are never summed together. Summing is done on the real (scaled)
// amounts, not the raw integers — each value is lifted to the group's highest
// precision first — so no fractional digits are lost (1.0000 + 1.00000000 →
// 2.00000000). Balance is derived as Input - Output. Results are sorted by
// (currency, color). An asset whose input or output does not parse as an integer
// is skipped — server-issued amounts are always canonical integer strings.
func AggregateVolumes(volumes []RawVolume) []AssetVolumes {
	type bucket struct {
		base  string
		color string
	}

	maxPrecision := make(map[bucket]uint8, len(volumes))

	type entry struct {
		bucket    bucket
		precision uint8
		input     *big.Int
		output    *big.Int
	}

	entries := make([]entry, 0, len(volumes))

	for _, vol := range volumes {
		base, precision := invariants.ParseAssetPrecision(vol.Asset)

		input, okIn := new(big.Int).SetString(vol.Input, 10)
		output, okOut := new(big.Int).SetString(vol.Output, 10)
		if !okIn || !okOut {
			continue
		}

		b := bucket{base: base, color: vol.Color}

		entries = append(entries, entry{bucket: b, precision: precision, input: input, output: output})

		if precision > maxPrecision[b] {
			maxPrecision[b] = precision
		}
	}

	type sums struct {
		input  *big.Int
		output *big.Int
	}

	agg := make(map[bucket]*sums, len(maxPrecision))
	order := make([]bucket, 0, len(maxPrecision))

	for _, e := range entries {
		s, seen := agg[e.bucket]
		if !seen {
			s = &sums{input: new(big.Int), output: new(big.Int)}
			agg[e.bucket] = s

			order = append(order, e.bucket)
		}

		shift := maxPrecision[e.bucket] - e.precision
		s.input.Add(s.input, lift(e.input, shift))
		s.output.Add(s.output, lift(e.output, shift))
	}

	sort.Slice(order, func(i, j int) bool {
		if order[i].base != order[j].base {
			return order[i].base < order[j].base
		}

		return order[i].color < order[j].color
	})

	result := make([]AssetVolumes, 0, len(order))

	for _, b := range order {
		s := agg[b]
		result = append(result, AssetVolumes{
			Asset:     b.base,
			Color:     b.color,
			Precision: maxPrecision[b],
			Input:     s.input,
			Output:    s.output,
			Balance:   new(big.Int).Sub(s.input, s.output),
		})
	}

	return result
}

// lift multiplies amount by 10^shift, used to express a value at a higher
// precision before summing. shift 0 returns amount unchanged.
func lift(amount *big.Int, shift uint8) *big.Int {
	if shift == 0 {
		return amount
	}

	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(shift)), nil)

	return new(big.Int).Mul(amount, factor)
}

// scaleDown renders amount / 10^precision as a fixed-point decimal string with
// exactly precision fractional digits, preserving the sign and any trailing
// zeros (so "780" stays ".780", never ".78").
func scaleDown(amount *big.Int, precision uint8) string {
	if precision == 0 {
		return amount.String()
	}

	sign := ""

	abs := amount
	if amount.Sign() < 0 {
		sign = "-"
		abs = new(big.Int).Neg(amount)
	}

	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(precision)), nil)
	quo, rem := new(big.Int).QuoRem(abs, divisor, new(big.Int))

	frac := rem.String()
	if pad := int(precision) - len(frac); pad > 0 {
		frac = strings.Repeat("0", pad) + frac
	}

	return sign + quo.String() + "." + frac
}
