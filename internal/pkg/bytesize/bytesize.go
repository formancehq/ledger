package bytesize

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

// ByteSize represents a size in bytes. It supports Kubernetes-style quantity
// suffixes for both binary (Ki, Mi, Gi, Ti, Pi, Ei) and decimal (k, M, G, T,
// P, E) units, as well as plain integer values (interpreted as bytes).
type ByteSize int64

const (
	Ki ByteSize = 1024
	Mi ByteSize = 1024 * Ki
	Gi ByteSize = 1024 * Mi
	Ti ByteSize = 1024 * Gi
	Pi ByteSize = 1024 * Ti
	Ei ByteSize = 1024 * Pi

	K ByteSize = 1000
	M ByteSize = 1000 * K
	G ByteSize = 1000 * M
	T ByteSize = 1000 * G
	P ByteSize = 1000 * T
	E ByteSize = 1000 * P
)

var binarySuffixes = []struct {
	suffix string
	mult   ByteSize
}{
	// Longer suffixes first to avoid ambiguous prefix matches.
	{"Ei", Ei},
	{"Pi", Pi},
	{"Ti", Ti},
	{"Gi", Gi},
	{"Mi", Mi},
	{"Ki", Ki},
}

var decimalSuffixes = []struct {
	suffix string
	mult   ByteSize
}{
	{"E", E},
	{"P", P},
	{"T", T},
	{"G", G},
	{"M", M},
	{"k", K},
}

// Parse parses a Kubernetes-style byte size string.
//
// Accepted formats:
//   - Plain integer: "1048576" (bytes)
//   - Binary suffix: "1Ki", "256Mi", "2Gi"
//   - Decimal suffix: "1k", "10M", "1G"
func Parse(s string) (ByteSize, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty byte size string")
	}

	for _, entry := range binarySuffixes {
		if before, ok := strings.CutSuffix(s, entry.suffix); ok {
			num := before

			v, err := strconv.ParseInt(num, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid byte size %q: %w", s, err)
			}

			return ByteSize(v) * entry.mult, nil
		}
	}

	for _, entry := range decimalSuffixes {
		if before, ok := strings.CutSuffix(s, entry.suffix); ok {
			num := before

			v, err := strconv.ParseInt(num, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid byte size %q: %w", s, err)
			}

			return ByteSize(v) * entry.mult, nil
		}
	}

	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q: must be a number with optional suffix (Ki, Mi, Gi, Ti, Pi, Ei, k, M, G, T, P, E)", s)
	}

	return ByteSize(v), nil
}

// String returns the most human-readable representation of the byte size,
// preferring binary suffixes when the value is an exact multiple.
func (b ByteSize) String() string {
	if b == 0 {
		return "0"
	}

	for _, entry := range binarySuffixes {
		if b%entry.mult == 0 {
			return strconv.FormatInt(int64(b/entry.mult), 10) + entry.suffix
		}
	}

	return strconv.FormatInt(int64(b), 10)
}

// Int returns the byte size as an int.
func (b ByteSize) Int() int { return int(b) }

// Int64 returns the byte size as an int64.
func (b ByteSize) Int64() int64 { return int64(b) }

// Uint64 returns the byte size as a uint64.
func (b ByteSize) Uint64() uint64 { return uint64(b) }

// byteSizeValue implements pflag.Value for ByteSize flags.
type byteSizeValue struct {
	val *ByteSize
}

func (v *byteSizeValue) String() string {
	if v.val == nil {
		return "0"
	}

	return v.val.String()
}

func (v *byteSizeValue) Set(s string) error {
	parsed, err := Parse(s)
	if err != nil {
		return err
	}

	*v.val = parsed

	return nil
}

func (v *byteSizeValue) Type() string { return "ByteSize" }

// ByteSizeVar registers a ByteSize flag on the given command.
func ByteSizeVar(cmd *cobra.Command, p *ByteSize, name string, value ByteSize, usage string) {
	*p = value
	cmd.Flags().Var(&byteSizeValue{val: p}, name, usage)
}

// Get retrieves a ByteSize flag value by name from the command.
// It returns the zero value if the flag was not set.
func Get(cmd *cobra.Command, name string) ByteSize {
	f := cmd.Flags().Lookup(name)
	if f == nil {
		return 0
	}

	if bsv, ok := f.Value.(*byteSizeValue); ok {
		return *bsv.val
	}

	return 0
}
