package json

import (
	stdjson "encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/bytedance/sonic"
)

var numberDecoder = sonic.Config{UseNumber: true}.Froze()

// UnmarshalUseNumber preserves numeric tokens as encoding/json.Number in
// interface values. Use it only where the consumer explicitly handles Number;
// the shared Unmarshal default remains unchanged.
func UnmarshalUseNumber(data []byte, v any) error {
	return numberDecoder.Unmarshal(data, v)
}

// UnmarshalReadUseNumber is UnmarshalRead with exact numeric tokens in interface
// values. Typed fields and custom UnmarshalJSON methods retain their own rules.
func UnmarshalReadUseNumber(r io.Reader, v any) error {
	decoder := sonic.ConfigStd.NewDecoder(r)
	decoder.UseNumber()

	return decoder.Decode(v)
}

// Errors returned by ExactInteger.
var (
	// ErrInvalidNumber means the input is not a JSON number token.
	ErrInvalidNumber = errors.New("invalid JSON number")
	// ErrNotInteger means the number has a non-zero fractional part.
	ErrNotInteger = errors.New("number is not an integer")
	// ErrIntegerOutOfRange means the integer does not fit int64 (negative) or
	// uint64 (non-negative).
	ErrIntegerOutOfRange = errors.New("integer is out of range")
)

// ExactInteger returns the exact integer value of the JSON number token s as a
// magnitude and a sign. Integral decimal and exponent spellings such as 1.0,
// 1e3 or 10e-1 are accepted. A negative value fits int64 (magnitude at most
// 2^63); a non-negative value fits uint64. Zero is never negative.
//
// The token is normalized lexically before any integer conversion, so work and
// allocation are linear in len(s) and never depend on an untrusted exponent:
// the final integer has at most 20 digits. A non-zero value whose decimal
// shift leaves a fractional digit returns ErrNotInteger, including extreme
// negative exponents such as 1e-1000000000.
func ExactInteger(s string) (magnitude uint64, negative bool, err error) {
	if s == "" || (s[0] != '-' && (s[0] < '0' || s[0] > '9')) ||
		strings.TrimSpace(s) != s || !stdjson.Valid([]byte(s)) {
		return 0, false, ErrInvalidNumber
	}

	negative = s[0] == '-'
	mantissa := strings.TrimPrefix(s, "-")
	exponentText := "0"
	if index := strings.IndexAny(mantissa, "eE"); index >= 0 {
		exponentText = mantissa[index+1:]
		mantissa = mantissa[:index]
	}

	fractionDigits := 0
	if index := strings.IndexByte(mantissa, '.'); index >= 0 {
		fractionDigits = len(mantissa) - index - 1
		mantissa = mantissa[:index] + mantissa[index+1:]
	}
	digits := strings.TrimLeft(mantissa, "0")
	if digits == "" {
		return 0, false, nil
	}

	exponent, err := strconv.ParseInt(exponentText, 10, 64)
	// An exponent beyond the input length cannot be canceled by the decimal
	// point or trailing zeros. Reject it before computing the decimal shift.
	limit := int64(len(s)) + 20
	if err != nil || exponent > limit || exponent < -limit {
		if strings.HasPrefix(exponentText, "-") {
			return 0, negative, ErrNotInteger
		}

		return 0, negative, ErrIntegerOutOfRange
	}

	shift := exponent - int64(fractionDigits)
	if shift < 0 {
		remove := -shift
		trailingZeros := len(digits) - len(strings.TrimRight(digits, "0"))
		if remove > int64(trailingZeros) {
			return 0, negative, ErrNotInteger
		}
		digits = digits[:len(digits)-int(remove)]
	} else {
		if int64(len(digits))+shift > 20 {
			return 0, negative, ErrIntegerOutOfRange
		}
		digits += strings.Repeat("0", int(shift))
	}

	magnitude, err = strconv.ParseUint(digits, 10, 64)
	if err != nil || (negative && magnitude > uint64(1)<<63) {
		return 0, negative, ErrIntegerOutOfRange
	}

	return magnitude, negative, nil
}
