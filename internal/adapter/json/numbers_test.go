package json

import (
	stdjson "encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnmarshalUseNumber(t *testing.T) {
	t.Parallel()
	for _, streaming := range []bool{false, true} {
		t.Run(map[bool]string{false: "bytes", true: "reader"}[streaming], func(t *testing.T) {
			t.Parallel()
			body := `{"value":9007199254740993,"typed":9223372036854775807}`
			var exact, ordinary struct {
				Value any
				Typed int64
			}
			if streaming {
				require.NoError(t, UnmarshalReadUseNumber(strings.NewReader(body), &exact))
				require.NoError(t, UnmarshalRead(strings.NewReader(body), &ordinary))
			} else {
				require.NoError(t, UnmarshalUseNumber([]byte(body), &exact))
				require.NoError(t, Unmarshal([]byte(body), &ordinary))
			}
			require.Equal(t, stdjson.Number("9007199254740993"), exact.Value)
			require.IsType(t, float64(0), ordinary.Value)
			require.Equal(t, int64(9223372036854775807), exact.Typed)
			require.Equal(t, exact.Typed, ordinary.Typed)
		})
	}
}
