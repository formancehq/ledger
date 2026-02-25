package domain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSinkConfigKey_Bytes(t *testing.T) {
	t.Parallel()

	k := SinkConfigKey{Name: "my-sink"}
	require.Equal(t, []byte("my-sink"), k.Bytes())
}
