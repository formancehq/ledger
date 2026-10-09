package json

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type failingJSONWriter struct {
	buffer bytes.Buffer
	failAt int
	calls  int
	err    error
}

func (w *failingJSONWriter) Write(data []byte) (int, error) {
	w.calls++
	if w.calls == w.failAt {
		return 0, w.err
	}

	return w.buffer.Write(data)
}

func TestMarshalWriteWithOptionsWriterFailure(t *testing.T) {
	failure := errors.New("writer failed")
	for _, failAt := range []int{1, 2} {
		writer := &failingJSONWriter{failAt: failAt, err: failure}
		err := MarshalWriteWithOptions(writer, struct {
			Amount int `json:"amount"`
		}{Amount: 42})
		require.ErrorIs(t, err, failure)
		require.Equal(t, failAt, writer.calls, "no extra writes after an encoding or newline failure")
		if failAt == 1 {
			require.Empty(t, writer.buffer.String())
		} else {
			require.Equal(t, `{"amount":42}`, writer.buffer.String())
		}
	}
}

func TestMarshalWriteWithOptionsEncodeFailure(t *testing.T) {
	writer := &bytes.Buffer{}
	err := MarshalWriteWithOptions(writer, func() {})
	require.Error(t, err)
	require.False(t, bytes.HasSuffix(writer.Bytes(), []byte("\n")), "failed encoding must not append the success newline")
	// The same invalid value must also fail before a checked response writes it.
	_, err = MarshalWithOptions(func() {})
	require.Error(t, err)
}
