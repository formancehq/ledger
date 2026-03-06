package json

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarshal(t *testing.T) {
	t.Parallel()

	data, err := Marshal(map[string]string{"key": "value"})
	require.NoError(t, err)
	require.JSONEq(t, `{"key":"value"}`, string(data))
}

func TestMarshal_Nil(t *testing.T) {
	t.Parallel()

	data, err := Marshal(nil)
	require.NoError(t, err)
	require.Equal(t, "null", string(data))
}

func TestUnmarshal(t *testing.T) {
	t.Parallel()

	var out map[string]string

	err := Unmarshal([]byte(`{"key":"value"}`), &out)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"key": "value"}, out)
}

func TestUnmarshal_InvalidJSON(t *testing.T) {
	t.Parallel()

	var out map[string]string

	err := Unmarshal([]byte(`{invalid`), &out)
	require.Error(t, err)
}

func TestMarshalWrite(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	err := MarshalWrite(&buf, map[string]int{"count": 42})
	require.NoError(t, err)
	require.JSONEq(t, `{"count":42}`, strings.TrimSpace(buf.String()))
}

func TestUnmarshalRead(t *testing.T) {
	t.Parallel()

	r := strings.NewReader(`{"name":"test"}`)

	var out map[string]string

	err := UnmarshalRead(r, &out)
	require.NoError(t, err)
	require.Equal(t, "test", out["name"])
}

func TestUnmarshalRead_InvalidJSON(t *testing.T) {
	t.Parallel()

	r := strings.NewReader(`not json`)

	var out map[string]string

	err := UnmarshalRead(r, &out)
	require.Error(t, err)
}

func TestValid(t *testing.T) {
	t.Parallel()

	require.True(t, Valid([]byte(`{"valid":true}`)))
	require.True(t, Valid([]byte(`[]`)))
	require.True(t, Valid([]byte(`"string"`)))
	require.True(t, Valid([]byte(`null`)))
	require.False(t, Valid([]byte(`{invalid`)))
	require.False(t, Valid([]byte(``)))
}

func TestRawValue_MarshalJSON(t *testing.T) {
	t.Parallel()

	rv := RawValue(`{"nested":true}`)
	data, err := rv.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, `{"nested":true}`, string(data))
}

func TestRawValue_MarshalJSON_Nil(t *testing.T) {
	t.Parallel()

	var rv RawValue

	data, err := rv.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, "null", string(data))
}

func TestRawValue_UnmarshalJSON(t *testing.T) {
	t.Parallel()

	rv := RawValue{}
	err := rv.UnmarshalJSON([]byte(`{"key":"val"}`))
	require.NoError(t, err)
	require.JSONEq(t, `{"key":"val"}`, string(rv))
}

func TestRawValue_UnmarshalJSON_NilReceiver(t *testing.T) {
	t.Parallel()

	var rv *RawValue

	err := rv.UnmarshalJSON([]byte(`{"key":"val"}`))
	require.NoError(t, err)
}

func TestRawValue_String(t *testing.T) {
	t.Parallel()

	rv := RawValue(`[1,2,3]`)
	require.Equal(t, "[1,2,3]", rv.String())
}

func TestRawValue_RoundTrip(t *testing.T) {
	t.Parallel()

	type wrapper struct {
		Data RawValue `json:"data"`
	}

	original := wrapper{Data: RawValue(`{"inner":"value"}`)}
	data, err := Marshal(original)
	require.NoError(t, err)

	var decoded wrapper

	err = Unmarshal(data, &decoded)
	require.NoError(t, err)
	require.JSONEq(t, `{"inner":"value"}`, string(decoded.Data))
}
