package pagecursor

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	for _, c := range []Cursor{
		{},
		{Key: "42"},
		{Key: "users:1", Back: true},
		{Back: true},
	} {
		got, err := Decode(c.Encode())
		require.NoError(t, err)
		require.Equal(t, c, got)
	}
}

func TestEncodeIsBase64URLJSON(t *testing.T) {
	t.Parallel()

	raw, err := base64.RawURLEncoding.DecodeString(Cursor{Key: "42", Back: true}.Encode())
	require.NoError(t, err)
	require.JSONEq(t, `{"key":"42","back":true}`, string(raw))

	require.NotEmpty(t, Cursor{}.Encode(), "the first-page token must stay distinguishable from no link")
}

func TestDecodeEmptyIsFirstPage(t *testing.T) {
	t.Parallel()

	c, err := Decode("")
	require.NoError(t, err)
	require.Equal(t, Cursor{}, c)
}

func TestDecodeAllowsTrailingWhitespace(t *testing.T) {
	t.Parallel()

	c, err := Decode(base64.RawURLEncoding.EncodeToString([]byte(`{"key":"1"} `)))
	require.NoError(t, err)
	require.Equal(t, Cursor{Key: "1"}, c)
}

func TestDecodeRejectsMalformed(t *testing.T) {
	t.Parallel()

	enc := base64.RawURLEncoding.EncodeToString

	for name, token := range map[string]string{
		"not base64":       "!!!",
		"not json":         enc([]byte("42")),
		"unknown field":    enc([]byte(`{"after":"42"}`)),
		"wrong type":       enc([]byte(`{"key":42}`)),
		"trailing data":    enc([]byte(`{"key":"1"}{}`)),
		"trailing brace":   enc([]byte(`{"key":"1"}}`)),
		"trailing bracket": enc([]byte(`{"key":"1"}]`)),
		"null payload":     enc([]byte(`null`)),
		"array payload":    enc([]byte(`["1"]`)),
		"null key":         enc([]byte(`{"key":null}`)),
		"null back":        enc([]byte(`{"back":null}`)),
		"empty payload":    enc([]byte(` `)),
	} {
		_, err := Decode(token)
		require.ErrorIs(t, err, ErrInvalid, name)
	}
}

func TestUint64(t *testing.T) {
	t.Parallel()

	v, err := Cursor{}.Uint64()
	require.NoError(t, err)
	require.Zero(t, v)

	v, err = Cursor{Key: "18446744073709551615"}.Uint64()
	require.NoError(t, err)
	require.Equal(t, uint64(18446744073709551615), v)

	_, err = Cursor{Key: "users:1"}.Uint64()
	require.ErrorIs(t, err, ErrInvalid)
}

func TestReadReverse(t *testing.T) {
	t.Parallel()

	require.False(t, Cursor{}.ReadReverse(false))
	require.True(t, Cursor{}.ReadReverse(true))
	require.True(t, Cursor{Back: true}.ReadReverse(false))
	require.False(t, Cursor{Back: true}.ReadReverse(true))
}

func identity(s string) string { return s }

func TestPageForward(t *testing.T) {
	t.Parallel()

	t.Run("first page with more", func(t *testing.T) {
		t.Parallel()

		page, next, previous := Page(Cursor{}, []string{"1", "2", "3"}, 2, false, identity)
		require.Equal(t, []string{"1", "2"}, page)
		require.Equal(t, Cursor{Key: "2"}.Encode(), next)
		require.Empty(t, previous, "the first page has nothing before it")
	})

	t.Run("resumed last page", func(t *testing.T) {
		t.Parallel()

		page, next, previous := Page(Cursor{Key: "2"}, []string{"3", "4"}, 2, false, identity)
		require.Equal(t, []string{"3", "4"}, page)
		require.Empty(t, next)
		require.Equal(t, Cursor{Key: "3", Back: true}.Encode(), previous)
	})

	t.Run("upstream more on an exact page", func(t *testing.T) {
		t.Parallel()

		_, next, _ := Page(Cursor{}, []string{"1", "2"}, 2, true, identity)
		require.Equal(t, Cursor{Key: "2"}.Encode(), next)
	})

	t.Run("empty resumed page links to the last page", func(t *testing.T) {
		t.Parallel()

		page, next, previous := Page(Cursor{Key: "9"}, []string{}, 2, false, identity)
		require.Empty(t, page)
		require.Empty(t, next)
		require.Equal(t, Cursor{Back: true}.Encode(), previous)
	})
}

func TestPageBack(t *testing.T) {
	t.Parallel()

	t.Run("rows are returned in query order", func(t *testing.T) {
		t.Parallel()

		// Back from 5: read in the opposite order, 4 3 2 with one extra row.
		page, next, previous := Page(Cursor{Key: "5", Back: true}, []string{"4", "3", "2"}, 2, false, identity)
		require.Equal(t, []string{"3", "4"}, page)
		require.Equal(t, Cursor{Key: "4"}.Encode(), next)
		require.Equal(t, Cursor{Key: "3", Back: true}.Encode(), previous)
	})

	t.Run("reaching the head", func(t *testing.T) {
		t.Parallel()

		page, next, previous := Page(Cursor{Key: "3", Back: true}, []string{"2", "1"}, 2, false, identity)
		require.Equal(t, []string{"1", "2"}, page)
		require.Equal(t, Cursor{Key: "2"}.Encode(), next)
		require.Empty(t, previous)
	})

	t.Run("last page has no next", func(t *testing.T) {
		t.Parallel()

		page, next, previous := Page(Cursor{Back: true}, []string{"9", "8", "7"}, 2, false, identity)
		require.Equal(t, []string{"8", "9"}, page)
		require.Empty(t, next)
		require.Equal(t, Cursor{Key: "8", Back: true}.Encode(), previous)
	})

	t.Run("empty back page links to the head", func(t *testing.T) {
		t.Parallel()

		page, next, previous := Page(Cursor{Key: "1", Back: true}, []string{}, 2, false, identity)
		require.Empty(t, page)
		require.Equal(t, Cursor{}.Encode(), next)
		require.Empty(t, previous)
	})

	t.Run("does not mutate the input", func(t *testing.T) {
		t.Parallel()

		rows := []string{"2", "1"}
		_, _, _ = Page(Cursor{Key: "3", Back: true}, rows, 2, false, identity)
		require.Equal(t, []string{"2", "1"}, rows)
	})
}

func TestPageZeroSizeHasNoLinks(t *testing.T) {
	t.Parallel()

	page, next, previous := Page(Cursor{Key: "1"}, []string{"2", "3"}, 0, true, identity)
	require.Equal(t, []string{"2", "3"}, page)
	require.Empty(t, next)
	require.Empty(t, previous)
}

func TestLinksSkipEmptyKeys(t *testing.T) {
	t.Parallel()

	next, previous := Cursor{Key: "1"}.Links("", "", 2, true)
	require.Empty(t, next)
	require.Empty(t, previous)
}
