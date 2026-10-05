// Package pagecursor encodes the resume tokens Ledger list endpoints
// exchange: the x-next-cursor / x-previous-cursor gRPC trailers, the
// prepared-query and index-inspection cursor fields, and the HTTP `cursor`
// parameter with its `next` / `previous` response fields.
//
// A token is base64url (unpadded) of the JSON form of Cursor. Clients
// normally pass tokens back verbatim; building one by hand is supported, and
// the key's textual form is documented per endpoint (a decimal id, an
// address, a name, …).
//
// Both directions are exclusive of the key:
//
//   - a forward cursor {key: K} serves the rows strictly after K in the
//     query's order;
//   - a back cursor {key: K, back: true} serves the page that ends strictly
//     before K, still returned in the query's order. A back cursor with an
//     empty key serves the last page.
package pagecursor

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
)

// ErrInvalid reports a token that does not decode to a Cursor.
var ErrInvalid = errors.New("invalid cursor")

// Cursor is the decoded form of a page token.
type Cursor struct {
	Key  string `json:"key,omitempty"`
	Back bool   `json:"back,omitempty"`
}

// Encode returns the token for c. The zero Cursor encodes to a non-empty
// token that serves the first page.
func (c Cursor) Encode() string {
	data, err := json.Marshal(c)
	if err != nil {
		// A struct of a string and a bool always marshals.
		panic(fmt.Sprintf("pagecursor: marshal: %v", err))
	}

	return base64.RawURLEncoding.EncodeToString(data)
}

// Decode parses a token. The empty token is the zero Cursor: the first page.
func Decode(token string) (Cursor, error) {
	if token == "" {
		return Cursor{}, nil
	}

	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return Cursor{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}

	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] != '{' {
		return Cursor{}, fmt.Errorf("%w: not a JSON object", ErrInvalid)
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()

	// Fields decode raw first so a null, which encoding/json would accept as
	// the zero value, is rejected like any other mistyped field.
	var w struct {
		Key  json.RawMessage `json:"key"`
		Back json.RawMessage `json:"back"`
	}
	if err := dec.Decode(&w); err != nil {
		return Cursor{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}

	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return Cursor{}, fmt.Errorf("%w: trailing data", ErrInvalid)
	}

	var c Cursor
	if err := decodeField(w.Key, &c.Key); err != nil {
		return Cursor{}, fmt.Errorf("%w: key: %w", ErrInvalid, err)
	}

	if err := decodeField(w.Back, &c.Back); err != nil {
		return Cursor{}, fmt.Errorf("%w: back: %w", ErrInvalid, err)
	}

	return c, nil
}

// decodeField decodes a present field into v, rejecting null.
func decodeField(raw json.RawMessage, v any) error {
	if raw == nil {
		return nil
	}

	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return errors.New("null")
	}

	return json.Unmarshal(raw, v)
}

// Uint64 parses the key of an endpoint keyed by a decimal id. The empty key
// is zero, which every such endpoint treats as "no position": ids start at 1.
func (c Cursor) Uint64() (uint64, error) {
	if c.Key == "" {
		return 0, nil
	}

	v, err := strconv.ParseUint(c.Key, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: key %q is not a decimal id", ErrInvalid, c.Key)
	}

	return v, nil
}

// ReadReverse returns the order the source must be read in to serve c on a
// query whose requested order is reverse: a back page is the opposite read
// from the same key.
func (c Cursor) ReadReverse(reverse bool) bool {
	return reverse != c.Back
}

// Links returns the next and previous tokens of the page c requested. first
// and last are the keys of the page's first and last rows in query order;
// rows is the page's row count; more reports that a row beyond the page was
// seen in the read direction. An empty first or last key yields no link
// through it.
func (c Cursor) Links(first, last string, rows int, more bool) (next, previous string) {
	if !c.Back {
		if more && rows > 0 && last != "" {
			next = Cursor{Key: last}.Encode()
		}

		if c.Key != "" {
			switch {
			case rows == 0:
				// Nothing follows K, so the page ending at K is the last page.
				previous = Cursor{Back: true}.Encode()
			case first != "":
				previous = Cursor{Key: first, Back: true}.Encode()
			}
		}

		return next, previous
	}

	if more && rows > 0 && first != "" {
		previous = Cursor{Key: first, Back: true}.Encode()
	}

	if c.Key != "" {
		switch {
		case rows == 0:
			// Nothing precedes K, so the page after it starts at the head.
			next = Cursor{}.Encode()
		case last != "":
			next = Cursor{Key: last}.Encode()
		}
	}

	return next, previous
}

// Page builds the page c requested from rows read in c.ReadReverse order,
// fetched with one row beyond pageSize so a further row can be detected.
// more additionally reports a further row known to the source (a routed read
// whose upstream capped its own response). It returns the page in query
// order with its links. A zero pageSize returns every row without links.
func Page[T any](c Cursor, rows []T, pageSize uint32, more bool, keyOf func(T) string) (page []T, next, previous string) {
	if pageSize > 0 && len(rows) > int(pageSize) {
		rows = rows[:pageSize]
		more = true
	}

	if c.Back {
		rows = slices.Clone(rows)
		slices.Reverse(rows)
	}

	if pageSize == 0 {
		return rows, "", ""
	}

	var first, last string
	if len(rows) > 0 {
		first, last = keyOf(rows[0]), keyOf(rows[len(rows)-1])
	}

	next, previous = c.Links(first, last, len(rows), more)

	return rows, next, previous
}
