package http

import (
	"bytes"
	"encoding/json"
	"io"
)

// decodePreparedQueryBody consumes the entire body through the router's size
// limit before decoding one JSON value. A streaming Decode alone would ignore
// a second value or trailing junk, and could miss an oversized suffix.
func decodePreparedQueryBody(reader io.Reader, dst any) error {
	if reader == nil {
		return io.EOF
	}

	body, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	if len(bytes.Trim(body, " \t\r\n")) == 0 {
		return io.EOF
	}

	return json.Unmarshal(body, dst)
}
