package preparedquery

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// cursorData is the internal representation of a pagination cursor.
type cursorData struct {
	After []byte `json:"after"`
}

// encodeCursor encodes the last entity seen into an opaque cursor string.
func encodeCursor(lastEntity []byte) string {
	data, _ := json.Marshal(cursorData{After: lastEntity})
	return base64.RawURLEncoding.EncodeToString(data)
}

// decodeCursor decodes an opaque cursor string into the last entity seen.
func decodeCursor(cursor string) ([]byte, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}
	var data cursorData
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("invalid cursor data: %w", err)
	}
	return data.After, nil
}
