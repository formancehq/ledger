package backend

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

func EncodeLink(errStr string) string {
	if errStr == "" {
		return ""
	}

	errStr = strings.ReplaceAll(errStr, "\n", "\r\n")
	payload, err := json.Marshal(map[string]any{
		"error": errStr,
	})
	if err != nil {
		panic(err)
	}
	payloadB64 := base64.StdEncoding.EncodeToString(payload)
	return fmt.Sprintf("https://play.numscript.org/?payload=%v", payloadB64)
}
