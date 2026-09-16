package cmdutil

import (
	"fmt"
	"net/url"
	"strings"
)

// FormatBytes formats a byte count as a human-readable string.
func FormatBytes(bytes uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/TB)
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// WrapText wraps text to fit within maxWidth, preferring to break at separator
// boundaries. Returns a slice of lines. If the text fits within maxWidth, a
// single-element slice is returned.
func WrapText(text string, maxWidth int, separator string) []string {
	if maxWidth <= 0 || len(text) <= maxWidth {
		return []string{text}
	}

	var lines []string

	remaining := text

	for len(remaining) > maxWidth {
		cutPoint := -1

		if separator != "" {
			lastSep := strings.LastIndex(remaining[:maxWidth], separator)
			if lastSep > 0 {
				cutPoint = lastSep + len(separator)
			}
		}

		if cutPoint <= 0 {
			cutPoint = maxWidth
		}

		lines = append(lines, remaining[:cutPoint])
		remaining = remaining[cutPoint:]
	}

	if remaining != "" {
		lines = append(lines, remaining)
	}

	return lines
}

// ObfuscateDSN replaces the password in a DSN URL with "****".
// Works with postgres://, postgresql://, clickhouse:// and similar URL-format DSNs.
// If the DSN is not URL-formatted or has no password, it is returned unchanged.
func ObfuscateDSN(dsn string) string {
	return obfuscateURLPassword(dsn)
}

// ObfuscateClickHouseDSN replaces userinfo passwords and the ClickHouse
// driver's supported password query parameter with "****".
func ObfuscateClickHouseDSN(dsn string) string {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return ObfuscateDSN(dsn)
	}

	if parsed.User != nil {
		username := parsed.User.Username()
		password, hasPassword := parsed.User.Password()
		if hasPassword && password != "" {
			parsed.User = url.UserPassword(username, "****")
		}
	}

	query := parsed.Query()
	passwords, ok := query["password"]
	if !ok {
		return restoreObfuscationMask(parsed.String())
	}

	for index, password := range passwords {
		if password != "" {
			passwords[index] = "****"
		}
	}
	parsed.RawQuery = query.Encode()

	return restoreObfuscationMask(parsed.String())
}

// ObfuscateURLPassword replaces a URL password with "****" while preserving
// the username and all other connection details.
func ObfuscateURLPassword(value string) string {
	parsed, err := url.Parse(value)
	if err != nil || parsed.User == nil {
		return value
	}

	username := parsed.User.Username()
	password, hasPassword := parsed.User.Password()
	if hasPassword && password != "" {
		parsed.User = url.UserPassword(username, "****")
	}

	return restoreObfuscationMask(parsed.String())
}

// ObfuscateURLUserinfo replaces URL passwords and token-only userinfo with
// "****" while preserving non-secret connection details. A username paired
// with a password remains visible; sole userinfo is treated as a token.
func ObfuscateURLUserinfo(value string) string {
	const defaultScheme = "nats://"

	normalized := value
	schemeLess := !strings.Contains(value, "://") && strings.Contains(value, "@")
	if schemeLess {
		normalized = defaultScheme + value
	}

	parsed, err := url.Parse(normalized)
	if err != nil || parsed.User == nil {
		return value
	}

	if _, hasPassword := parsed.User.Password(); !hasPassword {
		parsed.User = url.User("****")
	} else {
		username := parsed.User.Username()
		password, _ := parsed.User.Password()
		if password != "" {
			parsed.User = url.UserPassword(username, "****")
		}
	}

	obfuscated := restoreObfuscationMask(parsed.String())
	if schemeLess {
		return strings.TrimPrefix(obfuscated, defaultScheme)
	}

	return obfuscated
}

func obfuscateURLPassword(dsn string) string {
	schemeEnd := strings.Index(dsn, "://")
	if schemeEnd == -1 {
		return dsn
	}

	rest := dsn[schemeEnd+3:]

	lastAt := strings.LastIndex(rest, "@")
	if lastAt == -1 {
		return dsn
	}

	creds := rest[:lastAt]

	before, _, ok := strings.Cut(creds, ":")
	if !ok {
		return dsn
	}

	user := before
	hostPart := rest[lastAt:]

	return dsn[:schemeEnd+3] + user + ":****" + hostPart
}

func restoreObfuscationMask(value string) string {
	return strings.ReplaceAll(value, "%2A%2A%2A%2A", "****")
}
