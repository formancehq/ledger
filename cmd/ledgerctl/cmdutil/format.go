package cmdutil

import (
	"fmt"
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

// ObfuscateDSN replaces the password in a DSN URL with "****".
// Works with postgres://, postgresql://, clickhouse:// and similar URL-format DSNs.
// If the DSN is not URL-formatted or has no password, it is returned unchanged.
func ObfuscateDSN(dsn string) string {
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
	colonIdx := strings.Index(creds, ":")
	if colonIdx == -1 {
		return dsn
	}

	user := creds[:colonIdx]
	hostPart := rest[lastAt:]

	return dsn[:schemeEnd+3] + user + ":****" + hostPart
}
