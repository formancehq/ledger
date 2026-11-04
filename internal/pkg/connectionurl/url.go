// Package connectionurl defines the lexical URL boundary shared by connector
// admission and public projection. It never loads driver or ambient settings.
package connectionurl

import (
	"net/url"
	"slices"
	"strings"
)

// Parse accepts hierarchical URLs with an explicit scheme from schemes.
// allowEmptyHost permits connector socket/default-host URLs, including port-only
// authorities. The boolean keeps parser errors, which can contain credentials,
// out of public diagnostics.
func Parse(raw string, allowEmptyHost bool, schemes ...string) (*url.URL, bool) {
	scheme, _, explicit := strings.Cut(raw, "://")
	if !explicit || !slices.Contains(schemes, strings.ToLower(scheme)) {
		return nil, false
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Opaque != "" || (!allowEmptyHost && parsed.Hostname() == "") {
		return nil, false
	}

	return parsed, true
}
