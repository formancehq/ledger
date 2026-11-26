// Package connectionurl defines the lexical URL boundary shared by connector
// admission and public projection. It never loads driver or ambient settings.
package connectionurl

import (
	"net/url"
	"slices"
	"strconv"
	"strings"
)

// Parse accepts hierarchical URLs with an explicit scheme from schemes.
// allowEmptyHost permits connector socket/default-host URLs, including port-only
// authorities. The boolean keeps parser errors, which can contain credentials,
// out of public diagnostics.
func Parse(raw string, allowEmptyHost bool, schemes ...string) (*url.URL, bool) {
	parsed, ok := ParseShape(raw, allowEmptyHost)
	if !ok || !slices.Contains(schemes, strings.ToLower(parsed.Scheme)) {
		return nil, false
	}

	return parsed, true
}

// ParseShape accepts a hierarchical URL with any explicit scheme. It checks
// syntax only; callers that need connector-specific schemes use Parse instead.
// It never loads driver options or ambient configuration.
func ParseShape(raw string, allowEmptyHost bool) (*url.URL, bool) {
	scheme, _, explicit := strings.Cut(raw, "://")
	if !explicit {
		return nil, false
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Scheme != strings.ToLower(scheme) || parsed.Opaque != "" || (!allowEmptyHost && parsed.Hostname() == "") {
		return nil, false
	}

	// net/url checks only the final port of a comma-separated authority.
	// Validate each host independently while retaining the original authority.
	for authority := range strings.SplitSeq(parsed.Host, ",") {
		hostURL := &url.URL{Host: authority}
		host, err := url.Parse(hostURL.String())
		if err != nil {
			return nil, false
		}
		// URL.String re-escapes decoded IPv6 zone identifiers before parsing.
		// net/url validates port syntax, but not its numeric range.
		if host.Port() != "" {
			if _, err := strconv.ParseUint(host.Port(), 10, 16); err != nil {
				return nil, false
			}
		}
	}

	return parsed, true
}
