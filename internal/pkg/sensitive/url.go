package sensitive

import (
	"net/url"
	"strings"

	"github.com/formancehq/ledger/v3/internal/pkg/connectionurl"
)

// credentialQueryKeys are query parameter names that commonly carry credentials.
// The lookup is case-folded and dash-normalised on the key before comparison.
var credentialQueryKeys = map[string]bool{
	"password": true, "sslpassword": true, "passwd": true, "secret": true, "token": true,
	"api_key": true, "apikey": true, "access_token": true, "access_key": true,
	"client_secret": true, "authorization": true,
}

// proxyQueryKeys are query parameter names whose values are themselves URLs
// that may carry credentials (e.g. ClickHouse http_proxy, https_proxy).
var proxyQueryKeys = map[string]bool{
	"http_proxy": true, "https_proxy": true,
}

// urlPolicy is selected by a declared connection format, never inferred from
// arbitrary annotated input. Parsing does not load driver or ambient settings.
type urlPolicy struct {
	schemes          []string
	maskBareUserinfo bool
	allowEmptyHost   bool
}

const maxProxyDepth = 8

func redactConnectionURL(raw string, policy urlPolicy, depth int) (string, bool) {
	if depth > maxProxyDepth {
		return "", false
	}
	parsed, ok := connectionurl.Parse(strings.TrimSpace(raw), policy.allowEmptyHost, policy.schemes...)
	if !ok {
		return "", false
	}
	if parsed.User != nil {
		if _, hasPassword := parsed.User.Password(); hasPassword {
			parsed.User = url.UserPassword(parsed.User.Username(), "xxxxx")
		} else if policy.maskBareUserinfo && parsed.User.Username() != "" {
			parsed.User = url.User("xxxxx")
		}
	}
	if parsed.RawQuery != "" {
		values, qErr := url.ParseQuery(parsed.RawQuery)
		if qErr != nil {
			parsed.RawQuery = "xxxxx"
		} else {
			for key, items := range values {
				norm := strings.ToLower(strings.ReplaceAll(key, "-", "_"))
				if credentialQueryKeys[norm] {
					for i := range items {
						items[i] = "xxxxx"
					}
					values[key] = items
				} else if proxyQueryKeys[norm] {
					// The value is itself a URL that may contain credentials.
					// url.ParseQuery already decoded the value; do not call QueryUnescape
					// again or inner encoded delimiters will be misinterpreted.
					for i, v := range items {
						if sanitized, ok := redactHTTPURL(v, depth+1); ok {
							items[i] = sanitized
						} else {
							items[i] = "xxxxx"
						}
					}
					values[key] = items
				}
			}
			parsed.RawQuery = values.Encode()
		}
	}

	result := parsed.String()
	// URL.String omits an empty authority on pathless URLs (nats:// becomes
	// nats:). Keep the explicit URL form accepted by admission and drivers.
	if !strings.HasPrefix(result, parsed.Scheme+"://") {
		result = strings.Replace(result, ":", "://", 1)
	}

	return result, true
}
