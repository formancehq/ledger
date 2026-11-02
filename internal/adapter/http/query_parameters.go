package http

import (
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
)

// knownQuery validates the query contract at the HTTP boundary, before the
// handler can execute a read or a mutation with an unintended set of options.
// Each route declares its own names beside its registration in handler.go.
func knownQuery(next http.HandlerFunc, allowed ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid query string: %w", err))

			return
		}

		for _, key := range slices.Sorted(maps.Keys(query)) {
			if slices.Contains(allowed, key) {
				continue
			}

			message := fmt.Sprintf("unknown query parameter %q", key)
			switch key {
			case "prefix", "reference":
				message += ": use filter instead"
			case "checkpointId":
				message += ": checkpoint-pinned reads are available through gRPC only"
			}
			writeBadRequest(w, "INVALID_REQUEST", errors.New(message))

			return
		}

		next(w, r)
	}
}
