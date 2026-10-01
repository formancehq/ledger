package http

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/formancehq/ledger/v3/internal/query"
)

// headerConsistency is the HTTP twin of the gRPC `x-consistency` metadata.
const headerConsistency = "X-Consistency"

// readConsistency stores the caller's X-Consistency selector in the request
// context, where RoutedController.readCtrl picks the read route exactly as it
// does for gRPC: `linearizable` (the default) runs the ReadIndex barrier,
// `stale` reads the local node's store without a barrier and is never
// forwarded to the leader. A valid level has no effect on writes.
//
// Unlike the gRPC interceptor, which silently ignores unknown values, this
// middleware rejects them with 400 on every /v3 route, writes included: a
// caller that asked for a read contract the server does not offer must not be
// served a different one without being told. A repeated header, including the
// comma-joined form a proxy may merge it into (RFC 9110 §5.3), is rejected for
// the same reason.
func readConsistency(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values(headerConsistency)
		if len(values) == 0 {
			next.ServeHTTP(w, r)

			return
		}

		if len(values) > 1 || strings.Contains(values[0], ",") {
			writeBadRequest(w, "INVALID_REQUEST",
				fmt.Errorf("%s must be set at most once", headerConsistency))

			return
		}

		level, ok := query.ParseConsistency(values[0])
		if !ok {
			writeBadRequest(w, "INVALID_REQUEST",
				fmt.Errorf("unsupported %s %q: use %q or %q", headerConsistency, values[0],
					query.ConsistencyLinearizable, query.ConsistencyStale))

			return
		}

		next.ServeHTTP(w, r.WithContext(query.WithConsistency(r.Context(), level)))
	})
}
