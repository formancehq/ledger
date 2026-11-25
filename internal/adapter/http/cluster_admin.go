package http

import (
	"bytes"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	internalgrpc "github.com/formancehq/ledger/v3/internal/adapter/grpc"
)

// clusterNodeID is a full-width uint64 node identity. JSON accepts an integer
// literal or its decimal string form, so clients without exact 64-bit numbers
// can still address every node.
type clusterNodeID uint64

func (id *clusterNodeID) UnmarshalJSON(data []byte) error {
	raw := data
	if len(data) > 0 && data[0] == '"' {
		var s string
		if err := jsonv2.Unmarshal(data, &s); err != nil {
			return err
		}
		raw = []byte(s)
	}
	parsed, err := parseClusterNodeID(string(raw))
	if err != nil {
		return err
	}
	*id = clusterNodeID(parsed)

	return nil
}

func parseClusterNodeID(raw string) (uint64, error) {
	parsed, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("node ID must be a decimal unsigned 64-bit integer, got %q", raw)
	}
	if parsed == 0 {
		return 0, errors.New("node ID must be non-zero")
	}

	return parsed, nil
}

// requireClusterNodeID reads the {nodeId} path parameter.
func requireClusterNodeID(w http.ResponseWriter, r *http.Request) (uint64, bool) {
	id, err := parseClusterNodeID(chi.URLParam(r, "nodeId"))
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return 0, false
	}

	return id, true
}

// decodeClusterBody decodes exactly one JSON object and rejects unknown or
// duplicate members, so a misspelled field cannot silently select a default.
func decodeClusterBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return false
	}
	if len(bytes.TrimSpace(body)) == 0 {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("request body is required"))

		return false
	}
	if err := jsonv2.Unmarshal(body, dst, jsonv2.RejectUnknownMembers(true)); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))

		return false
	}

	return true
}

// handleClusterError renders a cluster administration failure with the status
// the gRPC ClusterService answers for it. The in-process call returns the raw
// node error on the leader and the leader's status when forwarded, so both are
// normalized to a status before rendering.
func handleClusterError(w http.ResponseWriter, r *http.Request, err error) {
	if classifyError(err).sanitized {
		if st, ok := internalgrpc.KnownErrorStatus(err); ok {
			err = st.Err()
		}
	}
	if c, ok := clusterStatusClass(err); ok {
		writeErrorClass(w, r, err, c)

		return
	}
	handleError(w, r, err)
}

// clusterStatusClass renders the ClusterService statuses whose message the
// gRPC surface already returns to the same caller. A ledger ErrorInfo reason,
// such as RAFT_NODE_REMOVAL_COMMITTED, becomes the error code.
func clusterStatusClass(err error) (errorClass, bool) {
	st, ok := status.FromError(err)
	if !ok {
		return errorClass{}, false
	}
	reason := ""
	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.GetDomain() == "ledger" {
			reason = info.GetReason()
		}
	}
	c := errorClass{code: reason, message: st.Message()}
	switch st.Code() {
	case codes.InvalidArgument:
		c.status, c.code = http.StatusBadRequest, "INVALID_REQUEST"
	case codes.NotFound:
		c.status = http.StatusNotFound
		if c.code == "" {
			c.code = "NOT_FOUND"
		}
	case codes.AlreadyExists:
		c.status = http.StatusConflict
		if c.code == "" {
			c.code = "ALREADY_EXISTS"
		}
	case codes.FailedPrecondition:
		c.status = http.StatusConflict
		if c.code == "" {
			c.code = "FAILED_PRECONDITION"
		}
	case codes.Unavailable:
		if reason == "" {
			return errorClass{}, false
		}
		c.status = http.StatusServiceUnavailable
	default:
		return errorClass{}, false
	}

	return c, true
}
