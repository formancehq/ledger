package http

import (
	"errors"
	"net/http"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// knownMutationQuery extends the route's query contract with an optional,
// strict presentation selector. Validate before execution, including empty and
// duplicate values; response preferences never enter the admitted request.
func knownMutationQuery(next http.HandlerFunc, allowed ...string) http.HandlerFunc {
	return knownQuery(func(w http.ResponseWriter, r *http.Request) {
		if values, present := r.URL.Query()["skipResponse"]; present &&
			(len(values) != 1 || (values[0] != "true" && values[0] != "false")) {
			writeBadRequest(w, "INVALID_REQUEST", errors.New("skipResponse must be exactly one true or false value"))

			return
		}
		next(w, r)
	}, append(allowed, "skipResponse")...)
}

type writeAcknowledgement struct {
	Sequence uint64 `json:"sequence"`
}

// writeMutationCreated projects only after the caller validates the expected
// log body. It never alters a log or attaches a full signed receipt, which
// would disclose the payload being suppressed.
func writeMutationCreated(w http.ResponseWriter, r *http.Request, entry *commonpb.Log, data any) {
	if queryParamBool(r, "skipResponse") {
		requireAcknowledgementSequence(entry.GetSequence())
		writeCreated(w, r, writeAcknowledgement{Sequence: entry.GetSequence()})

		return
	}
	writeCreated(w, r, data)
}

func requireAcknowledgementSequence(sequence uint64) {
	if sequence == 0 {
		assert.Unreachable("HTTP acknowledgement requires a committed log sequence", nil)
		panic("HTTP acknowledgement requires a committed log sequence")
	}
}
