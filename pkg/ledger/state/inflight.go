package state

import (
	"github.com/formancehq/ledger/pkg/core"
)

type inFlight struct {
	reference      string
	timestamp      core.Time
	previous, next *inFlight
	terminated     bool
}
