package admission

import "github.com/formancehq/ledger/v3/internal/domain"

// invalidConnectionURL is an admission-only error. It owns safe public details
// rather than minting a typed audited failure code: this rejection never enters
// Raft, the audit chain or the idempotency projection.
type invalidConnectionURL struct {
	message string
}

func (e *invalidConnectionURL) Error() string        { return e.message }
func (*invalidConnectionURL) Kind() domain.ErrorKind { return domain.KindValidation }
func (*invalidConnectionURL) Reason() string         { return domain.ErrReasonValidation }
func (e *invalidConnectionURL) PublicDetails() (string, map[string]string, bool) {
	return e.message, nil, true
}
