package admission

import "github.com/formancehq/ledger/v3/internal/domain"

// invalidIntegrationConfig is an admission-only error. It owns safe public details
// rather than minting a typed audited failure code: this rejection never enters
// Raft, the audit chain or the idempotency projection.
type invalidIntegrationConfig struct {
	message string
}

func (e *invalidIntegrationConfig) Error() string { return e.message }
func (e *invalidIntegrationConfig) ValidationCode() string {
	if e == ErrSinkConfigIncomplete {
		return "SINK_CONFIG_INCOMPLETE"
	}

	return ""
}
func (*invalidIntegrationConfig) Kind() domain.ErrorKind { return domain.KindValidation }
func (*invalidIntegrationConfig) Reason() string         { return domain.ErrReasonValidation }
func (e *invalidIntegrationConfig) PublicDetails() (string, map[string]string, bool) {
	return e.message, nil, true
}
