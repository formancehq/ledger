package admission

import "github.com/formancehq/ledger/v3/internal/domain"

var ErrPostingAmountRequired = postingAmountRequired{}

type postingAmountRequired struct{}

func (postingAmountRequired) Error() string          { return "posting amount is required" }
func (postingAmountRequired) Kind() domain.ErrorKind { return domain.KindValidation }
func (postingAmountRequired) Reason() string         { return domain.ErrReasonValidation }
func (postingAmountRequired) PublicDetails() (string, map[string]string, bool) {
	return "posting amount is required", nil, true
}
