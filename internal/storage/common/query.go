package common

import ledger "github.com/formancehq/ledger/internal"

type RunQuery struct {
	QueryTemplateParams ledger.QueryTemplateParams `json:"params,omitempty"`
	Vars                map[string]string          `json:"vars,omitempty"`
	Cursor              *string                    `json:"cursor,omitempty"`
}
