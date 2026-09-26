package bulking

import "github.com/formancehq/ledger/internal/api/common"

type APIResult struct {
	ErrorCode        string                    `json:"errorCode,omitempty"`
	ErrorDescription string                    `json:"errorDescription,omitempty"`
	Data             any                       `json:"data,omitempty"`
	ResponseType     string                    `json:"responseType"` // Added for sdk generation (discriminator in oneOf)
	LogID            uint64                    `json:"logID"`
	Diagnostics      []common.ParserDiagnostic `json:"diagnostics,omitempty"`
}
