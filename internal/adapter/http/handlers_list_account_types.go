package http

import (
	"net/http"
	"sort"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

type accountTypeJSON struct {
	Name    string `json:"name"`
	Pattern string `json:"pattern"`
	Status  string `json:"status"`
}

type listAccountTypesResponse struct {
	Types []accountTypeJSON `json:"types"`
}

func accountTypeStatusToString(status commonpb.AccountTypeStatus) string {
	switch status {
	case commonpb.AccountTypeStatus_ACCOUNT_TYPE_DEPRECATED:
		return "DEPRECATED"
	default:
		return "ACTIVE"
	}
}

func toAccountTypeJSON(at *commonpb.AccountType) accountTypeJSON {
	return accountTypeJSON{
		Name:    at.GetName(),
		Pattern: at.GetPattern(),
		Status:  accountTypeStatusToString(at.GetStatus()),
	}
}

// handleListAccountTypes handles GET /{ledgerName}/account-types.
func (s *Server) handleListAccountTypes(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	ledgerInfo, err := s.backend.GetLedgerByName(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)

		return
	}

	types := make([]accountTypeJSON, 0, len(ledgerInfo.GetAccountTypes()))
	for _, at := range ledgerInfo.GetAccountTypes() {
		types = append(types, toAccountTypeJSON(at))
	}

	sort.Slice(types, func(i, j int) bool {
		return types[i].Name < types[j].Name
	})

	writeOK(w, &listAccountTypesResponse{Types: types})
}
