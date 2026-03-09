package http

import (
	"errors"
	"net/http"
	"sort"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/go-chi/chi/v5"
)

// protoTimestampToRFC3339 converts a proto Timestamp (microseconds since epoch) to RFC3339 string.
func protoTimestampToRFC3339(ts *commonpb.Timestamp) string {
	if ts == nil {
		return ""
	}
	return time.UnixMicro(int64(ts.Data)).UTC().Format(time.RFC3339)
}

type accountTypeJSON struct {
	Name              string                   `json:"name"`
	Pattern           string                   `json:"pattern"`
	Status            string                   `json:"status"`
	EnforcementMode   string                   `json:"enforcementMode"`
	SupersededBy      string                   `json:"supersededBy,omitempty"`
	MigrationProgress *migrationProgressJSON   `json:"migrationProgress,omitempty"`
}

type migrationProgressJSON struct {
	TotalAccounts    uint64 `json:"totalAccounts"`
	MigratedAccounts uint64 `json:"migratedAccounts"`
	StartedAt        string `json:"startedAt,omitempty"`
	CompletedAt      string `json:"completedAt,omitempty"`
}

type listAccountTypesResponse struct {
	Types []accountTypeJSON `json:"types"`
}

func accountTypeStatusToString(status commonpb.AccountTypeStatus) string {
	switch status {
	case commonpb.AccountTypeStatus_ACCOUNT_TYPE_MIGRATING:
		return "MIGRATING"
	case commonpb.AccountTypeStatus_ACCOUNT_TYPE_DEPRECATED:
		return "DEPRECATED"
	default:
		return "ACTIVE"
	}
}

func toAccountTypeJSON(at *commonpb.AccountType) accountTypeJSON {
	result := accountTypeJSON{
		Name:            at.Name,
		Pattern:         at.Pattern,
		Status:          accountTypeStatusToString(at.Status),
		EnforcementMode: enforcementModeToString(at.EnforcementMode),
		SupersededBy:    at.SupersededBy,
	}
	if at.MigrationProgress != nil {
		result.MigrationProgress = &migrationProgressJSON{
			TotalAccounts:    at.MigrationProgress.TotalAccounts,
			MigratedAccounts: at.MigrationProgress.MigratedAccounts,
		}
		if at.MigrationProgress.StartedAt != nil {
			result.MigrationProgress.StartedAt = protoTimestampToRFC3339(at.MigrationProgress.StartedAt)
		}
		if at.MigrationProgress.CompletedAt != nil {
			result.MigrationProgress.CompletedAt = protoTimestampToRFC3339(at.MigrationProgress.CompletedAt)
		}
	}
	return result
}

// handleListAccountTypes handles GET /{ledgerName}/account-types
func (s *Server) handleListAccountTypes(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	ledgerInfo, err := s.backend.GetLedgerByName(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)
		return
	}

	types := make([]accountTypeJSON, 0, len(ledgerInfo.AccountTypes))
	for _, at := range ledgerInfo.AccountTypes {
		types = append(types, toAccountTypeJSON(at))
	}

	sort.Slice(types, func(i, j int) bool {
		return types[i].Name < types[j].Name
	})

	writeOK(w, &listAccountTypesResponse{Types: types})
}
