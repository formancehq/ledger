package controller

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// The audit command returns generic, decoded orders. The operator alone decides
// whether a creation batch establishes ownership of a current index.
type decodedCreationAudit struct {
	Entry struct {
		Timestamp *struct {
			Data json.Number `json:"data"`
		} `json:"timestamp"`
		Idempotency struct {
			Key string `json:"key"`
		} `json:"idempotency"`
		Success *struct {
			Min json.Number `json:"minLogSequence"`
			Max json.Number `json:"maxLogSequence"`
		} `json:"success"`
		OrderCount json.Number `json:"orderCount"`
		Items      []struct {
			OrderIndex  json.Number `json:"orderIndex"`
			LogSequence json.Number `json:"logSequence"`
		} `json:"items"`
	} `json:"entry"`
	Orders []struct {
		LedgerScoped struct {
			Ledger       string           `json:"ledger"`
			CreateLedger *json.RawMessage `json:"createLedger"`
			Apply        struct {
				CreateIndex *struct {
					ID listedIndexID `json:"id"`
				} `json:"createIndex"`
			} `json:"apply"`
		} `json:"ledgerScoped"`
	} `json:"orders"`
}

type listedIndexID struct {
	TxBuiltin      string `json:"txBuiltin"`
	AccountBuiltin string `json:"accountBuiltin"`
	Metadata       *struct {
		Target string `json:"target"`
		Key    string `json:"key"`
	} `json:"metadata"`
}

func canonicalAuditIndex(id listedIndexID) string {
	if flag, ok := protoTxEnumToFlag[id.TxBuiltin]; ok {
		return flag
	}
	if id.AccountBuiltin == "ACCT_BUILTIN_INDEX_ASSET" {
		return accountAssetFlag
	}
	if id.Metadata != nil {
		if target, ok := protoTargetToFlag[id.Metadata.Target]; ok {
			return canonicalIndex(metadataTypeFlag, target, id.Metadata.Key)
		}
	}

	return ""
}

func currentIndexCreationDates(stdout, ledger string) (map[string]string, error) {
	var entries []listedIndex
	if err := json.Unmarshal([]byte(stdout), &entries); err != nil {
		return nil, fmt.Errorf("parsing current indexes for audit attribution: %w", err)
	}
	current := make(map[string]string)
	for _, entry := range entries {
		if entry.Ledger != ledger || entry.CreatedAt == nil {
			continue
		}
		id := listedIndexID{TxBuiltin: entry.ID.TxBuiltin, AccountBuiltin: entry.ID.AccountBuiltin, Metadata: entry.ID.Metadata}
		if canonical := canonicalAuditIndex(id); canonical != "" {
			current[canonical] = entry.CreatedAt.Data.String()
		}
	}

	return current, nil
}

func initialIndexesFromAudit(stdout, ledger, prefix string, current map[string]string) (map[string]bool, error) {
	var entries []decodedCreationAudit
	if err := json.Unmarshal([]byte(stdout), &entries); err != nil {
		return nil, fmt.Errorf("parsing initial creation audit: %w", err)
	}
	owned := make(map[string]bool)
	for _, entry := range entries {
		e := entry.Entry
		if !strings.HasPrefix(e.Idempotency.Key, prefix) || e.Success == nil || e.Timestamp == nil || len(e.Items) < 2 || len(e.Items) != len(entry.Orders) {
			continue
		}
		count, err := strconv.Atoi(e.OrderCount.String())
		if err != nil || count != len(e.Items) {
			continue
		}
		minSequence, err := strconv.ParseUint(e.Success.Min.String(), 10, 64)
		if err != nil || minSequence == 0 {
			continue
		}
		maxSequence, err := strconv.ParseUint(e.Success.Max.String(), 10, 64)
		if err != nil || maxSequence < minSequence || maxSequence-minSequence+1 != uint64(count) {
			continue
		}
		batch := make(map[string]bool)
		valid := true
		for i, item := range e.Items {
			position, err := strconv.Atoi(item.OrderIndex.String())
			if err != nil || position != i {
				valid = false

				break
			}
			sequence, err := strconv.ParseUint(item.LogSequence.String(), 10, 64)
			if err != nil || sequence != minSequence+uint64(i) {
				valid = false

				break
			}
			order := entry.Orders[i].LedgerScoped
			if order.Ledger != ledger {
				valid = false

				break
			}
			if i == 0 {
				if order.CreateLedger == nil {
					valid = false
				}

				continue
			}
			if order.Apply.CreateIndex == nil {
				valid = false

				break
			}
			canonical := canonicalAuditIndex(order.Apply.CreateIndex.ID)
			if canonical == "" || batch[canonical] {
				valid = false

				break
			}
			batch[canonical] = true
		}
		if !valid {
			continue
		}
		for canonical := range batch {
			if current[canonical] != "" && current[canonical] == e.Timestamp.Data.String() {
				owned[canonical] = true
			}
		}
	}

	return owned, nil
}
