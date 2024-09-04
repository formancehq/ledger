package v1

import (
	"net/http"
	"strings"

	"github.com/formancehq/go-libs/query"
	ledger "github.com/formancehq/ledger/internal"
)

func mapTransactionToV1(tx ledger.Transaction) any {
	return struct {
		ledger.Transaction
		TxID int `json:"txid"`
		ID   int `json:"-"`
	}{
		Transaction: tx,
		TxID:        tx.ID,
	}
}

func mapExpandedTransactionToV1(tx ledger.ExpandedTransaction) any {
	return struct {
		ledger.ExpandedTransaction
		TxID int `json:"txid"`
		ID   int `json:"-"`
	}{
		ExpandedTransaction: tx,
		TxID:                tx.ID,
	}
}

func buildGetTransactionsQuery(r *http.Request) (query.Builder, error) {
	clauses := make([]query.Builder, 0)
	if after := r.URL.Query().Get("after"); after != "" {
		clauses = append(clauses, query.Lt("id", after))
	}

	if startTime := r.URL.Query().Get("start_time"); startTime != "" {
		clauses = append(clauses, query.Gte("date", startTime))
	}
	if endTime := r.URL.Query().Get("end_time"); endTime != "" {
		clauses = append(clauses, query.Lt("date", endTime))
	}

	if reference := r.URL.Query().Get("reference"); reference != "" {
		clauses = append(clauses, query.Match("reference", reference))
	}
	if source := r.URL.Query().Get("source"); source != "" {
		clauses = append(clauses, query.Match("source", source))
	}
	if destination := r.URL.Query().Get("destination"); destination != "" {
		clauses = append(clauses, query.Match("destination", destination))
	}
	if address := r.URL.Query().Get("account"); address != "" {
		clauses = append(clauses, query.Match("account", address))
	}
	for elem, value := range r.URL.Query() {
		if strings.HasPrefix(elem, "metadata") {
			clauses = append(clauses, query.Match(elem, value[0]))
		}
	}

	if len(clauses) == 0 {
		return nil, nil
	}
	if len(clauses) == 1 {
		return clauses[0], nil
	}

	return query.And(clauses...), nil
}
