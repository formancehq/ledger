package v1

import (
	"math/big"
	"net/http"
	"strings"

	"github.com/formancehq/go-libs/v2/query"
	ledger "github.com/formancehq/ledger/internal"
)

func mapTransactionToV1(tx ledger.Transaction) any {
	type Aux ledger.Transaction
	type Ret struct {
		Aux

		Reverted                  bool                     `json:"reverted"`
		PreCommitVolumes          ledger.PostCommitVolumes `json:"preCommitVolumes,omitempty"`
		PreCommitEffectiveVolumes ledger.PostCommitVolumes `json:"preCommitEffectiveVolumes,omitempty"`
		TxID                      *int                     `json:"txid"`
		ID                        *int                     `json:"-"`
	}

	var (
		preCommitVolumes          ledger.PostCommitVolumes
		preCommitEffectiveVolumes ledger.PostCommitVolumes
	)
	if len(tx.PostCommitVolumes) > 0 {
		if tx.PostCommitVolumes != nil {
			preCommitVolumes = tx.PostCommitVolumes.Copy()
			for _, posting := range tx.Postings {
				preCommitVolumes.AddOutput(posting.Source, posting.Asset, big.NewInt(0).Neg(posting.Amount))
				preCommitVolumes.AddInput(posting.Destination, posting.Asset, big.NewInt(0).Neg(posting.Amount))
			}
		}
	}
	if len(tx.PostCommitEffectiveVolumes) > 0 {
		if tx.PostCommitEffectiveVolumes != nil {
			preCommitEffectiveVolumes = tx.PostCommitEffectiveVolumes.Copy()
			for _, posting := range tx.Postings {
				preCommitEffectiveVolumes.AddOutput(posting.Source, posting.Asset, big.NewInt(0).Neg(posting.Amount))
				preCommitEffectiveVolumes.AddInput(posting.Destination, posting.Asset, big.NewInt(0).Neg(posting.Amount))
			}
		}
	}

	return &Ret{
		Aux:                       Aux(tx),
		Reverted:                  tx.RevertedAt != nil && !tx.RevertedAt.IsZero(),
		PreCommitVolumes:          preCommitVolumes,
		PreCommitEffectiveVolumes: preCommitEffectiveVolumes,
		TxID:                      tx.ID,
	}
}

func buildGetTransactionsQuery(r *http.Request) query.Builder {
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
		return nil
	}
	if len(clauses) == 1 {
		return clauses[0]
	}

	return query.And(clauses...)
}
