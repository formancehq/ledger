package v2

import (
	"encoding/json"
	"math/big"
	"net/http"
	"strings"

	. "github.com/formancehq/go-libs/v4/collectionutils"

	ledger "github.com/formancehq/ledger/internal"
)

const HeaderBigIntAsString = "Formance-Bigint-As-String"

type volumes ledger.Volumes

func (v volumes) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Input   any `json:"input"`
		Output  any `json:"output"`
		Balance any `json:"balance"`
	}{
		Input:   v.Input.String(),
		Output:  v.Output.String(),
		Balance: (ledger.Volumes)(v).Balance().String(),
	})
}

type volumesByAssets ledger.VolumesByAssets

func (v volumesByAssets) MarshalJSON() ([]byte, error) {
	return json.Marshal(ConvertMap(v, func(v ledger.Volumes) volumes {
		return volumes(v)
	}))
}

type postCommitVolumes ledger.PostCommitVolumes

func (v postCommitVolumes) MarshalJSON() ([]byte, error) {
	return json.Marshal(ConvertMap(v, func(v ledger.VolumesByAssets) volumesByAssets {
		return volumesByAssets(v)
	}))
}

type posting ledger.Posting

func (p posting) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ledger.Posting
		Amount string `json:"amount"`
	}{
		Posting: ledger.Posting(p),
		Amount:  p.Amount.String(),
	})
}

type postings []posting

type transaction ledger.Transaction

func (tx transaction) MarshalJSON() ([]byte, error) {
	type Aux transaction

	return json.Marshal(struct {
		Aux
		Postings                   postings          `json:"postings"`
		PostCommitVolumes          postCommitVolumes `json:"postCommitVolumes,omitempty"`
		PostCommitEffectiveVolumes postCommitVolumes `json:"postCommitEffectiveVolumes,omitempty"`
		Reverted                   bool              `json:"reverted"`
		PreCommitVolumes           postCommitVolumes `json:"preCommitVolumes,omitempty"`
		PreCommitEffectiveVolumes  postCommitVolumes `json:"preCommitEffectiveVolumes,omitempty"`
	}{
		Aux: Aux(tx),
		Postings: Map(tx.Postings, func(p ledger.Posting) posting {
			return posting(p)
		}),
		PostCommitVolumes:          postCommitVolumes(tx.PostCommitVolumes),
		PostCommitEffectiveVolumes: postCommitVolumes(tx.PostCommitEffectiveVolumes),
		Reverted:                   tx.RevertedAt != nil && !tx.RevertedAt.IsZero(),
		PreCommitVolumes: postCommitVolumes(
			tx.PostCommitVolumes.SubtractPostings(tx.Postings),
		),
		PreCommitEffectiveVolumes: postCommitVolumes(
			tx.PostCommitEffectiveVolumes.SubtractPostings(tx.Postings),
		),
	})
}

func renderTransaction(r *http.Request, tx ledger.Transaction) any {
	if !needBigIntAsString(r) {
		return tx
	}

	return transaction(tx)
}

type volumesWithBalanceByAssetByAccount ledger.VolumesWithBalanceByAssetByAccount

func (v volumesWithBalanceByAssetByAccount) MarshalJSON() ([]byte, error) {
	type Aux volumesWithBalanceByAssetByAccount
	return json.Marshal(struct {
		Aux
		Input   string `json:"input"`
		Output  string `json:"output"`
		Balance string `json:"balance"`
	}{
		Aux:     Aux(v),
		Input:   v.Input.String(),
		Output:  v.Output.String(),
		Balance: v.Balance.String(),
	})
}

func renderVolumesWithBalances(r *http.Request, volumes ledger.VolumesWithBalanceByAssetByAccount) any {
	if !needBigIntAsString(r) {
		return volumes
	}

	return volumesWithBalanceByAssetByAccount(volumes)
}

type account ledger.Account

func (v account) MarshalJSON() ([]byte, error) {
	type Aux account
	return json.Marshal(struct {
		Aux
		Volumes          volumesByAssets `json:"volumes,omitempty"`
		EffectiveVolumes volumesByAssets `json:"effectiveVolumes,omitempty"`
	}{
		Aux:              Aux(v),
		Volumes:          volumesByAssets(v.Volumes),
		EffectiveVolumes: volumesByAssets(v.EffectiveVolumes),
	})
}

func renderAccount(r *http.Request, v ledger.Account) any {
	if !needBigIntAsString(r) {
		return v
	}

	return account(v)
}

func renderSchema(r *http.Request, v ledger.Schema) any {
	return v
}

type balancesByAssets ledger.BalancesByAssets

func (v balancesByAssets) MarshalJSON() ([]byte, error) {
	return json.Marshal(ConvertMap(v, func(v *big.Int) string {
		return v.String()
	}))
}

func renderBalancesByAssets(r *http.Request, v ledger.BalancesByAssets) any {
	if !needBigIntAsString(r) {
		return v
	}

	return balancesByAssets(v)
}

type createdTransaction ledger.CreatedTransaction

func (tx createdTransaction) MarshalJSON() ([]byte, error) {
	type Aux ledger.CreatedTransaction
	return json.Marshal(struct {
		Aux
		Transaction transaction `json:"transaction"`
	}{
		Aux:         Aux(tx),
		Transaction: transaction(tx.Transaction),
	})
}

type revertedTransaction ledger.RevertedTransaction

func (tx revertedTransaction) MarshalJSON() ([]byte, error) {
	type Aux ledger.RevertedTransaction
	return json.Marshal(struct {
		Aux
		RevertedTransaction transaction `json:"revertedTransaction"`
		RevertTransaction   transaction `json:"transaction"`
	}{
		Aux:                 Aux(tx),
		RevertedTransaction: transaction(tx.RevertedTransaction),
		RevertTransaction:   transaction(tx.RevertTransaction),
	})
}

type log ledger.Log

func (l log) MarshalJSON() ([]byte, error) {
	type Aux ledger.Log
	return json.Marshal(struct {
		Aux
		Data any `json:"data"`
	}{
		Aux: Aux(l),
		Data: func() any {
			switch l.Type {
			case ledger.NewTransactionLogType:
				return createdTransaction(l.Data.(ledger.CreatedTransaction))
			case ledger.RevertedTransactionLogType:
				return revertedTransaction(l.Data.(ledger.RevertedTransaction))
			default:
				return l.Data
			}
		}(),
	})
}

func renderLog(r *http.Request, v ledger.Log) any {
	if !needBigIntAsString(r) {
		return v
	}

	return log(v)
}

func needBigIntAsString(r *http.Request) bool {
	v := strings.ToLower(r.Header.Get(HeaderBigIntAsString))
	return v == "true" || v == "yes" || v == "y" || v == "1"
}
