package ledger

import (
	"encoding/json"
	"math/big"
	"slices"
	"sort"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/pointer"

	"github.com/formancehq/go-libs/metadata"
)

type Transactions struct {
	Transactions []TransactionData `json:"transactions"`
}

type TransactionData struct {
	Postings   Postings          `json:"postings"`
	Metadata   metadata.Metadata `json:"metadata"`
	Timestamp  time.Time         `json:"timestamp"`
	Reference  string            `json:"reference,omitempty"`
	InsertedAt time.Time         `json:"insertedAt,omitempty"`
}

func (data TransactionData) WithPostings(postings ...Posting) TransactionData {
	data.Postings = append(data.Postings, postings...)
	return data
}

func NewTransactionData() TransactionData {
	return TransactionData{
		Metadata:  metadata.Metadata{},
		Timestamp: time.Now(),
	}
}

type Transaction struct {
	TransactionData
	ID                         int               `json:"id"`
	Reverted                   bool              `json:"reverted"`
	PostCommitVolumes          PostCommitVolumes `json:"postCommitVolumes,omitempty"`
	PostCommitEffectiveVolumes PostCommitVolumes `json:"postCommitEffectiveVolumes,omitempty"`
}

func (tx Transaction) Reverse(atEffectiveDate bool) Transaction {
	ret := NewTransaction().WithPostings(tx.Postings.Reverse()...)
	if atEffectiveDate {
		ret = ret.WithTimestamp(tx.Timestamp)
	}

	return ret
}

func (tx Transaction) WithPostings(postings ...Posting) Transaction {
	tx.TransactionData = tx.TransactionData.WithPostings(postings...)
	return tx
}

func (tx Transaction) WithReference(ref string) Transaction {
	tx.Reference = ref
	return tx
}

func (tx Transaction) WithTimestamp(ts time.Time) Transaction {
	tx.Timestamp = ts
	return tx
}

func (tx Transaction) WithMetadata(m metadata.Metadata) Transaction {
	tx.Metadata = m
	return tx
}

func (tx Transaction) WithInsertedAt(date time.Time) Transaction {
	tx.InsertedAt = date

	return tx
}

func (tx Transaction) InvolvedAccountAndAssets() map[string][]string {
	ret := make(map[string][]string)
	for _, posting := range tx.Postings {
		ret[posting.Source] = append(ret[posting.Source], posting.Asset)
		ret[posting.Destination] = append(ret[posting.Destination], posting.Asset)
	}

	for account, assets := range ret {
		sort.Strings(assets)
		ret[account] = slices.Compact(assets)
	}

	return ret
}

func (tx Transaction) InvolvedAccounts() []string {
	ret := make([]string, 0)
	for _, posting := range tx.Postings {
		ret = append(ret, posting.Source, posting.Destination)
	}

	sort.Strings(ret)

	return slices.Compact(ret)
}

func (tx Transaction) MarshalJSON() ([]byte, error) {
	type Aux Transaction
	type Ret struct {
		Aux

		PreCommitVolumes          PostCommitVolumes `json:"preCommitVolumes,omitempty"`
		PreCommitEffectiveVolumes PostCommitVolumes `json:"preCommitEffectiveVolumes,omitempty"`
	}

	var (
		preCommitVolumes          PostCommitVolumes
		preCommitEffectiveVolumes PostCommitVolumes
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

	return json.Marshal(&Ret{
		Aux:                       Aux(tx),
		PreCommitVolumes:          preCommitVolumes,
		PreCommitEffectiveVolumes: preCommitEffectiveVolumes,
	})
}

func NewTransaction() Transaction {
	return Transaction{
		TransactionData: NewTransactionData(),
	}
}

type TransactionRequest struct {
	Postings  Postings          `json:"postings"`
	Script    ScriptV1          `json:"script"`
	Timestamp time.Time         `json:"timestamp"`
	Reference string            `json:"reference"`
	Metadata  metadata.Metadata `json:"metadata" swaggertype:"object"`
}

func (req *TransactionRequest) ToRunScript(allowUnboundedOverdrafts bool) *RunScript {

	if len(req.Postings) > 0 {
		txData := TransactionData{
			Postings:  req.Postings,
			Timestamp: req.Timestamp,
			Reference: req.Reference,
			Metadata:  req.Metadata,
		}

		return pointer.For(TxToScriptData(txData, allowUnboundedOverdrafts))
	}

	return &RunScript{
		Script:    req.Script.ToCore(),
		Timestamp: req.Timestamp,
		Reference: req.Reference,
		Metadata:  req.Metadata,
	}
}
