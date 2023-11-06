package ledger

import (
	"fmt"
	"math/big"

	"github.com/pkg/errors"

	"github.com/formancehq/stack/libs/go-libs/metadata"
)

type Transactions struct {
	Transactions []TransactionData `json:"transactions"`
}

type TransactionData struct {
	Postings  Postings          `json:"postings"`
	Metadata  metadata.Metadata `json:"metadata"`
	Timestamp Time              `json:"timestamp"`
	Reference string            `json:"reference,omitempty"`
}

func (d TransactionData) WithPostings(postings ...Posting) TransactionData {
	d.Postings = append(d.Postings, postings...)
	return d
}

func NewTransactionData() TransactionData {
	return TransactionData{
		Metadata: metadata.Metadata{},
	}
}

func (t *TransactionData) Reverse() TransactionData {
	postings := make(Postings, len(t.Postings))
	copy(postings, t.Postings)
	postings.Reverse()

	return TransactionData{
		Postings: postings,
	}
}

func (d TransactionData) WithDate(now Time) TransactionData {
	d.Timestamp = now

	return d
}

type Transaction struct {
	TransactionData
	ID       *big.Int `json:"id"`
	Reverted bool     `json:"reverted"`
}

func (t *Transaction) WithPostings(postings ...Posting) *Transaction {
	t.TransactionData = t.TransactionData.WithPostings(postings...)
	return t
}

func (t *Transaction) WithReference(ref string) *Transaction {
	t.Reference = ref
	return t
}

func (t *Transaction) WithDate(ts Time) *Transaction {
	t.Timestamp = ts
	return t
}

func (t *Transaction) WithIDUint64(id uint64) *Transaction {
	t.ID = big.NewInt(int64(id))
	return t
}

func (t *Transaction) WithID(id *big.Int) *Transaction {
	t.ID = id
	return t
}

func (t *Transaction) WithMetadata(m metadata.Metadata) *Transaction {
	t.Metadata = m
	return t
}

func NewTransaction() *Transaction {
	return &Transaction{
		ID: big.NewInt(0),
		TransactionData: NewTransactionData().
			WithDate(Now()),
	}
}

type ExpandedTransaction struct {
	Transaction
	PreCommitVolumes           AccountsAssetsVolumes `json:"preCommitVolumes,omitempty"`
	PostCommitVolumes          AccountsAssetsVolumes `json:"postCommitVolumes,omitempty"`
	PreCommitEffectiveVolumes  AccountsAssetsVolumes `json:"preCommitEffectiveVolumes,omitempty"`
	PostCommitEffectiveVolumes AccountsAssetsVolumes `json:"postCommitEffectiveVolumes,omitempty"`
}

func (t *ExpandedTransaction) AppendPosting(p Posting) {
	t.Postings = append(t.Postings, p)
}

func ExpandTransaction(tx *Transaction, preCommitVolumes AccountsAssetsVolumes) ExpandedTransaction {
	postCommitVolumes := preCommitVolumes.Copy()
	for _, posting := range tx.Postings {
		preCommitVolumes.AddInput(posting.Destination, posting.Asset, Zero)
		preCommitVolumes.AddOutput(posting.Source, posting.Asset, Zero)
		postCommitVolumes.AddOutput(posting.Source, posting.Asset, posting.Amount)
		postCommitVolumes.AddInput(posting.Destination, posting.Asset, posting.Amount)
	}
	return ExpandedTransaction{
		Transaction:       *tx,
		PreCommitVolumes:  preCommitVolumes,
		PostCommitVolumes: postCommitVolumes,
	}
}

type TransactionRequest struct {
	Postings  Postings          `json:"postings"`
	Script    ScriptV1          `json:"script"`
	Timestamp Time              `json:"timestamp"`
	Reference string            `json:"reference"`
	Metadata  metadata.Metadata `json:"metadata" swaggertype:"object"`
}

func (req *TransactionRequest) ToRunScript() (*RunScript, error) {

	if len(req.Postings) > 0 && req.Script.Plain != "" ||
		len(req.Postings) == 0 && req.Script.Plain == "" {
		return nil, errors.New("invalid payload: should contain either postings or script")
	}

	if len(req.Postings) > 0 {
		if i, err := req.Postings.Validate(); err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("invalid posting %d", i))
		}
		txData := TransactionData{
			Postings:  req.Postings,
			Timestamp: req.Timestamp,
			Reference: req.Reference,
			Metadata:  req.Metadata,
		}

		rs := TxToScriptData(txData)
		return &rs, nil
	}

	return &RunScript{
		Script:    req.Script.ToCore(),
		Timestamp: req.Timestamp,
		Reference: req.Reference,
		Metadata:  req.Metadata,
	}, nil
}
