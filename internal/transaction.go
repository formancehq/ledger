package ledger

import (
	"encoding/json"
	"github.com/formancehq/go-libs/v2/time"
	"github.com/invopop/jsonschema"
	"github.com/uptrace/bun"
	"math/big"
	"slices"
	"sort"

	"github.com/formancehq/go-libs/v2/metadata"
)

type Transactions struct {
	Transactions []TransactionData `json:"transactions"`
}

type TransactionData struct {
	Postings   Postings          `json:"postings" bun:"postings,type:jsonb"`
	Metadata   metadata.Metadata `json:"metadata" bun:"metadata,type:jsonb,default:'{}'"`
	Timestamp  time.Time         `json:"timestamp" bun:"timestamp,type:timestamp without time zone,nullzero"`
	Reference  string            `json:"reference,omitempty" bun:"reference,type:varchar,unique,nullzero"`
	InsertedAt time.Time         `json:"insertedAt,omitempty" bun:"inserted_at,type:timestamp without time zone,nullzero"`
}

func (data TransactionData) WithPostings(postings ...Posting) TransactionData {
	data.Postings = append(data.Postings, postings...)
	return data
}

func NewTransactionData() TransactionData {
	return TransactionData{
		Metadata: metadata.Metadata{},
	}
}

type Transaction struct {
	bun.BaseModel `bun:"table:transactions,alias:transactions"`

	TransactionData
	ID         int        `json:"id" bun:"id,type:numeric"`
	RevertedAt *time.Time `json:"revertedAt,omitempty" bun:"reverted_at,type:timestamp without time zone"`
	// PostCommitVolumes are the volumes of each account/asset after a transaction has been committed.
	// Those volumes will never change as those are computed in flight.
	PostCommitVolumes PostCommitVolumes `json:"postCommitVolumes,omitempty" bun:"post_commit_volumes,type:jsonb"`
	// PostCommitEffectiveVolumes are the volumes of each account/asset after the transaction TransactionData.Timestamp.
	// Those volumes are also computed in flight, but can be updated if a transaction is inserted in the past.
	PostCommitEffectiveVolumes PostCommitVolumes `json:"postCommitEffectiveVolumes,omitempty" bun:"post_commit_effective_volumes,type:jsonb,scanonly"`
}

func (Transaction) JSONSchemaExtend(schema *jsonschema.Schema) {
	schema.Properties.Set("reverted", &jsonschema.Schema{
		Type: "boolean",
	})
	postCommitVolumesSchema, _ := schema.Properties.Get("postCommitVolumes")
	schema.Properties.Set("preCommitVolumes", postCommitVolumesSchema)
	schema.Properties.Set("preCommitEffectiveVolumes", postCommitVolumesSchema)
}

func (tx Transaction) Reverse() Transaction {
	ret := NewTransaction().WithPostings(tx.Postings.Reverse()...)
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

func (tx Transaction) InvolvedDestinations() map[string][]string {
	ret := make(map[string][]string)
	for _, posting := range tx.Postings {
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

func (tx Transaction) VolumeUpdates() []AccountsVolumes {
	aggregatedVolumes := make(map[string]map[string][]Posting)
	for _, posting := range tx.Postings {
		if _, ok := aggregatedVolumes[posting.Source]; !ok {
			aggregatedVolumes[posting.Source] = make(map[string][]Posting)
		}
		aggregatedVolumes[posting.Source][posting.Asset] = append(aggregatedVolumes[posting.Source][posting.Asset], posting)

		if posting.Source == posting.Destination {
			continue
		}

		if _, ok := aggregatedVolumes[posting.Destination]; !ok {
			aggregatedVolumes[posting.Destination] = make(map[string][]Posting)
		}
		aggregatedVolumes[posting.Destination][posting.Asset] = append(aggregatedVolumes[posting.Destination][posting.Asset], posting)
	}

	ret := make([]AccountsVolumes, 0)
	for account, movesByAsset := range aggregatedVolumes {
		for asset, postings := range movesByAsset {
			volumes := NewEmptyVolumes()
			for _, posting := range postings {
				if account == posting.Source {
					volumes.Output.Add(volumes.Output, posting.Amount)
				}
				if account == posting.Destination {
					volumes.Input.Add(volumes.Input, posting.Amount)
				}
			}

			ret = append(ret, AccountsVolumes{
				Account: account,
				Asset:   asset,
				Input:   volumes.Input,
				Output:  volumes.Output,
			})
		}
	}

	slices.SortStableFunc(ret, func(a, b AccountsVolumes) int {
		switch {
		case a.Account < b.Account:
			return -1
		case a.Account > b.Account:
			return 1
		default:
			switch {
			case a.Asset < b.Asset:
				return -1
			case a.Asset > b.Asset:
				return 1
			default:
				return 0
			}
		}
	})

	return ret
}

func (tx Transaction) MarshalJSON() ([]byte, error) {
	type Aux Transaction
	type Ret struct {
		Aux

		Reverted                  bool              `json:"reverted"`
		PreCommitVolumes          PostCommitVolumes `json:"preCommitVolumes,omitempty"`
		PreCommitEffectiveVolumes PostCommitVolumes `json:"preCommitEffectiveVolumes,omitempty"`
	}

	var (
		preCommitVolumes          PostCommitVolumes
		preCommitEffectiveVolumes PostCommitVolumes
	)
	if len(tx.PostCommitVolumes) > 0 {
		preCommitVolumes = tx.PostCommitVolumes.Copy()
		for _, posting := range tx.Postings {
			preCommitVolumes.AddOutput(posting.Source, posting.Asset, big.NewInt(0).Neg(posting.Amount))
			preCommitVolumes.AddInput(posting.Destination, posting.Asset, big.NewInt(0).Neg(posting.Amount))
		}
	}
	if len(tx.PostCommitEffectiveVolumes) > 0 {
		preCommitEffectiveVolumes = tx.PostCommitEffectiveVolumes.Copy()
		for _, posting := range tx.Postings {
			preCommitEffectiveVolumes.AddOutput(posting.Source, posting.Asset, big.NewInt(0).Neg(posting.Amount))
			preCommitEffectiveVolumes.AddInput(posting.Destination, posting.Asset, big.NewInt(0).Neg(posting.Amount))
		}
	}

	return json.Marshal(&Ret{
		Aux:                       Aux(tx),
		Reverted:                  tx.RevertedAt != nil && !tx.RevertedAt.IsZero(),
		PreCommitVolumes:          preCommitVolumes,
		PreCommitEffectiveVolumes: preCommitEffectiveVolumes,
	})
}

func (tx Transaction) IsReverted() bool {
	return tx.RevertedAt != nil && !tx.RevertedAt.IsZero()
}

func (tx Transaction) WithRevertedAt(timestamp time.Time) Transaction {
	tx.RevertedAt = &timestamp
	return tx
}

func (tx Transaction) WithPostCommitEffectiveVolumes(volumes PostCommitVolumes) Transaction {
	tx.PostCommitEffectiveVolumes = volumes

	return tx
}

func NewTransaction() Transaction {
	return Transaction{
		TransactionData: NewTransactionData(),
	}
}
