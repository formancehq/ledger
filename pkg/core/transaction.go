package core

import (
	"crypto/sha256"
	"fmt"
	"time"

	json "github.com/gibson042/canonicaljson-go"
)

type Transactions struct {
	Transactions []TransactionData `json:"transactions" binding:"required,dive"`
}

type TransactionData struct {
	Postings  Postings  `json:"postings"`
	Reference string    `json:"reference"`
	Metadata  Metadata  `json:"metadata" swaggertype:"object"`
	Timestamp time.Time `json:"timestamp"`
}

func (t *TransactionData) Reverse() TransactionData {
	postings := t.Postings
	postings.Reverse()

	ret := TransactionData{
		Postings: postings,
	}
	if t.Reference != "" {
		ret.Reference = "revert_" + t.Reference
	}
	return ret
}

var _ json.Marshaler = ExpandedTransaction{}

type Transaction struct {
	TransactionData
	ID uint64 `json:"txid"`
}

//func (r Transaction) ToTransaction() ExpandedTransaction {
//	return ExpandedTransaction{
//		Transaction: Transaction{
//			TransactionData: TransactionData{
//				Postings:  r.Postings,
//				Reference: r.Reference,
//				Metadata:  r.Metadata.ToMetadata(),
//				Timestamp: r.Timestamp,
//			},
//			ID:                r.ID,
//		},
//		PreCommitVolumes:  AccountsAssetsVolumes{},
//		PostCommitVolumes: AccountsAssetsVolumes{},
//	}
//}

type ExpandedTransaction struct {
	Transaction
	PreCommitVolumes  AccountsAssetsVolumes `json:"preCommitVolumes,omitempty"`  // Keep omitempty to keep consistent hash
	PostCommitVolumes AccountsAssetsVolumes `json:"postCommitVolumes,omitempty"` // Keep omitempty to keep consistent hash
}

//func (t ExpandedTransaction) raw() Transaction {
//	metadata := make(map[string]interface{})
//	for k, v := range t.Metadata {
//		var i interface{}
//		err := json.Unmarshal(v, &i)
//		if err != nil {
//			panic(err)
//		}
//		metadata[k] = i
//	}
//	return Transaction{
//		Postings:  t.Postings,
//		Reference: t.Reference,
//		Metadata:  metadata,
//		ID:        t.ID,
//		Timestamp: t.Timestamp,
//	}
//}

func (t ExpandedTransaction) MarshalJSON() ([]byte, error) {
	type transaction ExpandedTransaction
	return json.Marshal(struct {
		transaction
		Timestamp string `json:"timestamp"`
	}{
		transaction: transaction(t),
		// The std lib format time as RFC3339Nano, use a custom encoding to ensure backward compatibility
		Timestamp: t.Timestamp.Format(time.RFC3339),
	})
}

func (t *ExpandedTransaction) AppendPosting(p Posting) {
	t.Postings = append(t.Postings, p)
}

func (t *ExpandedTransaction) IsReverted() bool {
	if _, ok := t.Metadata[RevertedMetadataSpecKey()]; ok {
		return true
	}
	return false
}

func Hash(t1, t2 interface{}) string {
	b1, err := json.Marshal(t1)
	if err != nil {
		panic(err)
	}

	b2, err := json.Marshal(t2)
	if err != nil {
		panic(err)
	}

	h := sha256.New()
	_, err = h.Write(b1)
	if err != nil {
		panic(err)
	}
	_, err = h.Write(b2)
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}

func CheckHash(logs ...Log) (int, bool) {
	for i := len(logs) - 1; i >= 0; i-- {
		var lastLog *Log
		if i < len(logs)-1 {
			lastLog = &logs[i+1]
		}
		log := logs[i]
		log.Hash = ""
		h := Hash(lastLog, log)
		if logs[i].Hash != h {
			return i, false
		}
	}
	return 0, true
}
