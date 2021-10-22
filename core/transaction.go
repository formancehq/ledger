package core

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

type Posting struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Amount      int64  `json:"amount"`
	Asset       string `json:"asset"`
}

type Transaction struct {
	ID        int64     `json:"txid"`
	Postings  []Posting `json:"postings"`
	Reference string    `json:"reference"`
	Timestamp string    `json:"timestamp"`
	Hash      string    `json:"hash"`
	Metadata  Metadata  `json:"metadata"`
}

func (t *Transaction) AppendPosting(p Posting) {
	t.Postings = append(t.Postings, p)
}

func (t *Transaction) Reverse() *Transaction {
	var reversedPostings []Posting
	for i := len(t.Postings) - 1; i >= 0; i-- {
		p := t.Postings[i]
		newPosting := Posting{
			Source:      p.Destination,
			Destination: p.Source,
			Amount:      p.Amount,
			Asset:       p.Asset,
		}
		reversedPostings = append(reversedPostings, newPosting)
	}
	t.Postings = reversedPostings
	return t
}

func Hash(t1 *Transaction, t2 *Transaction) string {
	b1, _ := json.Marshal(t1)
	b2, _ := json.Marshal(t2)

	h := sha256.New()
	h.Write(b1)
	h.Write(b2)

	return fmt.Sprintf("%x", h.Sum(nil))
}
