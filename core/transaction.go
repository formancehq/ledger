package core

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
