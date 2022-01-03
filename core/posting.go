package core

// Posting struct
type Posting struct {
	Source      string `json:"source" validate:"required,source"`
	Destination string `json:"destination" validate:"required,destination"`
	Amount      int64  `json:"amount" validate:"required,min=1"`
	Asset       string `json:"asset" validate:"required,asset"`
}

// Postings struct
type Postings []Posting

// Reverse
func (ps Postings) Reverse() {
	if len(ps) == 1 {
		ps[0].Source, ps[0].Destination = ps[0].Destination, ps[0].Source
		return
	}
	for i := len(ps)/2 - 1; i >= 0; i-- {
		opp := len(ps) - 1 - i
		ps[i], ps[opp] = ps[opp], ps[i]
		ps[i].Source, ps[i].Destination = ps[i].Destination, ps[i].Source
		ps[opp].Source, ps[opp].Destination = ps[opp].Destination, ps[opp].Source
	}
}
