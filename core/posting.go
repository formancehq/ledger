package core

import "regexp"

// Posting struct
type Posting struct {
	Source      string `json:"source" binding:"source"`
	Destination string `json:"destination" binding:"destination"`
	Amount      int64  `json:"amount" binding:"required,min=1"`
	Asset       string `json:"asset" binding:"asset"`
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

// IsValidSourceOrDestination
func IsValidSourceOrDestination(source string) bool {
	valid, _ := regexp.MatchString("^[a-zA-Z_0-9]+(:[a-zA-Z_0-9]+){0,}$", source)
	return valid
}
