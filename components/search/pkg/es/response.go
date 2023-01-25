package es

import "encoding/json"

type ResponseShards struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Skipped    int `json:"skipped"`
	Failed     int `json:"failed"`
}

type ResponseHitsTotal struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

type ResponseHit struct {
	Index     string              `json:"_index"`
	Type      string              `json:"_type"`
	ID        string              `json:"_id"`
	Score     float64             `json:"_score"`
	Source    json.RawMessage     `json:"_source"`
	Fields    map[string][]string `json:"fields,omitempty"`
	InnerHits map[string]struct {
		Hits ResponseHits `json:"hits"`
	} `json:"inner_hits,omitempty"`
}

type ResponseHits struct {
	Total    ResponseHitsTotal `json:"total"`
	MaxScore float64           `json:"max_score"`
	Hits     []ResponseHit     `json:"hits"`
}

type Response struct {
	Took     int            `json:"took"`
	TimedOut bool           `json:"timed_out"`
	Shards   ResponseShards `json:"_shards"`
	Hits     ResponseHits   `json:"hits"`
}
