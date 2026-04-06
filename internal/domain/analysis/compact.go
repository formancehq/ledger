package analysis

import (
	"math/big"
	"sort"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// CompactAccount holds only the fields needed for account analysis.
type CompactAccount struct {
	Address      string
	Assets       []string
	MetadataKeys []string
}

// CompactPosting holds only the fields needed for transaction analysis.
type CompactPosting struct {
	Source      string
	Destination string
	Asset       string
	Amount      *big.Int
}

// CompactTransaction holds only the fields needed for transaction analysis.
type CompactTransaction struct {
	Postings     []CompactPosting
	Timestamp    uint64 // microseconds since epoch (from commonpb.Timestamp.Data)
	HasTimestamp bool   // true when the source proto had a non-nil Timestamp
	Reverted     bool
	MetadataKeys []string
}

// ExtractCompactTransaction extracts the minimal fields from a proto Transaction.
func ExtractCompactTransaction(tx *commonpb.Transaction) CompactTransaction {
	postings := make([]CompactPosting, len(tx.GetPostings()))
	for i, p := range tx.GetPostings() {
		postings[i] = CompactPosting{
			Source:      p.GetSource(),
			Destination: p.GetDestination(),
			Asset:       p.GetAsset(),
			Amount:      p.GetAmount().ToBigInt(),
		}
	}

	var (
		ts           uint64
		hasTimestamp bool
	)
	if tx.GetTimestamp() != nil {
		ts = tx.GetTimestamp().GetData()
		hasTimestamp = true
	}

	ct := CompactTransaction{
		Postings:     postings,
		Timestamp:    ts,
		HasTimestamp: hasTimestamp,
		Reverted:     tx.GetReverted(),
	}

	if tx.GetMetadata() != nil {
		for _, m := range tx.GetMetadata().GetMetadata() {
			ct.MetadataKeys = append(ct.MetadataKeys, m.GetKey())
		}

		sort.Strings(ct.MetadataKeys)
	}

	return ct
}
