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

// ExtractCompactAccount extracts the minimal fields from a proto Account.
func ExtractCompactAccount(acc *commonpb.Account) CompactAccount {
	return CompactAccount{
		Address:      acc.Address,
		Assets:       collectAssets(acc),
		MetadataKeys: collectMetadataKeys(acc),
	}
}

// ExtractCompactTransaction extracts the minimal fields from a proto Transaction.
func ExtractCompactTransaction(tx *commonpb.Transaction) CompactTransaction {
	postings := make([]CompactPosting, len(tx.Postings))
	for i, p := range tx.Postings {
		postings[i] = CompactPosting{
			Source:      p.Source,
			Destination: p.Destination,
			Asset:       p.Asset,
			Amount:      p.Amount.ToBigInt(),
		}
	}

	var (
		ts          uint64
		hasTimestamp bool
	)
	if tx.Timestamp != nil {
		ts = tx.Timestamp.Data
		hasTimestamp = true
	}

	ct := CompactTransaction{
		Postings:     postings,
		Timestamp:    ts,
		HasTimestamp: hasTimestamp,
		Reverted:     tx.Reverted,
	}

	if tx.Metadata != nil {
		for _, m := range tx.Metadata.Metadata {
			ct.MetadataKeys = append(ct.MetadataKeys, m.Key)
		}
		sort.Strings(ct.MetadataKeys)
	}

	return ct
}
