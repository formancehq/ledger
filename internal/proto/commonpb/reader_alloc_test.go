package commonpb_test

import (
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// BenchmarkReader_AsReader confirms that obtaining a Reader view is zero-alloc.
// The wrapper is a named-type over the concrete message (`type X Msg`) so
// AsReader() does a pointer type-conversion — the resulting *xReadonly fits in
// the interface data word and never escapes to the heap.
func BenchmarkReader_AsReader(b *testing.B) {
	tx := &commonpb.Transaction{
		Postings: []*commonpb.Posting{{Source: "world", Destination: "user:1", Asset: "USD"}},
	}

	b.ReportAllocs()
	for b.Loop() {
		_ = tx.AsReader()
	}
}

// BenchmarkReader_GetChain measures a descent through a message list. Singular
// AsReader conversions allocate nothing; boxing a ListReader slice into an
// interface can allocate when the compiler cannot inline the chain.
func BenchmarkReader_GetChain(b *testing.B) {
	tx := &commonpb.Transaction{
		Postings: []*commonpb.Posting{{Source: "world", Destination: "user:1", Asset: "USD"}},
	}

	b.ReportAllocs()
	for b.Loop() {
		r := tx.AsReader()
		list := r.GetPostings()
		p := list.Get(0)
		_ = p.GetSource()
		_ = p.GetDestination()
		_ = p.GetAsset()
	}
}

// BenchmarkReader_ListRange confirms that ListReader.Range yields Reader views
// without per-element allocation.
func BenchmarkReader_ListRange(b *testing.B) {
	postings := make([]*commonpb.Posting, 32)
	for i := range postings {
		postings[i] = &commonpb.Posting{Source: "world", Destination: "user:1", Asset: "USD"}
	}
	tx := &commonpb.Transaction{Postings: postings}

	b.ReportAllocs()
	for b.Loop() {
		var acc int
		tx.AsReader().GetPostings().Range(func(_ int, p commonpb.PostingReader) bool {
			acc += len(p.GetSource())

			return true
		})
		_ = acc
	}
}
