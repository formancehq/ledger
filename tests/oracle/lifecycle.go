package oracle

import "github.com/formancehq/ledger/v3/internal/proto/commonpb"

// LedgerLifecycle tracks explicitly created names independently of their data.
// Deletion removes the data projection but permanently reserves the name.
type LedgerLifecycle struct {
	Deleted      bool
	Mode         commonpb.LedgerMode
	MirrorSource *commonpb.MirrorSourceConfig
}

func lifecycleTerm(name string, lc LedgerLifecycle) Digest {
	t := newTerm("lifecycle")
	t.str(name)
	t.u64(uint64(lc.Mode))
	if lc.Deleted {
		t.u64(1)
	} else {
		t.u64(0)
	}
	if lc.MirrorSource != nil {
		t.u64(1)
		t.str(string(lc.MirrorSource.MarshalDeterministicVT(nil)))
	}

	return t.sum()
}
