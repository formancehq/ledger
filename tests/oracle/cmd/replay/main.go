// Command replay re-applies a captured commit stream through the model oracle.
//
// It reads the "[batch-dump] seq=.. b64=.." lines the singleton_driver_model
// driver emits under MODEL_DUMP_BATCHES=1 (e.g. from an Antithesis run's
// workload log), decodes each committed ApplyRequest, and folds them into a
// fresh oracle.GlobalState in commit order — the exact deterministic sequence
// the server applied. It prints each touched ledger's final model state and,
// when a target ledger is given, that ledger's (metadata, type count) at every
// committed prefix that touches it. A committed bulk the model rejects in
// commit order is a model/server divergence and is surfaced loudly.
//
// Usage: replay <dump-file> [target-ledger]
package main

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

type batch struct {
	seq uint64
	req *servicepb.ApplyRequest
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: replay <dump-file> [target-ledger]")
		os.Exit(2)
	}

	target := ""
	if len(os.Args) >= 3 {
		target = os.Args[2]
	}

	batches, err := parseDump(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	sort.SliceStable(batches, func(i, j int) bool { return batches[i].seq < batches[j].seq })
	fmt.Printf("parsed %d committed batches\n", len(batches))

	gs := oracle.NewGlobalState()
	touched := map[string]bool{}
	rejected := 0

	for _, b := range batches {
		ab, err := servicepb.PeekBatch(b.req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "seq=%d PeekBatch: %v\n", b.seq, err)
			os.Exit(1)
		}
		bulk := oracle.Bulk{Requests: ab.GetRequests()}

		hitsTarget := false
		for _, r := range bulk.Requests {
			l := oracle.LedgerOf(r)
			touched[l] = true
			if l == target {
				hitsTarget = true
			}
		}

		res := gs.Apply(bulk)
		if !res.OK {
			rejected++
			fmt.Printf("seq=%-6d MODEL REJECTED committed bulk: %s\n", b.seq, res.Reason)

			continue
		}
		gs = res.State

		if hitsTarget {
			ls := gs.Ledger(target)
			fmt.Printf("seq=%-6d %s meta=%s types=%d\n", b.seq, target, renderMeta(ls.LedgerMeta()), len(ls.Types()))
		}
	}

	fmt.Printf("\nfinal model state (%d committed bulks rejected):\n", rejected)
	names := make([]string, 0, len(touched))
	for l := range touched {
		names = append(names, l)
	}
	sort.Strings(names)
	for _, l := range names {
		ls := gs.Ledger(l)
		fmt.Printf("  %s meta=%s types=%d\n", l, renderMeta(ls.LedgerMeta()), len(ls.Types()))
	}
}

func parseDump(path string) ([]batch, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer func() { _ = f.Close() }()

	var batches []batch
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		_, after, ok := strings.Cut(line, "[batch-dump] seq=")
		if !ok {
			continue
		}
		rest := after
		fields := strings.SplitN(rest, " b64=", 2)
		if len(fields) != 2 {
			continue
		}
		seq, err := strconv.ParseUint(strings.TrimSpace(fields[0]), 10, 64)
		if err != nil {
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(fields[1]))
		if err != nil {
			continue
		}
		req := &servicepb.ApplyRequest{}
		if err := req.UnmarshalVT(raw); err != nil {
			continue
		}
		batches = append(batches, batch{seq: seq, req: req})
	}

	return batches, sc.Err()
}

func renderMeta(m map[string]*commonpb.MetadataValue) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + oracle.MetaValueString(m[k])
	}

	return "{" + strings.Join(parts, ",") + "}"
}
