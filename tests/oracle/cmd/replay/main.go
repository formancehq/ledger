// Command replay re-applies a captured commit stream through the model oracle.
//
// It reads the "[batch-dump] ticket=.. seq=.. outcome=.. b64=.." lines the
// singleton_driver_model driver emits under MODEL_DUMP_BATCHES=1 (e.g. from an
// Antithesis run's workload log): every submitted bulk with its dispatch ticket,
// server outcome (OK / a rejection reason / TRANSIENT), and — for committed
// bulks — its min committed log sequence. Two modes:
//
//   - committed (default): fold the committed (OK) bulks in log-sequence order —
//     the server's true serialization even under concurrency. A bulk the model
//     rejects is surfaced, and each touched ledger's final state is printed.
//   - --submits: fold every bulk in dispatch-ticket order and compare each
//     bulk's model outcome to the server's. Valid only for a single-worker run,
//     where dispatch order IS the serialization; the first mismatch is the bug.
//
// Usage: replay <dump-file> [target-ledger] [--submits]
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
	ticket  uint64 // dispatch order (single-worker serialization)
	seq     uint64 // min committed log sequence (server serialization); 0 if not committed
	outcome string // "OK", a rejection reason, or "TRANSIENT" ("" for legacy dumps)
	req     *servicepb.ApplyRequest
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: replay <dump-file> [target-ledger] [--submits]")
		os.Exit(2)
	}

	target := ""
	submitsMode := false
	for _, a := range os.Args[2:] {
		if a == "--submits" {
			submitsMode = true
		} else {
			target = a
		}
	}

	batches, err := parse(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if submitsMode {
		replaySubmits(batches, target)
	} else {
		replayCommitted(batches, target)
	}
}

// replaySubmits folds the submitted stream through a fresh oracle in dispatch
// order and reports every bulk whose model outcome differs from the server's.
func replaySubmits(submits []batch, target string) {
	sort.SliceStable(submits, func(i, j int) bool { return submits[i].ticket < submits[j].ticket })
	fmt.Printf("parsed %d submitted bulks (trace ledger=%q)\n\n", len(submits), target)

	gs := oracle.NewGlobalState()
	mismatches := 0

	for _, b := range submits {
		if b.outcome == "TRANSIENT" {
			continue // the model drops transient/uncertain outcomes (processor.go)
		}

		ab, err := servicepb.PeekBatch(b.req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "seq=%d PeekBatch: %v\n", b.seq, err)
			os.Exit(1)
		}
		bulk := oracle.Bulk{Requests: ab.GetRequests()}

		touchesTarget := false
		for _, r := range bulk.Requests {
			if oracle.LedgerOf(r) == target {
				touchesTarget = true
			}
		}

		res := gs.Apply(bulk)
		model := "OK"
		if !res.OK {
			model = res.Reason
		}

		if model != b.outcome {
			mismatches++
			fmt.Printf("ticket=%-6d MISMATCH server=%s model=%s\n", b.ticket, b.outcome, model)
			fmt.Printf("            postings=%s\n", renderPostings(bulk))
			fmt.Printf("            kinds=%s\n", renderKinds(bulk))
			fmt.Printf("            modelTypes=%s\n\n", renderTypes(gs, bulk))
		}

		if res.OK {
			gs = res.State
		}

		if target != "" && touchesTarget {
			fmt.Printf("  trace ticket=%-6d srv=%-22s mdl=%-22s kinds=%-26s typesAfter=%-14s postings=%s\n",
				b.ticket, b.outcome, model, renderKinds(bulk), typeNames(gs, target), renderPostings(bulk))
		}
	}

	fmt.Printf("\n%d submitted bulks, %d model/server mismatches\n", len(submits), mismatches)
}

func typeNames(gs oracle.GlobalState, ledger string) string {
	var names []string
	for n := range gs.Ledger(ledger).Types() {
		names = append(names, n)
	}
	sort.Strings(names)

	return "[" + strings.Join(names, ",") + "]"
}

// replayCommitted folds the committed stream and prints the final per-ledger
// state, surfacing any committed bulk the model rejects.
func replayCommitted(batches []batch, target string) {
	sort.SliceStable(batches, func(i, j int) bool { return batches[i].seq < batches[j].seq })
	fmt.Printf("parsed %d committed batches\n", len(batches))

	gs := oracle.NewGlobalState()
	touched := map[string]bool{}
	rejected := 0

	for _, b := range batches {
		// Only committed bulks form the server's log-ordered serialization. Skip
		// failures/transients; a legacy dump carries no outcome — treat as
		// committed.
		if b.outcome != "" && b.outcome != "OK" {
			continue
		}

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

// parse reads "[batch-dump] key=value ..." lines, extracting the ticket, commit
// sequence, outcome, and b64-encoded ApplyRequest. Missing keys default to zero
// values (legacy dumps carry only seq= and b64=), so older captures still load.
func parse(path string) ([]batch, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer func() { _ = f.Close() }()

	var out []batch
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		_, after, ok := strings.Cut(sc.Text(), "[batch-dump] ")
		if !ok {
			continue
		}

		var (
			ticket, seq      uint64
			outcome, encoded string
		)
		for field := range strings.FieldsSeq(after) {
			k, v, ok := strings.Cut(field, "=")
			if !ok {
				continue
			}
			switch k {
			case "ticket":
				ticket, _ = strconv.ParseUint(v, 10, 64)
			case "seq":
				seq, _ = strconv.ParseUint(v, 10, 64)
			case "outcome":
				outcome = v
			case "b64":
				encoded = v
			}
		}

		if encoded == "" {
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			continue
		}
		req := &servicepb.ApplyRequest{}
		if err := req.UnmarshalVT(raw); err != nil {
			continue
		}

		out = append(out, batch{ticket: ticket, seq: seq, outcome: outcome, req: req})
	}

	return out, sc.Err()
}

func renderPostings(b oracle.Bulk) string {
	var ps []string
	for _, r := range b.Requests {
		if ct := r.GetApply().GetAction().GetCreateTransaction(); ct != nil {
			l := oracle.LedgerOf(r)
			for _, p := range ct.GetPostings() {
				ps = append(ps, fmt.Sprintf("%s:%s->%s(%s)", l, p.GetSource(), p.GetDestination(), p.GetAsset()))
			}
		}
	}

	return "[" + strings.Join(ps, " ") + "]"
}

func renderKinds(b oracle.Bulk) string {
	kinds := make([]string, 0, len(b.Requests))
	for _, r := range b.Requests {
		switch t := r.GetType().(type) {
		case *servicepb.Request_Apply:
			switch t.Apply.GetAction().GetData().(type) {
			case *servicepb.LedgerAction_CreateTransaction:
				kinds = append(kinds, "tx")
			case *servicepb.LedgerAction_AddMetadata:
				kinds = append(kinds, "addMeta")
			case *servicepb.LedgerAction_DeleteMetadata:
				kinds = append(kinds, "delMeta")
			case *servicepb.LedgerAction_RevertTransaction:
				kinds = append(kinds, "revert")
			default:
				kinds = append(kinds, "apply?")
			}
		case *servicepb.Request_AddAccountType:
			kinds = append(kinds, "+"+t.AddAccountType.GetAccountType().GetName())
		case *servicepb.Request_RemoveAccountType:
			kinds = append(kinds, "-"+t.RemoveAccountType.GetName())
		default:
			kinds = append(kinds, "other")
		}
	}

	return "[" + strings.Join(kinds, ",") + "]"
}

func renderTypes(gs oracle.GlobalState, b oracle.Bulk) string {
	ledgers := map[string]bool{}
	for _, r := range b.Requests {
		ledgers[oracle.LedgerOf(r)] = true
	}

	var out []string
	for l := range ledgers {
		var names []string
		for n := range gs.Ledger(l).Types() {
			names = append(names, n)
		}
		sort.Strings(names)
		out = append(out, l+"=["+strings.Join(names, ",")+"]")
	}
	sort.Strings(out)

	return "[" + strings.Join(out, " ") + "]"
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
