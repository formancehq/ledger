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
		bulk := oracle.Bulk{Requests: ab.GetRequests(), IdempotencyKey: ab.GetIdempotencyKey()}

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
	for n := range gs.Ledger(ledger).Types().All() {
		names = append(names, n)
	}

	return "[" + strings.Join(names, ",") + "]"
}

// replayCommitted folds the committed stream and prints the final per-ledger
// state, surfacing any committed bulk the model rejects.
func replayCommitted(batches []batch, target string) {
	sort.SliceStable(batches, func(i, j int) bool { return batches[i].seq < batches[j].seq })
	fmt.Printf("parsed %d committed batches\n", len(batches))

	// MODEL_TRACE_SEQ=N stops the fold once a bulk's sequence exceeds N, so the
	// final-state dumps reflect the committed prefix at that point (e.g. the
	// sequence a finding was pinned to) rather than the whole log.
	var traceSeq uint64
	if s := os.Getenv("MODEL_TRACE_SEQ"); s != "" {
		traceSeq, _ = strconv.ParseUint(s, 10, 64)
	}

	traceType := os.Getenv("MODEL_TRACE_TYPE")
	traceAcct := os.Getenv("MODEL_TRACE_ACCOUNT")

	// MODEL_TRACE_TX=N: log every CreateIndex/DropIndex on the target ledger,
	// the bulk that committed transaction N, and — at the end — the folded
	// record's postings and account→tx index membership.
	var traceTx uint64
	if s := os.Getenv("MODEL_TRACE_TX"); s != "" {
		traceTx, _ = strconv.ParseUint(s, 10, 64)
	}

	gs := oracle.NewGlobalState()
	touched := map[string]bool{}
	rejected := 0

	for _, b := range batches {
		if traceSeq > 0 && b.seq > traceSeq {
			break
		}

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
		bulk := oracle.Bulk{Requests: ab.GetRequests(), IdempotencyKey: ab.GetIdempotencyKey()}

		hitsTarget := false
		for _, r := range bulk.Requests {
			l := oracle.LedgerOf(r)
			touched[l] = true
			if l == target {
				hitsTarget = true
			}
		}

		// MODEL_TRACE_TYPE=<name>: log every committed Add/RemoveAccountType for
		// that type on the target ledger, to trace how its persistence evolves.
		if traceType != "" {
			for _, r := range bulk.Requests {
				if target != "" && oracle.LedgerOf(r) != target {
					continue
				}
				switch x := r.GetType().(type) {
				case *servicepb.Request_AddAccountType:
					at := x.AddAccountType.GetAccountType()
					if at.GetName() == traceType {
						fmt.Printf("seq=%-6d ADD    %s persistence=%d pattern=%s\n", b.seq, traceType, at.GetPersistence(), at.GetPattern())
					}
				case *servicepb.Request_RemoveAccountType:
					if x.RemoveAccountType.GetName() == traceType {
						fmt.Printf("seq=%-6d REMOVE %s\n", b.seq, traceType)
					}
				}
			}
		}

		var preTxCount int
		if traceTx > 0 && target != "" {
			preTxCount = gs.Ledger(target).Txs().Len()

			for _, r := range bulk.Requests {
				switch x := r.GetType().(type) {
				case *servicepb.Request_CreateIndex:
					if x.CreateIndex.GetLedger() == target {
						fmt.Printf("seq=%-6d CREATE-INDEX %v\n", b.seq, x.CreateIndex.GetId())
					}
				case *servicepb.Request_DropIndex:
					if x.DropIndex.GetLedger() == target {
						fmt.Printf("seq=%-6d DROP-INDEX   %v\n", b.seq, x.DropIndex.GetId())
					}
				}
			}
		}

		res := gs.Apply(bulk)
		if !res.OK {
			rejected++
			fmt.Printf("seq=%-6d MODEL REJECTED committed bulk: %s\n", b.seq, res.Reason)

			continue
		}
		gs = res.State

		if traceTx > 0 && target != "" {
			post := gs.Ledger(target).Txs().Len()
			if uint64(preTxCount) < traceTx && uint64(post) >= traceTx {
				fmt.Printf("seq=%-6d TX %d COMMITTED (bulk kinds=%s postings=%s)\n", b.seq, traceTx, renderKinds(bulk), renderPostings(bulk))
			}
		}

		// Per-bulk account trace: any committed bulk whose postings touch the
		// traced account prints the account's cells right after apply, plus
		// whether the end-of-bulk chart still holds each type. Metadata ops
		// targeting the account and reverts of transactions that touched it
		// count as touches too.
		if traceAcct != "" {
			for _, r := range bulk.Requests {
				hit := false
				kind := "tx"
				switch a := r.GetApply().GetAction().GetData().(type) {
				case *servicepb.LedgerAction_CreateTransaction:
					for _, p := range a.CreateTransaction.GetPostings() {
						if p.GetSource() == traceAcct || p.GetDestination() == traceAcct {
							hit = true
						}
					}
				case *servicepb.LedgerAction_AddMetadata:
					kind = "addMeta"
					hit = a.AddMetadata.GetTarget().GetAccount().GetAddr() == traceAcct
				case *servicepb.LedgerAction_DeleteMetadata:
					kind = "delMeta"
					hit = a.DeleteMetadata.GetTarget().GetAccount().GetAddr() == traceAcct
				case *servicepb.LedgerAction_RevertTransaction:
					kind = fmt.Sprintf("revert(%d)", a.RevertTransaction.GetTransactionId())
					l := oracle.LedgerOf(r)
					ls := gs.Ledger(l)
					if id := a.RevertTransaction.GetTransactionId(); id >= 1 && id <= uint64(ls.Txs().Len()) {
						for _, p := range ls.Txs().Get(int(id - 1)).Postings() {
							if p.GetSource() == traceAcct || p.GetDestination() == traceAcct {
								hit = true
							}
						}
					}
				}
				if !hit {
					continue
				}
				l := oracle.LedgerOf(r)
				ls := gs.Ledger(l)
				var cells []string
				for k, vp := range ls.Volumes().All() {
					if k.Address == traceAcct {
						cells = append(cells, fmt.Sprintf("%s in=%s out=%s", k.Asset, vp.Input.Dec(), vp.Output.Dec()))
					}
				}
				sort.Strings(cells)
				var metas []string
				for k, v := range ls.Metadata().All() {
					if k.Address == traceAcct {
						metas = append(metas, k.Key+"="+oracle.MetaValueString(v))
					}
				}
				sort.Strings(metas)
				var chart []string
				for name, t := range ls.Types().All() {
					chart = append(chart, fmt.Sprintf("%s|%d", name, t.Persistence))
				}
				sort.Strings(chart)
				fmt.Printf("seq=%-6d ticket=%-6d TOUCH %-10s %s postings=%s cellsAfter=[%s] metaAfter=[%s] chart=[%s]\n",
					b.seq, b.ticket, kind, traceAcct, renderPostings(bulk), strings.Join(cells, ", "), strings.Join(metas, ","), strings.Join(chart, ","))
			}
		}

		if hitsTarget {
			ls := gs.Ledger(target)
			fmt.Printf("seq=%-6d %s meta=%s types=%d\n", b.seq, target, renderMeta(ls.LedgerMeta()), ls.Types().Len())
		}
	}

	if traceTx > 0 && target != "" {
		txs := gs.Ledger(target).Txs()
		if traceTx <= uint64(txs.Len()) {
			rec := txs.Get(int(traceTx - 1))
			fmt.Printf("\nTX %d: ref=%q reverted=%v postings=%v\n", traceTx, rec.Reference(), rec.Reverted(), rec.Postings())
			fmt.Printf("TX %d indexedAddrs: ", traceTx)
			for addr, bits := range rec.IndexedAddrs() {
				fmt.Printf("%s=%02b ", addr, bits)
			}
			fmt.Println()
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
		fmt.Printf("  %s meta=%s types=%d\n", l, renderMeta(ls.LedgerMeta()), ls.Types().Len())
	}

	// MODEL_TRACE_ACCOUNT=<addr>: print the final committed volume cells the model
	// holds for that account across every ledger, and whether the account matches
	// a type (persistence). Lets a "server has this account, model doesn't" finding
	// be resolved: no cells means the model purged/never-recorded it.
	if acct := os.Getenv("MODEL_TRACE_ACCOUNT"); acct != "" {
		fmt.Printf("\ntrace account %q:\n", acct)
		for _, l := range names {
			ls := gs.Ledger(l)
			cells := 0
			for k, vp := range ls.Volumes().All() {
				if k.Address == acct {
					fmt.Printf("  %s  %s in=%s out=%s\n", l, k.Asset, vp.Input.Dec(), vp.Output.Dec())
					cells++
				}
			}
			if hits := postingHits(ls, acct); hits > 0 {
				fmt.Printf("  %s  cells=%d committedPostings=%d\n", l, cells, hits)

				var chart []string
				for name, t := range ls.Types().All() {
					chart = append(chart, fmt.Sprintf("%s=%s|%d", name, t.Pattern, t.Persistence))
				}
				sort.Strings(chart)
				fmt.Printf("  %s  chart=[%s]\n", l, strings.Join(chart, ","))
			}
		}
	}
}

// postingHits counts committed postings in ls touching acct on either side.
func postingHits(ls oracle.LedgerState, acct string) int {
	n := 0
	for _, rec := range ls.Txs().All() {
		for _, p := range rec.Postings() {
			if p.GetSource() == acct || p.GetDestination() == acct {
				n++
			}
		}
	}

	return n
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
		for n := range gs.Ledger(l).Types().All() {
			names = append(names, n)
		}
		out = append(out, l+"=["+strings.Join(names, ",")+"]")
	}
	sort.Strings(out)

	return "[" + strings.Join(out, " ") + "]"
}

func renderMeta(m oracle.Map[string, *commonpb.MetadataValue]) string {
	parts := make([]string, 0, m.Len())
	for k, v := range m.All() {
		parts = append(parts, k+"="+oracle.MetaValueString(v))
	}

	return "{" + strings.Join(parts, ",") + "}"
}
