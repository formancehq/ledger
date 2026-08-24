// Command serverreplay re-applies a captured commit stream against a LIVE
// ledger node, in log-sequence order — the server's true serialization.
//
// It reads the same "[batch-dump] ..." lines as cmd/replay, keeps the
// committed (OK, seq>0) bulks, creates every ledger the stream touches
// (empty initial schema — the capture does not carry setup), then Applies
// each bulk sequentially over gRPC. Every schema/index op on the trace
// ledger is printed with its request and the resulting log payloads, so a
// dropped_index divergence from the captured run is directly visible.
//
// Usage: serverreplay --addr 127.0.0.1:PORT --dump dumps.txt [--ledger name] [--stop-seq N]
package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

type batch struct {
	ticket  uint64
	seq     uint64
	outcome string
	req     *servicepb.ApplyRequest
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "ledger gRPC address")
	dump := flag.String("dump", "", "batch-dump capture file")
	ledger := flag.String("ledger", "", "trace ledger (print its schema/index ops verbosely)")
	stopSeq := flag.Uint64("stop-seq", 0, "stop after applying the bulk with this captured seq (0 = all)")
	flag.Parse()

	if *dump == "" {
		fmt.Fprintln(os.Stderr, "usage: serverreplay --addr host:port --dump file [--ledger name] [--stop-seq N]")
		os.Exit(2)
	}

	all, err := parse(*dump)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var committed []batch
	ledgers := map[string]bool{}
	for _, b := range all {
		pb, err := servicepb.PeekBatch(b.req)
		if err != nil {
			continue
		}
		for _, r := range pb.GetRequests() {
			if l := oracle.LedgerOf(r); l != "" {
				ledgers[l] = true
			}
		}
		if b.outcome == "OK" && b.seq > 0 {
			committed = append(committed, b)
		}
	}
	sort.SliceStable(committed, func(i, j int) bool { return committed[i].seq < committed[j].seq })
	fmt.Printf("parsed %d bulks, %d committed, %d ledgers touched\n", len(all), len(committed), len(ledgers))

	ctx := context.Background()
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial:", err)
		os.Exit(1)
	}
	client := servicepb.NewBucketServiceClient(conn)

	names := make([]string, 0, len(ledgers))
	for l := range ledgers {
		names = append(names, l)
	}
	sort.Strings(names)
	for _, l := range names {
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("create-"+l, &servicepb.Request{
			Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: l}},
		}))
		fmt.Printf("create ledger %s: err=%v\n", l, err)
	}

	mismatches := 0
	for i, b := range committed {
		interesting := describeSchemaOps(b.req, *ledger)

		var resp *servicepb.ApplyResponse
		var applyErr error
		for attempt := 0; attempt < 50; attempt++ {
			cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			resp, applyErr = client.Apply(cctx, b.req)
			cancel()
			if applyErr == nil {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}

		if applyErr != nil {
			mismatches++
			fmt.Printf("[%d/%d] seq=%d MISMATCH captured=OK got err=%v\n", i+1, len(committed), b.seq, applyErr)
		} else if interesting != "" {
			fmt.Printf("[%d/%d] seq=%d ticket=%d schema ops:\n%s", i+1, len(committed), b.seq, b.ticket, interesting)
			for _, lg := range resp.GetLogs() {
				apply := lg.GetPayload().GetApply()
				if apply == nil {
					continue
				}
				data := apply.GetLog().GetData()
				if rm := data.GetRemovedMetadataFieldType(); rm != nil {
					fmt.Printf("    -> log seq=%d ledger=%s REMOVED target=%v key=%s dropped_index=%v\n",
						lg.GetSequence(), apply.GetLedgerName(), rm.GetTargetType(), rm.GetKey(), rm.GetDroppedIndex())
				}
			}
		}

		if *stopSeq != 0 && b.seq >= *stopSeq {
			fmt.Printf("stopped at captured seq %d\n", b.seq)
			break
		}
	}

	fmt.Printf("done, %d outcome mismatches\n", mismatches)

	if *ledger != "" {
		st, err := client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{Ledger: *ledger})
		if err != nil {
			fmt.Printf("GetIndexStatus(%s): %v\n", *ledger, err)
			return
		}
		fmt.Printf("final index registry for %s:\n", *ledger)
		for _, e := range st.GetIndexes() {
			fmt.Printf("  %v\n", e)
		}
	}
}

// describeSchemaOps renders the schema/index requests in a bulk. Non-empty
// only when the bulk carries at least one and (ledger filter unset or the op
// targets it) — the caller uses it both as the interest test and the output.
func describeSchemaOps(req *servicepb.ApplyRequest, ledger string) string {
	pb, err := servicepb.PeekBatch(req)
	if err != nil {
		return ""
	}

	var sb strings.Builder
	for _, r := range pb.GetRequests() {
		switch r.GetType().(type) {
		case *servicepb.Request_SetMetadataFieldType, *servicepb.Request_RemoveMetadataFieldType,
			*servicepb.Request_CreateIndex, *servicepb.Request_DropIndex:
			if ledger != "" && oracle.LedgerOf(r) != ledger {
				continue
			}
			fmt.Fprintf(&sb, "    %s\n", r.String())
		}
	}

	return sb.String()
}

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
