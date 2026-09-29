package main

import (
	"fmt"
	"testing"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
	"github.com/stretchr/testify/require"
)

// Benchmarks for the serialization search at the workload's steady-state scale:
// defaultLedgers ledgers, each carrying a chart, a txEmitStop-deep transaction
// log, a volume table at the pool shape, and account metadata. Each search node
// pays one full-state Hash (the dedup key) plus up to 1+len(inflight) Apply
// calls (each cloning the whole state); the component benchmarks isolate those
// two walks.

const (
	benchPrefixes     = 12
	benchIDsPerPrefix = 100
)

func benchAddr(i int) string {
	return fmt.Sprintf("t-%d:%d", i%benchPrefixes, (i/benchPrefixes)%benchIDsPerPrefix)
}

func addTypeReqL(ledger string, prefix int) *commonpb.Request {
	name := fmt.Sprintf("t-%d", prefix)

	return &commonpb.Request{
		Type: &commonpb.Request_AddAccountType{
			AddAccountType: &commonpb.AddAccountTypeLedgerRequest{
				Ledger: ledger,
				AccountType: &commonpb.AccountType{
					Name:    name,
					Pattern: name + ":{id}",
				},
			},
		},
	}
}

func saveAccountMetaReqL(ledger, addr, key, val string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &commonpb.LedgerAction{
					Data: &commonpb.LedgerAction_AddMetadata{
						AddMetadata: &commonpb.SaveMetadataCommand{
							Target: accountTarget(addr),
							Metadata: map[string]*commonpb.MetadataValue{
								key: {Type: &commonpb.MetadataValue_StringValue{StringValue: val}},
							},
						},
					},
				},
			},
		},
	}
}

// buildLoadedChecker seeds a checker with a steady-state-sized model: each
// ledger gets a benchPrefixes-type chart, then txPerLedger committed
// transactions cycling the address pool and assets (half referenced, so
// txByRef grows too), with account metadata on every 4th one.
func buildLoadedChecker(tb testing.TB, nLedgers, txPerLedger int) *Checker {
	tb.Helper()

	ledgers := make([]string, nLedgers)
	for i := range ledgers {
		ledgers[i] = fmt.Sprintf("bench-%d", i)
	}

	c := NewChecker(ledgers, nil)

	state := c.modelState
	apply := func(reqs []*commonpb.Request) {
		res := state.Apply(oracle.Bulk{Requests: reqs})
		require.True(tb, res.OK, "bench setup bulk rejected: %s", res.Reason)
		state = res.State
	}

	for _, ledger := range ledgers {
		chart := make([]*commonpb.Request, 0, benchPrefixes)
		for p := range benchPrefixes {
			chart = append(chart, addTypeReqL(ledger, p))
		}
		apply(chart)

		const batch = 250
		for start := 0; start < txPerLedger; start += batch {
			reqs := make([]*commonpb.Request, 0, batch+batch/4)
			for i := start; i < min(start+batch, txPerLedger); i++ {
				addr := benchAddr(i)
				asset := assets[i%len(assets)]

				if i%2 == 0 {
					reqs = append(reqs, oracletest.TxReqRefL(ledger, fmt.Sprintf("%s-ref-%d", ledger, i), "world", addr, asset, 5))
				} else {
					reqs = append(reqs, oracletest.TxReqL(ledger, "world", addr, asset, 5))
				}

				if i%4 == 0 {
					reqs = append(reqs, saveAccountMetaReqL(ledger, addr, fmt.Sprintf("k%d", i%numMetaKeys), "v"))
				}
			}
			apply(reqs)
		}
	}

	c.modelState = state

	return c
}

func benchBulk(ledger, addr, asset string) oracle.Bulk {
	return oracle.Bulk{Requests: []*commonpb.Request{
		oracletest.TxReqL(ledger, "world", addr, asset, 1),
	}}
}

// loadOutstanding registers nPending observed-but-undrained successes and
// nInflight dispatched bulks, mirroring a busy 8-worker steady state. Bulks
// touch distinct cells so they commute — the search's worst case, since
// success-gating prunes nothing and every inflight subset is explored.
func loadOutstanding(c *Checker, nPending, nInflight int) (maxTicket uint64) {
	ticket := uint64(0)

	for i := range nPending {
		ticket++
		b := benchBulk(c.ledgerNames[i%len(c.ledgerNames)], fmt.Sprintf("t-0:%d", i), "USD/2")
		c.pending = append(c.pending, &pendingObservation{minSeq: ticket, obs: observation{ticket: ticket, bulk: b}})
	}

	for i := range nInflight {
		ticket++
		c.inflight[ticket] = benchBulk(c.ledgerNames[i%len(c.ledgerNames)], fmt.Sprintf("t-1:%d", i), "EUR/2")
	}

	return ticket
}

// BenchmarkCandidateBasesFull is the failure-validation worst case: no base
// explains the observation, so the search enumerates every distinct
// (state, pending-prefix, inflight-subset) node.
func BenchmarkCandidateBasesFull(b *testing.B) {
	c := buildLoadedChecker(b, defaultLedgers, txEmitStop)
	maxTicket := loadOutstanding(c, 6, 4)

	bases := 0

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		bases = 0
		c.candidateBases(maxTicket, func(oracle.GlobalState) bool {
			bases++

			return false
		})
	}
	b.ReportMetric(float64(bases), "bases")
}

// BenchmarkApplySmallBulk isolates one search step: Apply of a single-posting
// bulk, dominated by GlobalState.clone.
func BenchmarkApplySmallBulk(b *testing.B) {
	c := buildLoadedChecker(b, defaultLedgers, txEmitStop)
	bulk := benchBulk(c.ledgerNames[0], "t-0:0", "USD/2")

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		res := c.modelState.Apply(bulk)
		if !res.OK {
			b.Fatal(res.Reason)
		}
	}
}

// BenchmarkStateFingerprint isolates the other search step: the dedup
// fingerprint (maintained incrementally, read per search node).
func BenchmarkStateFingerprint(b *testing.B) {
	c := buildLoadedChecker(b, defaultLedgers, txEmitStop)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c.modelState.Fingerprint()
	}
}
