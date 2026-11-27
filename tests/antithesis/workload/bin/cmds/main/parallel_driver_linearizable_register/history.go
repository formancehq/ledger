package main

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// absent is the register value before any write: the account has no "v"
// metadata key. Every written value is non-empty, so "" is never ambiguous.
const absent = ""

// indeterminate is the return time of a write whose outcome is unknown. A
// write that may or may not have committed can take effect at any point after
// its invocation, including never; an open-ended interval lets the checker
// place it anywhere after Call, and placing it after every other operation is
// the same as it never having happened.
const indeterminate = math.MaxInt64

// regInput is one operation on one register. Reads carry no value.
type regInput struct {
	Key   int
	Write bool
	Value string
}

// opMeta records where an operation ran, for failure reports and for the
// cross-node reachability probe. It plays no part in the check itself.
type opMeta struct {
	Worker int
	Node   string
}

// registerModel is a read/write register with an absent initial value. A read
// must return the value of the last write linearized before it.
var registerModel = porcupine.Model{
	Init: func() any { return absent },
	Step: func(state, input, output any) (bool, any) {
		in := input.(regInput)
		if in.Write {
			return true, in.Value
		}

		return output.(string) == state.(string), state
	},
	DescribeOperation: func(input, output any) string {
		return describe(input.(regInput), output)
	},
	DescribeState: func(state any) string {
		return fmt.Sprintf("%q", state)
	},
}

func describe(in regInput, output any) string {
	if in.Write {
		return fmt.Sprintf("write(%d, %q)", in.Key, in.Value)
	}

	return fmt.Sprintf("read(%d) -> %q", in.Key, output)
}

// keyVerdict is the outcome of checking one register's history.
type keyVerdict struct {
	Key    int
	Result porcupine.CheckResult
	Ops    []porcupine.Operation
}

// checkHistory checks every register independently. Registers do not
// interact, so the history is linearizable if and only if each per-key
// sub-history is, and checking them apart keeps the search small. A key that
// times out is reported as Unknown, never folded into Ok.
func checkHistory(ops []porcupine.Operation, perKeyTimeout time.Duration) []keyVerdict {
	byKey := make(map[int][]porcupine.Operation)
	for _, op := range ops {
		k := op.Input.(regInput).Key
		byKey[k] = append(byKey[k], op)
	}

	keys := make([]int, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	verdicts := make([]keyVerdict, 0, len(keys))
	for _, k := range keys {
		verdicts = append(verdicts, keyVerdict{
			Key:    k,
			Result: porcupine.CheckOperationsTimeout(registerModel, pruneUnobserved(byKey[k]), perKeyTimeout),
			Ops:    byKey[k],
		})
	}

	return verdicts
}

// illegalHistories renders the history of every register whose verdict is
// Illegal, keyed by register, for a single failure report.
func illegalHistories(verdicts []keyVerdict) map[string][]string {
	out := make(map[string][]string)
	for _, v := range verdicts {
		if v.Result == porcupine.Illegal {
			out[fmt.Sprintf("key %d", v.Key)] = renderOps(v.Ops)
		}
	}

	return out
}

// pruneUnobserved drops indeterminate writes whose value no read of the same
// register returned. Such a write has an open-ended interval, so it can always
// be linearized after every other operation, where it constrains nothing:
// removing it cannot change the verdict. Keeping it only multiplies the
// orders the search must try, and enough of them turn a quick verdict into a
// timeout. Values are unique per write, so "observed" is exact. Completed
// writes always stay: they must take effect inside their interval.
func pruneUnobserved(ops []porcupine.Operation) []porcupine.Operation {
	observed := make(map[string]bool)
	for _, op := range ops {
		if !op.Input.(regInput).Write {
			observed[op.Output.(string)] = true
		}
	}

	kept := make([]porcupine.Operation, 0, len(ops))
	for _, op := range ops {
		in := op.Input.(regInput)
		if in.Write && op.Return == indeterminate && !observed[in.Value] {
			continue
		}
		kept = append(kept, op)
	}

	return kept
}

// renderOps turns a sub-history into lines sorted by invocation time, for the
// assertion details of a violation. Indeterminate writes show "?" as their
// return time.
func renderOps(ops []porcupine.Operation) []string {
	sorted := append([]porcupine.Operation(nil), ops...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Call < sorted[j].Call })

	lines := make([]string, 0, len(sorted))
	for _, op := range sorted {
		ret := fmt.Sprintf("%d", op.Return)
		if op.Return == indeterminate {
			ret = "?"
		}
		meta, _ := op.Metadata.(opMeta)
		lines = append(lines, fmt.Sprintf("[%d, %s] worker %d @ %s: %s",
			op.Call, ret, meta.Worker, meta.Node, describe(op.Input.(regInput), op.Output)))
	}

	return lines
}

// registerValue decodes the register from an account's metadata entry. A
// missing entry is absent. Only non-empty strings are ever written, so any
// other variant, or an empty string, is not a value of this register and is
// reported as not ok rather than folded into absent.
func registerValue(v *commonpb.MetadataValue) (string, bool) {
	if v == nil {
		return absent, true
	}
	s, ok := v.GetType().(*commonpb.MetadataValue_StringValue)
	if !ok || s.StringValue == absent {
		return "", false
	}

	return s.StringValue, true
}

// conclusive reports whether a run's check said something: every register
// was searched to an Ok verdict, and at least one successful read returned a
// written value. A history of writes alone, or of reads that only ever saw the
// initial state, passes trivially and must not count as coverage.
func conclusive(verdicts []keyVerdict, ops []porcupine.Operation) bool {
	if len(verdicts) == 0 {
		return false
	}
	for _, v := range verdicts {
		if v.Result != porcupine.Ok {
			return false
		}
	}
	for _, op := range ops {
		if !op.Input.(regInput).Write && op.Output.(string) != absent {
			return true
		}
	}

	return false
}

// readsAcrossNodes reports whether some read returned a value written through
// a different node. "Through" is the node the request was sent to, not the
// one that served it: a write sent to a follower commits on the leader, and a
// read sent to a follower may be forwarded. So this shows that requests
// received by different nodes were compared, not that a follower's own read
// barrier was exercised.
func readsAcrossNodes(ops []porcupine.Operation) bool {
	// Keyed by register and value: a read returning another register's value
	// is a violation, and must not also count as coverage.
	// The write must also have started by the time the read returned:
	// otherwise the read saw the value before it existed, which is a
	// violation too.
	type written struct {
		key   int
		value string
	}
	type origin struct {
		node string
		call int64
	}
	writtenOn := make(map[written]origin)
	for _, op := range ops {
		if in := op.Input.(regInput); in.Write {
			writtenOn[written{in.Key, in.Value}] = origin{op.Metadata.(opMeta).Node, op.Call}
		}
	}

	for _, op := range ops {
		in := op.Input.(regInput)
		if in.Write {
			continue
		}
		w, ok := writtenOn[written{in.Key, op.Output.(string)}]
		if ok && w.call <= op.Return && w.node != op.Metadata.(opMeta).Node {
			return true
		}
	}

	return false
}
