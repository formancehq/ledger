package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// MODEL_DEBUG=1 enables verbose per-transaction logging.
var modelDebug = os.Getenv("MODEL_DEBUG") != ""

func dbg(format string, args ...any) {
	if modelDebug {
		log.Printf("[model-debug] "+format, args...)
	}
}

// MODEL_DUMP_BATCHES=1 dumps every submitted ApplyRequest (base64 vtproto) with
// its dispatch ticket, the server's outcome (OK / a rejection reason / TRANSIENT),
// and — for committed bulks — its min committed log sequence. The replay tool
// (tests/oracle/cmd/replay) uses this two ways: sorted by log sequence over the
// committed (OK) bulks it reconstructs a concurrent run's true serialization;
// sorted by dispatch ticket over all bulks — valid only single-worker, where
// dispatch order IS the serialization — it compares each bulk's model outcome to
// the server's, reproducing a divergence deterministically.
var dumpBatches = os.Getenv("MODEL_DUMP_BATCHES") != ""

func dumpBatch(ticket uint64, req *servicepb.ApplyRequest, resp *servicepb.ApplyResponse, err error) {
	if !dumpBatches {
		return
	}

	outcome := "OK"
	var seq uint64
	switch {
	case err != nil && (internal.IsTransient(err) || isShutdownError(err)):
		outcome = "TRANSIENT"
	case err != nil:
		if r := internal.ErrorReason(err); r != "" {
			outcome = r
		} else {
			outcome = "ERR:" + status.Code(err).String()
		}
	default:
		seq = minLogSequence(resp.GetLogs())
	}

	b, mErr := req.MarshalVT()
	if mErr != nil {
		return
	}

	log.Printf("[batch-dump] ticket=%d seq=%d outcome=%s b64=%s", ticket, seq, outcome, base64.StdEncoding.EncodeToString(b))
}

// Distinct ledgers a bulk touches, sorted — for debug lines.
func bulkLedgers(b oracle.Bulk) string {
	seen := map[string]bool{}
	var names []string
	for _, r := range b.Requests {
		l := oracle.LedgerOf(r)
		if !seen[l] {
			seen[l] = true
			names = append(names, l)
		}
	}
	sort.Strings(names)

	return "[" + strings.Join(names, ",") + "]"
}

func requestKinds(b oracle.Bulk) string {
	parts := make([]string, len(b.Requests))

	for i, r := range b.Requests {
		switch r.GetType().(type) {
		case *servicepb.Request_Apply:
			switch r.GetApply().GetAction().GetData().(type) {
			case *servicepb.LedgerAction_CreateTransaction:
				parts[i] = "tx"
			case *servicepb.LedgerAction_AddMetadata:
				parts[i] = "addMeta"
			case *servicepb.LedgerAction_DeleteMetadata:
				parts[i] = "delMeta"
			case *servicepb.LedgerAction_RevertTransaction:
				parts[i] = "revert"
			default:
				parts[i] = "apply?"
			}
		case *servicepb.Request_AddAccountType:
			parts[i] = "addType"
		case *servicepb.Request_RemoveAccountType:
			parts[i] = "removeType"
		case *servicepb.Request_SaveLedgerMetadata:
			parts[i] = "saveLedgerMeta"
		case *servicepb.Request_DeleteLedgerMetadata:
			parts[i] = "delLedgerMeta"
		case *servicepb.Request_SetMetadataFieldType:
			parts[i] = "setFieldType"
		case *servicepb.Request_RemoveMetadataFieldType:
			parts[i] = "removeFieldType"
		default:
			parts[i] = "other"
		}
	}

	return "[" + strings.Join(parts, ",") + "]"
}

// Metadata targets a bulk touches, for debug: account (add addr{k=v,...} /
// del addr/key) and ledger (saveL ledger{k=v,...} / delL ledger/key).
func bulkMeta(b oracle.Bulk) string {
	kvList := func(m map[string]*commonpb.MetadataValue) string {
		kvs := make([]string, 0, len(m))
		for k, v := range m {
			kvs = append(kvs, k+"="+oracle.MetaValueString(v))
		}
		sort.Strings(kvs)
		return strings.Join(kvs, ",")
	}

	var parts []string
	for _, r := range b.Requests {
		switch t := r.GetType().(type) {
		case *servicepb.Request_Apply:
			switch a := t.Apply.GetAction().GetData().(type) {
			case *servicepb.LedgerAction_CreateTransaction:
				ct := a.CreateTransaction
				if ct.GetReference() != "" || len(ct.GetMetadata()) > 0 {
					parts = append(parts, fmt.Sprintf("newtx:%s{%s}", ct.GetReference(), kvList(ct.GetMetadata())))
				}
			case *servicepb.LedgerAction_AddMetadata:
				parts = append(parts, fmt.Sprintf("add %s{%s}", metaTargetLabel(a.AddMetadata.GetTarget()), kvList(a.AddMetadata.GetMetadata())))
			case *servicepb.LedgerAction_DeleteMetadata:
				parts = append(parts, fmt.Sprintf("del %s/%s", metaTargetLabel(a.DeleteMetadata.GetTarget()), a.DeleteMetadata.GetKey()))
			}
		case *servicepb.Request_SaveLedgerMetadata:
			parts = append(parts, fmt.Sprintf("saveL %s{%s}", t.SaveLedgerMetadata.GetLedger(), kvList(t.SaveLedgerMetadata.GetMetadata())))
		case *servicepb.Request_DeleteLedgerMetadata:
			parts = append(parts, fmt.Sprintf("delL %s/%s", t.DeleteLedgerMetadata.GetLedger(), t.DeleteLedgerMetadata.GetKey()))
		case *servicepb.Request_SetMetadataFieldType:
			ft := t.SetMetadataFieldType
			parts = append(parts, fmt.Sprintf("setFT %s/tgt%d/%s=ty%d", ft.GetLedger(), ft.GetTargetType(), ft.GetKey(), ft.GetType()))
		case *servicepb.Request_RemoveMetadataFieldType:
			ft := t.RemoveMetadataFieldType
			parts = append(parts, fmt.Sprintf("rmFT %s/tgt%d/%s", ft.GetLedger(), ft.GetTargetType(), ft.GetKey()))
		}
	}

	return "[" + strings.Join(parts, " ") + "]"
}

// typeOps renders the add/remove-account-type ops a bulk carries ("+name" /
// "-name"), or "" if it carries none.
func typeOps(b oracle.Bulk) string {
	var ops []string
	for _, r := range b.Requests {
		switch t := r.GetType().(type) {
		case *servicepb.Request_AddAccountType:
			ops = append(ops, "+"+t.AddAccountType.GetAccountType().GetName())
		case *servicepb.Request_RemoveAccountType:
			ops = append(ops, "-"+t.RemoveAccountType.GetName())
		}
	}

	return strings.Join(ops, "")
}

// failureDiag renders a failed bulk's transaction postings, the model's declared
// account types for the ledgers it touches, and the type-ops sitting in the
// pending/inflight buffer (with their ticket vs the failure's maxTicket) — so an
// unexplained-failure finding shows which address the model treats as unmatched,
// whether a type for it exists in the committed model, and whether a folding
// candidate (an addType) is available in the buffer. Caller holds c.mu.
func (c *Checker) failureDiag(b oracle.Bulk, maxTicket uint64) (postings, modelTypes, buffered string) {
	var ps []string
	ledgers := map[string]bool{}
	for _, r := range b.Requests {
		l := oracle.LedgerOf(r)
		ledgers[l] = true
		if ct := r.GetApply().GetAction().GetCreateTransaction(); ct != nil {
			for _, p := range ct.GetPostings() {
				ps = append(ps, fmt.Sprintf("%s:%s->%s(%s)", l, p.GetSource(), p.GetDestination(), p.GetAsset()))
			}
		}
	}

	var types []string
	for l := range ledgers {
		var names []string
		for n, t := range c.modelState.Ledger(l).Types() {
			names = append(names, fmt.Sprintf("%s(%s)", n, t.Pattern))
		}
		sort.Strings(names)
		types = append(types, l+"=["+strings.Join(names, ",")+"]")
	}
	sort.Strings(types)

	within := func(t uint64) string {
		if t <= maxTicket {
			return "≤"
		}

		return ">"
	}

	var pend, infl []string
	for _, pe := range c.pending {
		if ops := typeOps(pe.obs.bulk); ops != "" {
			pend = append(pend, fmt.Sprintf("t%d%s%s", pe.obs.ticket, within(pe.obs.ticket), ops))
		}
	}
	for tkt, bulk := range c.inflight {
		if ops := typeOps(bulk); ops != "" {
			infl = append(infl, fmt.Sprintf("t%d%s%s", tkt, within(tkt), ops))
		}
	}
	sort.Strings(pend)
	sort.Strings(infl)

	buffered = fmt.Sprintf("max=%d pendingTypeOps=[%s] inflightTypeOps=[%s]", maxTicket, strings.Join(pend, " "), strings.Join(infl, " "))

	return "[" + strings.Join(ps, " ") + "]", "[" + strings.Join(types, " ") + "]", buffered
}

// metaTargetLabel renders a metadata target for debug output: the account address
// or "tx:<id>" for a transaction target.
func metaTargetLabel(target *commonpb.Target) string {
	switch t := target.GetTarget().(type) {
	case *commonpb.Target_Account:
		return t.Account.GetAddr()
	case *commonpb.Target_TransactionId:
		return fmt.Sprintf("tx:%d", t.TransactionId)
	default:
		return "?"
	}
}

// renderMetaMap renders a server metadata map as {k=typed,...}, sorted.
func renderMetaMap(m map[string]*commonpb.MetadataValue) string {
	parts := make([]string, 0, len(m))
	for k, v := range m {
		parts = append(parts, k+"="+oracle.MetaValueString(v))
	}
	sort.Strings(parts)

	return "{" + strings.Join(parts, ",") + "}"
}

// modelAccountMetaDump renders the committed model's metadata for addr as
// {k=value[ft],...} — for diagnosing read mismatches. Acquires c.mu.
func (c *Checker) modelAccountMetaDump(ledger, addr string) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	ls := c.modelState.Ledger(ledger)

	var parts []string
	for k, v := range ls.AccountMetadata(addr) {
		ft := "none"
		if t, ok := ls.AccountFieldTypes()[k]; ok {
			ft = fmt.Sprintf("%d", t)
		}
		parts = append(parts, fmt.Sprintf("%s=%s[ft=%s]", k, oracle.MetaValueString(v), ft))
	}
	sort.Strings(parts)

	return "{" + strings.Join(parts, ",") + "}"
}

// modelLedgerMetaDump renders the committed model's ledger metadata. Acquires c.mu.
func (c *Checker) modelLedgerMetaDump(ledger string) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	ls := c.modelState.Ledger(ledger)

	var parts []string
	for k, v := range ls.LedgerMeta() {
		ft := "none"
		if t, ok := ls.LedgerFieldTypes()[k]; ok {
			ft = fmt.Sprintf("%d", t)
		}
		parts = append(parts, fmt.Sprintf("%s=%s[ft=%s]", k, oracle.MetaValueString(v), ft))
	}
	sort.Strings(parts)

	return "{" + strings.Join(parts, ",") + "}"
}

// modelTxDump renders the committed model's transaction at id (reference,
// reverted, metadata), or "<absent>" if the log has no such id. Acquires c.mu.
func (c *Checker) modelTxDump(ledger string, id uint64) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	txs := c.modelState.Ledger(ledger).Txs()
	if id == 0 || id > uint64(len(txs)) {
		return "<absent>"
	}

	tx := txs[id-1]
	parts := make([]string, 0, len(tx.Metadata()))
	for k, v := range tx.Metadata() {
		parts = append(parts, k+"="+oracle.MetaValueString(v))
	}
	sort.Strings(parts)

	return fmt.Sprintf("{ref=%q reverted=%v revBy=%d reverts=%d meta={%s}}",
		tx.Reference(), tx.Reverted(), tx.RevertedBy(), tx.RevertsTransaction(), strings.Join(parts, ","))
}

// modelSchemaDump renders the committed model's declared field types per target
// (a=account, t=transaction, l=ledger). Acquires c.mu.
func (c *Checker) modelSchemaDump(ledger string) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	ls := c.modelState.Ledger(ledger)

	render := func(tag string, m map[string]commonpb.MetadataType) string {
		parts := make([]string, 0, len(m))
		for k, t := range m {
			parts = append(parts, fmt.Sprintf("%s/%s=%d", tag, k, t))
		}
		sort.Strings(parts)

		return strings.Join(parts, ",")
	}

	return "[" + render("a", ls.AccountFieldTypes()) +
		" " + render("t", ls.TransactionFieldTypes()) +
		" " + render("l", ls.LedgerFieldTypes()) + "]"
}

// Server log sequences — for verifying drain order vs commit order.
func logSeqs(logs []*commonpb.Log) string {
	ids := make([]string, len(logs))
	for i, l := range logs {
		ids[i] = fmt.Sprintf("%d", l.GetSequence())
	}
	return "[" + strings.Join(ids, ",") + "]"
}
