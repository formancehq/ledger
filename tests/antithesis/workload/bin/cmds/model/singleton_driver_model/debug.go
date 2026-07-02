package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// MODEL_DEBUG=1 enables verbose per-transaction logging.
var modelDebug = os.Getenv("MODEL_DEBUG") != ""

func dbg(format string, args ...any) {
	if modelDebug {
		log.Printf("[model-debug] "+format, args...)
	}
}

// MODEL_DUMP_BATCHES=1 dumps each committed ApplyRequest (base64 vtproto) keyed
// by its min committed log sequence, so the exact full sequence can be replayed
// deterministically through the oracle (see tests/oracle/cmd/replay).
var dumpBatches = os.Getenv("MODEL_DUMP_BATCHES") != ""

func dumpBatch(seq uint64, req *servicepb.ApplyRequest) {
	if !dumpBatches {
		return
	}

	b, err := req.MarshalVT()
	if err != nil {
		return
	}

	log.Printf("[batch-dump] seq=%d b64=%s", seq, base64.StdEncoding.EncodeToString(b))
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

// modelTxMetaDump renders the committed model's metadata for the transaction at
// reference ref. Acquires c.mu.
func (c *Checker) modelTxMetaDump(ledger, ref string) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	ls := c.modelState.Ledger(ledger)

	var parts []string
	for tk, v := range ls.TxMeta() {
		if tk.Reference == ref {
			parts = append(parts, tk.Key+"="+oracle.MetaValueString(v))
		}
	}
	sort.Strings(parts)

	return "{" + strings.Join(parts, ",") + "}"
}

// Server log sequences — for verifying drain order vs commit order.
func logSeqs(logs []*commonpb.Log) string {
	ids := make([]string, len(logs))
	for i, l := range logs {
		ids[i] = fmt.Sprintf("%d", l.GetSequence())
	}
	return "[" + strings.Join(ids, ",") + "]"
}
