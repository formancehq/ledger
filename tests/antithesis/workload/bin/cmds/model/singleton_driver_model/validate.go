package main

import (
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// The model-conformance checks: every observed server outcome — a committed
// bulk, a failed bulk, or a read — is checked against the model (model.go) over
// the candidate states candidateBases enumerates (search.go). Each check asserts
// at its own callsite with a literal message; Antithesis catalogues assertions by
// callsite and literal, so these must not be factored behind a shared assert.

// validateBulkSuccess records a committed bulk and cross-checks it against the
// forward model. Caller holds c.mu.
func (c *Checker) validateBulkSuccess(bulk oracle.Bulk, resp *servicepb.ApplyResponse) {
	dbg("BULK OK: ledgers=%s reqKinds=%s logSeqs=%s typeOps=%s meta=%s", bulkLedgers(bulk), requestKinds(bulk), logSeqs(resp.GetLogs()), typeOps(bulk), bulkMeta(bulk))

	c.crossCheckCommit(bulk, resp)
	c.captureReceipts(bulk, resp)
	coverReceiptReverts(bulk)
}

// coverReceiptReverts fires a coverage signal when the server commits a
// receipt-carried revert, proving admission's receipt path (verify +
// claims->postings) is actually exercised — not merely that the driver emitted
// one. If this stops firing, receipts are no longer being captured or sent.
func coverReceiptReverts(bulk oracle.Bulk) {
	for _, req := range bulk.Requests {
		if rt := req.GetApply().GetAction().GetRevertTransaction(); rt != nil && rt.GetReceipt() != "" {
			assert.Reachable("singleton_driver_model: receipt-carried revert committed", internal.Details{})

			return
		}
	}
}

// requestExpandsVolumes reports whether a create/revert asked the server to
// return post-commit volumes. When false the response log omits them, so
// crossCheckCommit skips the volume comparison for that order.
func requestExpandsVolumes(req *servicepb.Request) bool {
	action := req.GetApply().GetAction()
	if ct := action.GetCreateTransaction(); ct != nil {
		return ct.GetExpandVolumes()
	}

	if rt := action.GetRevertTransaction(); rt != nil {
		return rt.GetExpandVolumes()
	}

	return false
}

// captureReceipts records the signed receipt the server returned for each newly
// created, referenced transaction, keyed by reference, so generateRevert can
// exercise the receipt-carried revert path. The response log at index i pairs
// with bulk.Requests[i]; receipts sign CreatedTransaction logs only. Caller
// holds c.mu.
func (c *Checker) captureReceipts(bulk oracle.Bulk, resp *servicepb.ApplyResponse) {
	logs := resp.GetLogs()
	for i, req := range bulk.Requests {
		if i >= len(logs) {
			break
		}

		ct := req.GetApply().GetAction().GetCreateTransaction()
		if ct == nil || ct.GetReference() == "" {
			continue
		}

		if receipt := logs[i].GetReceipt(); receipt != "" {
			c.receiptByRef[ct.GetReference()] = receipt
		}
	}
}

// crossCheckCommit validates a committed bulk against the forward model
// (model.go). Bulks drain in log-sequence order, so c.modelState is this
// bulk's exact predecessor and the prediction is deterministic: the server
// committed the bulk, so the model must predict commit AND the identical
// post-commit volumes. A disagreement is a finding — the server accepted
// something the model rejects, or the volumes diverged. modelState advances
// only on agreement. Caller holds c.mu.
func (c *Checker) crossCheckCommit(bulk oracle.Bulk, resp *servicepb.ApplyResponse) {
	res := c.modelState.Apply(bulk)

	if !res.OK {
		dbg("MODEL FINDING: ledgers=%s server committed but model rejects (%s): kinds=%s meta=%s",
			bulkLedgers(bulk), res.Reason, requestKinds(bulk), bulkMeta(bulk))
		assert.Unreachable("singleton_driver_model: model rejects a server-committed bulk", internal.Details{
			"ledgers": bulkLedgers(bulk),
			"reason":  res.Reason,
			"kinds":   requestKinds(bulk),
			"meta":    bulkMeta(bulk),
		})

		return
	}

	logs := resp.GetLogs()
	for i, order := range res.Orders {
		if order.PCV == nil || i >= len(logs) {
			continue
		}

		// A tx created/reverted with ExpandVolumes:false carries no PCV on its
		// response log; skip the volume comparison (the commit is unaffected, and
		// later reads validate the volumes).
		if !requestExpandsVolumes(bulk.Requests[i]) {
			assert.Reachable("singleton_driver_model: non-expanded-volumes transaction committed", internal.Details{})

			continue
		}

		// PCV rides on CreatedTransaction for a create and RevertedTransaction
		// for a revert.
		data := logs[i].GetPayload().GetApply().GetLog().GetData()
		var serverPCV *commonpb.PostCommitVolumes
		switch {
		case data.GetCreatedTransaction() != nil:
			serverPCV = data.GetCreatedTransaction().GetPostCommitVolumes()
		case data.GetRevertedTransaction() != nil:
			serverPCV = data.GetRevertedTransaction().GetPostCommitVolumes()
		default:
			continue
		}

		for key, vp := range order.PCV {
			gotIn, gotOut, ok := postCommitVolume(serverPCV, key)
			if !ok || vp.Input.Cmp(&gotIn) != 0 || vp.Output.Cmp(&gotOut) != 0 {
				dbg("MODEL PCV MISMATCH: %s/%s model=(%s,%s) serverPresent=%v",
					key.Address, key.Asset, vp.Input.Dec(), vp.Output.Dec(), ok)
				assert.Unreachable("singleton_driver_model: model post-commit volume mismatch", internal.Details{
					"cell":     key.Address + "/" + key.Asset,
					"modelIn":  vp.Input.Dec(),
					"modelOut": vp.Output.Dec(),
				})

				return
			}
		}
	}

	// Check committed metadata writes against the server's response log: the
	// as-written values the server stored must match the model. Stored values are
	// verbatim — the declared type is applied only on read.
	for i, order := range res.Orders {
		if order.Meta == nil || i >= len(logs) {
			continue
		}

		gotSaved := responseMetaEffect(bulk.Requests[i], logs[i])

		if !metaMapEqual(order.Meta.Saved(), gotSaved) {
			dbg("MODEL META SAVED MISMATCH: model=%s server=%s", renderMetaMap(order.Meta.Saved()), renderMetaMap(gotSaved))
			assert.Unreachable("singleton_driver_model: response metadata value mismatch", internal.Details{
				"ledger":      oracle.LedgerOf(bulk.Requests[i]),
				"kinds":       requestKinds(bulk),
				"meta":        bulkMeta(bulk),
				"modelSaved":  renderMetaMap(order.Meta.Saved()),
				"serverSaved": renderMetaMap(gotSaved),
			})

			return
		}
	}

	// Check the remaining write ops against their response logs: the assigned
	// transaction id and echoed reference/postings, and the schema / account-type
	// mutations. These are all LedgerApplyOrders, so their logs sit under the
	// apply payload alongside CreateTransaction. Each mismatch asserts at its own
	// callsite (Antithesis catalogues by callsite + literal).
	for i := range res.Orders {
		if i >= len(logs) {
			continue
		}

		req := bulk.Requests[i]
		data := logs[i].GetPayload().GetApply().GetLog().GetData()

		order := res.Orders[i]

		if order.Revert != nil {
			rt := data.GetRevertedTransaction()
			revTx := rt.GetRevertTransaction()

			if revTx.GetId() != order.TxID {
				assert.Unreachable("singleton_driver_model: revert transaction id mismatch", internal.Details{
					"ledger":   oracle.LedgerOf(req),
					"modelId":  fmt.Sprintf("%d", order.TxID),
					"serverId": fmt.Sprintf("%d", revTx.GetId()),
				})

				return
			}

			if rt.GetRevertedTransactionId() != order.Revert.RevertedID() {
				assert.Unreachable("singleton_driver_model: reverted transaction id mismatch", internal.Details{
					"ledger":   oracle.LedgerOf(req),
					"modelId":  fmt.Sprintf("%d", order.Revert.RevertedID()),
					"serverId": fmt.Sprintf("%d", rt.GetRevertedTransactionId()),
				})

				return
			}

			if !postingsEqual(order.Revert.Postings(), revTx.GetPostings()) {
				assert.Unreachable("singleton_driver_model: revert postings mismatch", internal.Details{
					"ledger":   oracle.LedgerOf(req),
					"model":    renderPostings(order.Revert.Postings()),
					"returned": renderPostings(revTx.GetPostings()),
				})

				return
			}
			// The revert transaction's metadata is verified by reading its log
			// entry back (validateTransactionRead), not at commit.
		} else if order.TxID != 0 {
			tx := data.GetCreatedTransaction().GetTransaction()
			ct := req.GetApply().GetAction().GetCreateTransaction()

			if tx.GetId() != order.TxID {
				assert.Unreachable("singleton_driver_model: transaction id mismatch", internal.Details{
					"ledger":   oracle.LedgerOf(req),
					"modelId":  fmt.Sprintf("%d", order.TxID),
					"serverId": fmt.Sprintf("%d", tx.GetId()),
				})

				return
			}

			if tx.GetReference() != ct.GetReference() {
				assert.Unreachable("singleton_driver_model: transaction reference mismatch", internal.Details{
					"ledger":    oracle.LedgerOf(req),
					"requested": ct.GetReference(),
					"returned":  tx.GetReference(),
				})

				return
			}

			if !postingsEqual(ct.GetPostings(), tx.GetPostings()) {
				assert.Unreachable("singleton_driver_model: transaction postings mismatch", internal.Details{
					"ledger":    oracle.LedgerOf(req),
					"requested": renderPostings(ct.GetPostings()),
					"returned":  renderPostings(tx.GetPostings()),
				})

				return
			}

			// Account metadata set via the transaction payload is echoed verbatim
			// on the CreatedTransaction log (the workload uses no numscript, so the
			// server merges nothing else in).
			if !accountMetaMapEqual(ct.GetAccountMetadata(), data.GetCreatedTransaction().GetAccountMetadata()) {
				assert.Unreachable("singleton_driver_model: transaction account-metadata mismatch", internal.Details{
					"ledger":    oracle.LedgerOf(req),
					"requested": len(ct.GetAccountMetadata()),
					"returned":  len(data.GetCreatedTransaction().GetAccountMetadata()),
				})

				return
			}
		}

		// DeleteMetadata (account or transaction) echoes the deleted target and key
		// on its log; the deletion's effect is verified separately by reads.
		if dm := req.GetApply().GetAction().GetDeleteMetadata(); dm != nil {
			logDM := data.GetDeletedMetadata()
			if logDM.GetKey() != dm.GetKey() || metaTargetLabel(logDM.GetTarget()) != metaTargetLabel(dm.GetTarget()) {
				assert.Unreachable("singleton_driver_model: delete-metadata response mismatch", internal.Details{
					"ledger":    oracle.LedgerOf(req),
					"requested": metaTargetLabel(dm.GetTarget()) + "/" + dm.GetKey(),
					"returned":  metaTargetLabel(logDM.GetTarget()) + "/" + logDM.GetKey(),
				})

				return
			}
		}

		switch r := req.GetType().(type) {
		case *servicepb.Request_SetMetadataFieldType:
			lg, rq := data.GetSetMetadataFieldType(), r.SetMetadataFieldType
			if lg.GetTargetType() != rq.GetTargetType() || lg.GetKey() != rq.GetKey() || lg.GetType() != rq.GetType() {
				assert.Unreachable("singleton_driver_model: set-field-type response mismatch", internal.Details{
					"ledger":    oracle.LedgerOf(req),
					"requested": fmt.Sprintf("tgt%d/%s=%v", rq.GetTargetType(), rq.GetKey(), rq.GetType()),
					"returned":  fmt.Sprintf("tgt%d/%s=%v", lg.GetTargetType(), lg.GetKey(), lg.GetType()),
				})

				return
			}

		case *servicepb.Request_RemoveMetadataFieldType:
			lg, rq := data.GetRemovedMetadataFieldType(), r.RemoveMetadataFieldType
			if lg.GetTargetType() != rq.GetTargetType() || lg.GetKey() != rq.GetKey() {
				assert.Unreachable("singleton_driver_model: remove-field-type response mismatch", internal.Details{
					"ledger":    oracle.LedgerOf(req),
					"requested": fmt.Sprintf("tgt%d/%s", rq.GetTargetType(), rq.GetKey()),
					"returned":  fmt.Sprintf("tgt%d/%s", lg.GetTargetType(), lg.GetKey()),
				})

				return
			}

			// The workload never creates indexes, so removing a field type must
			// never report dropping one.
			if lg.GetDroppedIndex() != nil {
				assert.Unreachable("singleton_driver_model: remove-field-type unexpectedly dropped an index", internal.Details{
					"ledger": oracle.LedgerOf(req),
					"key":    rq.GetKey(),
				})

				return
			}

		case *servicepb.Request_AddAccountType:
			lg, rq := data.GetAddedAccountType().GetAccountType(), r.AddAccountType.GetAccountType()
			if lg.GetName() != rq.GetName() || lg.GetPattern() != rq.GetPattern() || lg.GetPersistence() != rq.GetPersistence() {
				assert.Unreachable("singleton_driver_model: add-account-type response mismatch", internal.Details{
					"ledger":    oracle.LedgerOf(req),
					"requested": fmt.Sprintf("%s=%s/p%d", rq.GetName(), rq.GetPattern(), rq.GetPersistence()),
					"returned":  fmt.Sprintf("%s=%s/p%d", lg.GetName(), lg.GetPattern(), lg.GetPersistence()),
				})

				return
			}

		case *servicepb.Request_RemoveAccountType:
			lg := data.GetRemovedAccountType()
			if lg.GetName() != r.RemoveAccountType.GetName() {
				assert.Unreachable("singleton_driver_model: remove-account-type response mismatch", internal.Details{
					"ledger":    oracle.LedgerOf(req),
					"requested": r.RemoveAccountType.GetName(),
					"returned":  lg.GetName(),
				})

				return
			}
		}
	}

	c.modelState = res.State
}

// validateFailure accepts the observed failure of failedBulk iff some
// candidate base reproduces the observed error reason when failedBulk is applied
// to it — i.e. there is a serialization of the in-flight bulks under which the
// server would reject failedBulk exactly this way. Because failedBulk is applied
// fresh on each base, its own requests can never "explain" its own rejection.
// Caller holds c.mu.
func (c *Checker) validateFailure(maxTicket uint64, failedBulk oracle.Bulk, reqErr error) {
	var reason string

	matched := false
	c.candidateBases(maxTicket, func(base oracle.GlobalState) bool {
		res := base.Apply(failedBulk)
		if !res.OK && internal.HasErrorReason(reqErr, res.Reason) {
			reason = res.Reason
			matched = true

			return true
		}

		return false
	})

	if matched {
		// Coverage: each deliberately-triggered rejection branch must actually be
		// exercised — if one stops firing, the generator has stopped emitting that
		// shape and the branch is no longer tested.
		switch reason {
		case domain.ErrReasonInsufficientFunds:
			assert.Reachable("singleton_driver_model: insufficient-funds rejection exercised", internal.Details{})
		case domain.ErrReasonTransactionReferenceConflict:
			assert.Reachable("singleton_driver_model: reference-conflict rejection exercised", internal.Details{})
		case domain.ErrReasonVolumeOverflow:
			assert.Reachable("singleton_driver_model: volume-overflow rejection exercised", internal.Details{})
		case domain.ErrReasonValidation:
			assert.Reachable("singleton_driver_model: validation rejection exercised", internal.Details{})
		}

		dbg("MODEL FAIL OK: ledgers=%s kinds=%s explained by %s", bulkLedgers(failedBulk), requestKinds(failedBulk), reason)

		return
	}

	// What the model makes of the same bulk on the drained (committed-prefix)
	// state — surfaced so the finding says why the model disagreed (a different
	// reject reason, or OK meaning the model thought it would commit).
	mres := c.modelState.Apply(failedBulk)
	modelReason := mres.Reason
	if mres.OK {
		modelReason = "OK"
	}

	postings, modelTypes, buffered := c.failureDiag(failedBulk, maxTicket)

	dbg("MODEL FAIL FINDING: ledgers=%s kinds=%s postings=%s modelReason=%s modelTypes=%s %s err=%v", bulkLedgers(failedBulk), requestKinds(failedBulk), postings, modelReason, modelTypes, buffered, reqErr)
	assert.Unreachable("singleton_driver_model: bulk failure not explained by any serialization", internal.Details{
		"ledgers":     bulkLedgers(failedBulk),
		"error":       reqErr.Error(),
		"kinds":       requestKinds(failedBulk),
		"postings":    postings,
		"modelReason": modelReason,
		"modelTypes":  modelTypes,
		"buffered":    buffered,
	})
}

// validateEmptyCommit handles a successful Apply that returned no committed log
// (no sequenced entry). This is always a finding: the server assigns a log
// sequence to every committed order — transaction or chart op (ProcessOrders) —
// and the model never accepts a bulk that commits nothing (a transaction always
// moves world's volume; add-existing and remove-missing both fail). So a success
// with no log means the server committed, or reported committing, without
// returning the log reference the checker needs to linearize the bulk. Caller
// holds c.mu.
func (c *Checker) validateEmptyCommit(bulk oracle.Bulk) {
	dbg("BULK OK (empty): ledgers=%s kinds=%s", bulkLedgers(bulk), requestKinds(bulk))

	assert.Unreachable("singleton_driver_model: successful bulk returned no committed log", internal.Details{
		"ledgers": bulkLedgers(bulk),
		"kinds":   requestKinds(bulk),
	})
}

// matchesModel reports whether some candidate base satisfies matcher; label names
// the read kind for debug logging. The read is registered outstanding for its
// whole window (registerRead/finishRead), so modelState stays at or behind the
// prefix the read saw (see tryDrain); candidateBases then folds only the bulks
// dispatched no later than maxTicket — the ticket high-water when the read
// returned — to enumerate what the server could have returned. The caller asserts
// on a false result — each assert needs its own callsite with a literal message
// for Antithesis cataloguing, so this helper never asserts. Acquires c.mu.
func (c *Checker) matchesModel(maxTicket uint64, label string, matcher func(oracle.GlobalState) bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	matched := false
	c.candidateBases(maxTicket, func(base oracle.GlobalState) bool {
		matched = matcher(base)
		return matched
	})

	if matched {
		dbg("%s OK", label)
	} else {
		dbg("%s FINDING", label)
	}

	return matched
}

// validateAccountRead checks one GetAccount snapshot against the model: the read
// is legal iff some candidate base holds both the picked (gotIn, gotOut, found)
// volume cell and exactly the server's metadata for the address. Both must hold
// on the SAME base — the read is one atomic snapshot.
func (c *Checker) validateAccountRead(maxTicket uint64, ledger, addr, asset string, gotIn, gotOut uint256.Int, found bool, serverMeta map[string]*commonpb.MetadataValue) {
	key := oracle.VolumeKey{Address: addr, Asset: asset}

	if c.matchesModel(maxTicket, "READ", func(base oracle.GlobalState) bool {
		ls := base.Ledger(ledger)
		return volumeCellMatches(ls, key, gotIn, gotOut, found) && metadataMatches(ls, addr, serverMeta)
	}) {
		return
	}

	assert.Unreachable("singleton_driver_model: account read outside model", internal.Details{
		"ledger":         ledger,
		"address":        addr,
		"asset":          asset,
		"serverIn":       gotIn.Dec(),
		"serverOut":      gotOut.Dec(),
		"serverHadAsset": found,
		"serverMeta":     renderMetaMap(serverMeta),
		"modelMeta":      c.modelAccountMetaDump(ledger, addr),
	})
}

// volumeCellMatches reports whether ls holds exactly the (gotIn, gotOut, found)
// reading for cell key: present with matching volumes, or absent when the server
// returned no row (the base's purge sweep already removed zero-balance
// EPHEMERAL/TRANSIENT cells). An empty asset — a metadata-only pick — is never a
// present cell, so it imposes no volume constraint.
func volumeCellMatches(ls oracle.LedgerState, key oracle.VolumeKey, gotIn, gotOut uint256.Int, found bool) bool {
	vp, present := ls.Volumes()[key]
	switch {
	case found && present && vp.Input.Cmp(&gotIn) == 0 && vp.Output.Cmp(&gotOut) == 0:
		return true
	case !found && !present:
		return true
	}

	return false
}

// metadataMatches reports whether ls holds exactly serverMeta for addr — same
// keys, same verbatim values. Reads surface the stored value as-written; the
// declared type is an index hint, not applied on read.
func metadataMatches(ls oracle.LedgerState, addr string, serverMeta map[string]*commonpb.MetadataValue) bool {
	modelMeta := ls.AccountMetadata(addr)
	if len(modelMeta) != len(serverMeta) {
		return false
	}

	for k, v := range modelMeta {
		sv, ok := serverMeta[k]
		if !ok || oracle.MetaValueString(sv) != oracle.MetaValueString(v) {
			return false
		}
	}

	return true
}

// validateLedgerRead checks one GetLedger snapshot against the model: legal iff
// some candidate base holds both the server's chart (account types) and exactly
// the server's ledger metadata. Both must hold on the SAME base — the read is one
// atomic snapshot.
func (c *Checker) validateLedgerRead(maxTicket uint64, ledger string, serverTypes map[string]*commonpb.AccountType, serverMeta map[string]*commonpb.MetadataValue) {
	if c.matchesModel(maxTicket, "LEDGER", func(base oracle.GlobalState) bool {
		ls := base.Ledger(ledger)
		return chartMatches(ls, serverTypes) && ledgerMetaMatches(ls, serverMeta)
	}) {
		return
	}

	assert.Unreachable("singleton_driver_model: ledger read outside model", internal.Details{
		"ledger":      ledger,
		"serverTypes": len(serverTypes),
		"serverMeta":  renderMetaMap(serverMeta),
		"modelMeta":   c.modelLedgerMetaDump(ledger),
	})
}

// chartMatches reports whether ls's chart equals the server's account types
// exactly — same names, patterns, and persistence.
func chartMatches(ls oracle.LedgerState, serverTypes map[string]*commonpb.AccountType) bool {
	if len(ls.Types()) != len(serverTypes) {
		return false
	}

	for name, t := range ls.Types() {
		st, ok := serverTypes[name]
		if !ok || st.GetPattern() != t.Pattern || st.GetPersistence() != t.Persistence {
			return false
		}
	}

	return true
}

// ledgerMetaMatches reports whether ls's ledger metadata equals serverMeta
// exactly — same keys, same verbatim values. Reads surface the stored value
// as-written; the declared type is an index hint, not applied on read.
func ledgerMetaMatches(ls oracle.LedgerState, serverMeta map[string]*commonpb.MetadataValue) bool {
	if len(ls.LedgerMeta()) != len(serverMeta) {
		return false
	}

	for k, v := range ls.LedgerMeta() {
		sv, ok := serverMeta[k]
		if !ok || oracle.MetaValueString(sv) != oracle.MetaValueString(v) {
			return false
		}
	}

	return true
}

// validateTransactionRead checks one GetTransaction observation — a returned
// transaction, or NotFound when found is false — against the model: legal iff
// some candidate base's log agrees at that id. Either the base holds a
// transaction there matching the server's (id, reference, reverted flag,
// postings, and whole metadata map), or, for NotFound, the base has not assigned
// that id yet. The id is probed up to the dispatched frontier, so it may land on
// a committed tx, an in-flight one, or an unassigned id; candidateBases
// enumerates the serializations. This is the only path that reads accumulated
// transaction metadata back (create/add/overwrite/delete/revert), so a divergent
// stored projection with correct per-write echoes — the ledger-metadata
// cross-routing bug class — is caught here for transactions.
func (c *Checker) validateTransactionRead(maxTicket uint64, ledger string, id uint64, serverTx *commonpb.Transaction, found bool) {
	if c.matchesModel(maxTicket, "TXREAD", func(base oracle.GlobalState) bool {
		txs := base.Ledger(ledger).Txs()
		if id == 0 || id > uint64(len(txs)) {
			return !found // no tx at this id in this base: consistent only with NotFound
		}
		if !found {
			return false // base has a tx at this id, but the server returned NotFound
		}

		rec := txs[id-1]
		// A user-supplied timestamp is echoed verbatim; when the model has none
		// (nil) the server stamped its own command date, which is unpredictable,
		// so the timestamp is not checked for that record.
		tsOK := rec.Timestamp() == nil || rec.Timestamp().GetData() == serverTx.GetTimestamp().GetData()

		return rec.Id() == serverTx.GetId() &&
			rec.Reference() == serverTx.GetReference() &&
			rec.Reverted() == serverTx.GetReverted() &&
			tsOK &&
			postingsEqual(rec.Postings(), serverTx.GetPostings()) &&
			metaMapEqual(rec.Metadata(), serverTx.GetMetadata())
	}) {
		return
	}

	assert.Unreachable("singleton_driver_model: transaction read outside model", internal.Details{
		"ledger":     ledger,
		"id":         id,
		"found":      found,
		"serverRef":  serverTx.GetReference(),
		"serverRev":  serverTx.GetReverted(),
		"serverMeta": renderMetaMap(serverTx.GetMetadata()),
		"modelTx":    c.modelTxDump(ledger, id),
	})
}

// validateSchemaRead checks one GetMetadataSchemaStatus snapshot against the
// model: legal iff some candidate base's declared field types — per target,
// key-for-key — exactly match the server's. Field types are declared only by
// SetMetadataFieldType and by initial_schema at ledger creation, both tracked by
// the model, so this is the read-back that verifies the declared-schema
// projection rather than just the per-op response echo.
func (c *Checker) validateSchemaRead(maxTicket uint64, ledger string, acct, txn, ldg map[string]*servicepb.MetadataFieldStatus) {
	if c.matchesModel(maxTicket, "SCHEMA", func(base oracle.GlobalState) bool {
		ls := base.Ledger(ledger)

		return fieldTypesMatch(ls.AccountFieldTypes(), acct) &&
			fieldTypesMatch(ls.TransactionFieldTypes(), txn) &&
			fieldTypesMatch(ls.LedgerFieldTypes(), ldg)
	}) {
		return
	}

	assert.Unreachable("singleton_driver_model: metadata schema read outside model", internal.Details{
		"ledger":       ledger,
		"serverAcct":   len(acct),
		"serverTxn":    len(txn),
		"serverLedger": len(ldg),
		"modelSchema":  c.modelSchemaDump(ledger),
	})
}

// fieldTypesMatch reports whether the model's declared field types equal the
// server's for one target — same keys, same declared type.
func fieldTypesMatch(model map[string]commonpb.MetadataType, server map[string]*servicepb.MetadataFieldStatus) bool {
	if len(model) != len(server) {
		return false
	}

	for k, t := range model {
		st, ok := server[k]
		if !ok || st.GetDeclaredType() != t {
			return false
		}
	}

	return true
}

// responseMetaEffect extracts the server's stored (as-written) values for a
// committed metadata write from its response log, dispatching on the request
// type. Returns nil for non-metadata requests and for deletes, whose log carries
// only the target and key (validated through subsequent reads).
func responseMetaEffect(req *servicepb.Request, log *commonpb.Log) (saved map[string]*commonpb.MetadataValue) {
	switch r := req.GetType().(type) {
	case *servicepb.Request_Apply:
		data := log.GetPayload().GetApply().GetLog().GetData()
		switch r.Apply.GetAction().GetData().(type) {
		case *servicepb.LedgerAction_CreateTransaction:
			return data.GetCreatedTransaction().GetTransaction().GetMetadata()
		case *servicepb.LedgerAction_AddMetadata:
			return data.GetSavedMetadata().GetMetadata()
		}
	case *servicepb.Request_SaveLedgerMetadata:
		return log.GetPayload().GetSavedLedgerMetadata().GetMetadata()
	}

	return nil
}

// postingsEqual reports whether two posting lists match field-for-field
// (source, destination, asset, amount).
func postingsEqual(a, b []*commonpb.Posting) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		var x, y uint256.Int
		a[i].GetAmount().IntoUint256(&x)
		b[i].GetAmount().IntoUint256(&y)

		if a[i].GetSource() != b[i].GetSource() ||
			a[i].GetDestination() != b[i].GetDestination() ||
			a[i].GetAsset() != b[i].GetAsset() ||
			!x.Eq(&y) {
			return false
		}
	}

	return true
}

// renderPostings formats a posting list for assertion details.
func renderPostings(ps []*commonpb.Posting) string {
	out := ""
	for _, p := range ps {
		var amt uint256.Int
		p.GetAmount().IntoUint256(&amt)
		if out != "" {
			out += ","
		}
		out += p.GetSource() + "->" + p.GetDestination() + ":" + amt.Dec() + p.GetAsset()
	}

	return "[" + out + "]"
}

// metaMapEqual reports whether two metadata maps hold the same keys and the same
// type-tagged values.
func metaMapEqual(a, b map[string]*commonpb.MetadataValue) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		bv, ok := b[k]
		if !ok || oracle.MetaValueString(v) != oracle.MetaValueString(bv) {
			return false
		}
	}

	return true
}

// accountMetaMapEqual reports whether two account-metadata maps (address -> map)
// hold the same addresses and, per address, the same metadata.
func accountMetaMapEqual(a, b map[string]*commonpb.MetadataMap) bool {
	if len(a) != len(b) {
		return false
	}

	for addr, am := range a {
		bm, ok := b[addr]
		if !ok || !metaMapEqual(am.GetValues(), bm.GetValues()) {
			return false
		}
	}

	return true
}

// postCommitVolume extracts (input, output) for one cell from a server response,
// parsing the decimal-string volumes into uint256 — the ledger's native volume
// type. ok is false when the cell is absent or the values don't parse.
func postCommitVolume(pcv *commonpb.PostCommitVolumes, key oracle.VolumeKey) (in, out uint256.Int, ok bool) {
	byAsset, found := pcv.GetVolumesByAccount()[key.Address]
	if !found {
		return in, out, false
	}

	vol, found := byAsset.GetVolumes()[key.Asset]
	if !found {
		return in, out, false
	}

	if err := in.SetFromDecimal(vol.GetInput()); err != nil {
		return in, out, false
	}

	if err := out.SetFromDecimal(vol.GetOutput()); err != nil {
		return in, out, false
	}

	return in, out, true
}
