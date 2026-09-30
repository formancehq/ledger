package main

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// ledgerInfoMatches reports whether a served LedgerInfo is the record base holds
// for its ledger. Every field the model derives is compared here, so a listing
// row and a GetLedger answer are held to one contract.
//
// id and created_at are absent: the model assigns neither. They are pinned
// separately, against what the ledger's own creation log reported — see
// ledgerIdentity.
func ledgerInfoMatches(base oracle.GlobalState, info *commonpb.LedgerInfo) bool {
	name := info.GetName()

	lifecycle, exists := base.Lifecycle(name)
	if !exists || lifecycle.Deleted {
		return false
	}

	ls := base.Ledger(name)

	return chartMatches(ls, info.GetAccountTypes()) &&
		ledgerMetaMatches(ls, info.GetMetadata()) &&
		metadataSchemaMatches(ls, info.GetMetadataSchema()) &&
		info.GetMode() == lifecycle.Mode &&
		info.GetMirrorSource().EqualVT(lifecycle.MirrorSource) &&
		info.GetDefaultEnforcementMode() == ls.DefaultEnforcementMode()
}

// metadataSchemaMatches reports whether ls's declared field types equal the
// schema a LedgerInfo carries, per target and key-for-key. GetMetadataSchemaStatus
// is served by projecting this same field, so the two reads answer one record.
func metadataSchemaMatches(ls oracle.LedgerState, schema *commonpb.MetadataSchema) bool {
	return schemaFieldsMatch(ls.AccountFieldTypes(), schema.GetAccountFields()) &&
		schemaFieldsMatch(ls.TransactionFieldTypes(), schema.GetTransactionFields()) &&
		schemaFieldsMatch(ls.LedgerFieldTypes(), schema.GetLedgerFields())
}

// schemaFieldsMatch compares one target's declared types — same keys, same type.
// An absent schema message reads as the empty one.
func schemaFieldsMatch(model oracle.Map[string, commonpb.MetadataType], server map[string]*commonpb.MetadataFieldSchema) bool {
	if model.Len() != len(server) {
		return false
	}

	for key, declared := range model.All() {
		field, ok := server[key]
		if !ok || field.GetType() != declared {
			return false
		}
	}

	return true
}

// ledgerInfoStructureViolation names what is wrong with a listed LedgerInfo
// independently of any base, or "" when nothing is. The listing drops every
// soft-deleted ledger and enriches no mirror progress — that enrichment belongs
// to GetLedger alone — so both fields are absent on every row whatever the fleet
// holds.
func ledgerInfoStructureViolation(info *commonpb.LedgerInfo) string {
	switch {
	case info.GetName() == "":
		return "listed ledger has no name"
	case info.GetId() == 0:
		return "listed ledger has no id"
	case info.GetCreatedAt() == nil:
		return "listed ledger has no creation timestamp"
	case info.GetDeletedAt() != nil:
		return "listing served a soft-deleted ledger"
	case info.GetMirrorSyncProgress() != nil:
		return "listing carried mirror sync progress"
	}

	return ""
}

// ledgerIdentity is the (id, created_at) pair a ledger's creation log reported.
// The model derives neither, so reads are held to what creation announced.
// Recreating a deleted ledger assigns a fresh pair, which replaces this one.
type ledgerIdentity struct {
	id        uint32
	createdAt *commonpb.Timestamp
}

func (i ledgerIdentity) matches(info *commonpb.LedgerInfo) bool {
	return i.id == info.GetId() && i.createdAt.EqualVT(info.GetCreatedAt())
}

// ledgerIdentityViolation names the first row whose identity is not the one its
// creation reported, or "" when every row agrees. A ledger whose creation could
// still land — or has landed without being folded in — is skipped: either
// identity is legal while that create is unsettled. Acquires c.mu.
func (c *Checker) ledgerIdentityViolation(infos []*commonpb.LedgerInfo) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, info := range infos {
		name := info.GetName()

		identity, known := c.ledgerIdentities[name]
		if !known || c.ledgerCreateUnsettled(name) {
			continue
		}

		if !identity.matches(info) {
			return name
		}
	}

	return ""
}

// ledgerCreateUnsettled reports whether a create for name is dispatched but
// unanswered, retained as ambiguous, or buffered undrained. Caller holds c.mu.
func (c *Checker) ledgerCreateUnsettled(name string) bool {
	for _, bulk := range c.inflight {
		if bulkCreatesLedger(bulk, name) {
			return true
		}
	}

	for _, bulk := range c.ambiguousBulks {
		if bulkCreatesLedger(bulk, name) {
			return true
		}
	}

	for _, entry := range c.pending {
		if bulkCreatesLedger(entry.obs.bulk, name) {
			return true
		}
	}

	return false
}

func bulkCreatesLedger(bulk oracle.Bulk, name string) bool {
	for _, req := range bulk.Requests {
		if req.GetCreateLedger().GetName() == name {
			return true
		}
	}

	return false
}
