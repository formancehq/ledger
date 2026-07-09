package controller

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

// This file holds the pure (Kubernetes-free) logic for reconciling a ledger's
// index set from a Ledger CRD spec: the mapping between CRD/CLI/proto index
// identities, parsing of `ledgerctl indexes list --json` and
// `ledgers get-schema --json`, and the add/drop diff. Keeping it side-effect
// free makes the identity mapping and diff table-testable without a cluster.

// managedIndex is the fully-resolved identity of one index the operator
// manages, carrying both a stable canonical key (for set membership and for
// status.appliedIndexes) and the ledgerctl invocation fields.
type managedIndex struct {
	// canonical is the stable identity used for diffing and persisted in
	// status.appliedIndexes: a builtin --type value (e.g. "reference",
	// "account-asset") or "metadata:<target>:<key>".
	canonical string
	// typeFlag is the ledgerctl `--type` value.
	typeFlag string
	// target/key are set only for metadata indexes (typeFlag == "metadata").
	target string
	key    string
	// mdType is the CRD metadata field type (e.g. "string"); metadata only.
	mdType string
}

const metadataTypeFlag = "metadata"

// crdTxIndexToFlag maps CRD transaction enum values (camelCase, k8s-idiomatic)
// to the ledgerctl `indexes create --type` values (kebab-case). It is the
// single authority for the transaction-builtin identity; keep it in sync with
// cmd/ledgerctl/indexes/id_helpers.go's indexTypeOptions.
var crdTxIndexToFlag = map[string]string{
	"reference":          "reference",
	"timestamp":          "timestamp",
	"address":            "address",
	"sourceAddress":      "source-address",
	"destinationAddress": "destination-address",
	"insertedAt":         "inserted-at",
	"revertedAt":         "reverted-at",
}

// accountAssetFlag is the ledgerctl `--type` value for the account asset index.
const accountAssetFlag = "account-asset"

// protoTxEnumToFlag maps the protojson enum names emitted by
// `indexes list --json` to ledgerctl `--type` values. TX_BUILTIN_INDEX_ID is
// intentionally absent: it is not creatable via the CLI, so it is
// CRD-unrepresentable and must never be a create/drop candidate.
var protoTxEnumToFlag = map[string]string{
	"TX_BUILTIN_INDEX_REFERENCE":           "reference",
	"TX_BUILTIN_INDEX_TIMESTAMP":           "timestamp",
	"TX_BUILTIN_INDEX_ADDRESS":             "address",
	"TX_BUILTIN_INDEX_SOURCE_ADDRESS":      "source-address",
	"TX_BUILTIN_INDEX_DESTINATION_ADDRESS": "destination-address",
	"TX_BUILTIN_INDEX_INSERTED_AT":         "inserted-at",
	"TX_BUILTIN_INDEX_REVERTED_AT":         "reverted-at",
}

// protoTargetToFlag maps the protojson TargetType enum names to CRD/CLI target
// values. TARGET_TYPE_LEDGER is absent: ledger-target metadata indexes are
// CRD-unrepresentable and are left untouched.
var protoTargetToFlag = map[string]string{
	"TARGET_TYPE_ACCOUNT":     "account",
	"TARGET_TYPE_TRANSACTION": "transaction",
}

// crdMetadataTypeToEnum maps CRD metadata type values to the protojson
// MetadataType enum names returned by `ledgers get-schema --json`, so a
// declared field type can be compared against the desired type.
var crdMetadataTypeToEnum = map[string]string{
	"string":   "METADATA_TYPE_STRING",
	"int64":    "METADATA_TYPE_INT64",
	"bool":     "METADATA_TYPE_BOOL",
	"uint64":   "METADATA_TYPE_UINT64",
	"int8":     "METADATA_TYPE_INT8",
	"int16":    "METADATA_TYPE_INT16",
	"int32":    "METADATA_TYPE_INT32",
	"uint8":    "METADATA_TYPE_UINT8",
	"uint16":   "METADATA_TYPE_UINT16",
	"uint32":   "METADATA_TYPE_UINT32",
	"datetime": "METADATA_TYPE_DATETIME",
}

// canonicalIndex builds the stable canonical key for an index identity.
func canonicalIndex(typeFlag, target, key string) string {
	if typeFlag == metadataTypeFlag {
		return metadataTypeFlag + ":" + target + ":" + key
	}

	return typeFlag
}

// parseCanonicalIndex reconstructs a ledgerctl invocation identity from a
// canonical key persisted in status.appliedIndexes. It is the inverse of
// canonicalIndex and is used to build drop commands.
func parseCanonicalIndex(canonical string) (typeFlag, target, key string) {
	if rest, ok := strings.CutPrefix(canonical, metadataTypeFlag+":"); ok {
		// metadata:<target>:<key> — key may itself contain ':' so split once.
		parts := strings.SplitN(rest, ":", 2)
		if len(parts) == 2 {
			return metadataTypeFlag, parts[0], parts[1]
		}

		return metadataTypeFlag, rest, ""
	}

	return canonical, "", ""
}

// desiredIndexes flattens a LedgerIndexesSpec into the operator-managed set.
// A nil spec yields nil (the caller treats that as "unmanaged"). Duplicate or
// unknown enum values are skipped defensively (CRD validation rejects them at
// admission, but the helper must not panic or emit bogus commands if they slip
// through, e.g. from an older API server).
func desiredIndexes(spec *ledgerv1alpha1.LedgerIndexesSpec) []managedIndex {
	if spec == nil {
		return nil
	}

	seen := make(map[string]struct{})
	var out []managedIndex

	add := func(mi managedIndex) {
		if _, dup := seen[mi.canonical]; dup {
			return
		}

		seen[mi.canonical] = struct{}{}
		out = append(out, mi)
	}

	for _, tx := range spec.Transaction {
		flag, ok := crdTxIndexToFlag[tx]
		if !ok {
			continue
		}

		add(managedIndex{canonical: canonicalIndex(flag, "", ""), typeFlag: flag})
	}

	for _, acct := range spec.Account {
		if acct != "asset" {
			continue
		}

		add(managedIndex{canonical: canonicalIndex(accountAssetFlag, "", ""), typeFlag: accountAssetFlag})
	}

	for _, md := range spec.Metadata {
		if _, ok := crdMetadataTypeToEnum[md.Type]; !ok {
			continue
		}

		if md.Target != "account" && md.Target != "transaction" {
			continue
		}

		if md.Key == "" {
			continue
		}

		add(managedIndex{
			canonical: canonicalIndex(metadataTypeFlag, md.Target, md.Key),
			typeFlag:  metadataTypeFlag,
			target:    md.Target,
			key:       md.Key,
			mdType:    md.Type,
		})
	}

	return out
}

// listedIndex mirrors the protojson shape of one commonpb.Index element from
// `ledgerctl indexes list --json`. Only the fields needed for identity are
// decoded; protojson emits set oneof members only, and enums as string names.
type listedIndex struct {
	ID struct {
		TxBuiltin      string `json:"txBuiltin"`
		AccountBuiltin string `json:"accountBuiltin"`
		LogBuiltin     string `json:"logBuiltin"`
		Metadata       *struct {
			Target string `json:"target"`
			Key    string `json:"key"`
		} `json:"metadata"`
	} `json:"id"`
	Ledger string `json:"ledger"`
}

// parseActualIndexes parses `indexes list --json` stdout into the set of
// canonical keys the operator can represent. CRD-unrepresentable entries
// (log builtins, the ID tx-builtin, ledger-target metadata, unknown kinds) are
// skipped so they are never treated as drop candidates.
func parseActualIndexes(stdout string) (map[string]bool, error) {
	trimmed := strings.TrimSpace(stdout)
	if trimmed == "" {
		return map[string]bool{}, nil
	}

	var entries []listedIndex
	if err := json.Unmarshal([]byte(trimmed), &entries); err != nil {
		return nil, fmt.Errorf("parsing indexes list output: %w", err)
	}

	actual := make(map[string]bool, len(entries))
	for _, e := range entries {
		switch {
		case e.ID.TxBuiltin != "":
			if flag, ok := protoTxEnumToFlag[e.ID.TxBuiltin]; ok {
				actual[canonicalIndex(flag, "", "")] = true
			}
		case e.ID.AccountBuiltin == "ACCT_BUILTIN_INDEX_ASSET":
			actual[canonicalIndex(accountAssetFlag, "", "")] = true
		case e.ID.Metadata != nil:
			if target, ok := protoTargetToFlag[e.ID.Metadata.Target]; ok {
				actual[canonicalIndex(metadataTypeFlag, target, e.ID.Metadata.Key)] = true
			}
		}
	}

	return actual, nil
}

// schemaStatus mirrors the protojson shape of GetMetadataSchemaStatusResponse
// from `ledgers get-schema --json`. Only the declared type per field is decoded.
type schemaStatus struct {
	AccountFields     map[string]schemaField `json:"accountFields"`
	TransactionFields map[string]schemaField `json:"transactionFields"`
}

type schemaField struct {
	DeclaredType string `json:"declaredType"`
}

// parseSchema parses `ledgers get-schema --json` stdout.
func parseSchema(stdout string) (*schemaStatus, error) {
	trimmed := strings.TrimSpace(stdout)
	if trimmed == "" {
		return &schemaStatus{}, nil
	}

	var s schemaStatus
	if err := json.Unmarshal([]byte(trimmed), &s); err != nil {
		return nil, fmt.Errorf("parsing get-schema output: %w", err)
	}

	return &s, nil
}

// declaredType returns the protojson MetadataType enum name declared for a
// (target, key) field, and whether the field exists in the schema.
func (s *schemaStatus) declaredType(target, key string) (string, bool) {
	var fields map[string]schemaField
	switch target {
	case "account":
		fields = s.AccountFields
	case "transaction":
		fields = s.TransactionFields
	default:
		return "", false
	}

	f, ok := fields[key]
	if !ok {
		return "", false
	}

	return f.DeclaredType, true
}

// metadataFieldNeedsUpdate reports whether `set-metadata-type` must be issued
// for a desired metadata index: the field is absent, or its declared type
// differs from the desired type.
func metadataFieldNeedsUpdate(schema *schemaStatus, mi managedIndex) bool {
	want := crdMetadataTypeToEnum[mi.mdType]
	have, ok := schema.declaredType(mi.target, mi.key)

	return !ok || have != want
}

// indexDiff is the reconcile plan: indexes to create and indexes to drop.
type indexDiff struct {
	toCreate []managedIndex
	toDrop   []managedIndex
}

// diffIndexes computes the create/drop plan. Creates are desired indexes not
// present in actual. Drops are ownership-scoped: only indexes the operator
// previously created (applied) that are no longer desired AND still present in
// actual — so externally-created and CRD-unrepresentable indexes are never
// dropped, and already-gone indexes are not re-dropped.
func diffIndexes(desired []managedIndex, actual map[string]bool, applied []string) indexDiff {
	desiredByCanonical := make(map[string]struct{}, len(desired))
	var diff indexDiff

	for _, mi := range desired {
		desiredByCanonical[mi.canonical] = struct{}{}
		if !actual[mi.canonical] {
			diff.toCreate = append(diff.toCreate, mi)
		}
	}

	for _, canonical := range applied {
		if _, stillDesired := desiredByCanonical[canonical]; stillDesired {
			continue
		}

		if !actual[canonical] {
			continue
		}

		typeFlag, target, key := parseCanonicalIndex(canonical)
		diff.toDrop = append(diff.toDrop, managedIndex{
			canonical: canonical,
			typeFlag:  typeFlag,
			target:    target,
			key:       key,
		})
	}

	return diff
}

// canonicalList returns the sorted canonical keys of the managed set.
func canonicalList(indexes []managedIndex) []string {
	if len(indexes) == 0 {
		return nil
	}

	out := make([]string, 0, len(indexes))
	for _, mi := range indexes {
		out = append(out, mi.canonical)
	}

	sort.Strings(out)

	return out
}

// nextAppliedIndexes computes the operator-owned set to persist in
// status.appliedIndexes after applying diff: the previously-owned set plus the
// indexes just created, minus the indexes just dropped. Indexes that were
// desired but already existed (never in diff.toCreate) are deliberately NOT
// adopted — the operator only ever owns (and can therefore later drop) indexes
// it created itself, so externally-created indexes are never dropped even when
// they appear in spec.indexes and are later removed.
func nextAppliedIndexes(oldApplied []string, diff indexDiff) []string {
	owned := make(map[string]struct{}, len(oldApplied)+len(diff.toCreate))
	for _, canonical := range oldApplied {
		owned[canonical] = struct{}{}
	}

	for _, mi := range diff.toCreate {
		owned[mi.canonical] = struct{}{}
	}

	for _, mi := range diff.toDrop {
		delete(owned, mi.canonical)
	}

	if len(owned) == 0 {
		return nil
	}

	out := make([]string, 0, len(owned))
	for canonical := range owned {
		out = append(out, canonical)
	}

	sort.Strings(out)

	return out
}

// createArgs builds the `ledgerctl indexes create` argument slice for mi.
func (mi managedIndex) createArgs(ledgerName string) []string {
	args := []string{"indexes", "create", "--ledger", ledgerName, "--type", mi.typeFlag}
	if mi.typeFlag == metadataTypeFlag {
		args = append(args, "--target", mi.target, "--key", mi.key)
	}

	return args
}

// dropArgs builds the `ledgerctl indexes drop` argument slice for mi.
func (mi managedIndex) dropArgs(ledgerName string) []string {
	args := []string{"indexes", "drop", "--ledger", ledgerName, "--type", mi.typeFlag}
	if mi.typeFlag == metadataTypeFlag {
		args = append(args, "--target", mi.target, "--key", mi.key)
	}

	return args
}

// setMetadataTypeArgs builds the `ledgerctl ledgers set-metadata-type` slice
// declaring the metadata field for mi (metadata indexes only).
func (mi managedIndex) setMetadataTypeArgs(ledgerName string) []string {
	return []string{
		"ledgers", "set-metadata-type",
		"--ledger", ledgerName,
		"--target", mi.target,
		"--key", mi.key,
		"--type", mi.mdType,
	}
}
