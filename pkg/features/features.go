package features

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/formancehq/go-libs/v5/pkg/types/collections"
)

// indexedMetadataKeyRe matches valid key names for INDEXED_METADATA_KEYS.
// Keys are embedded as SQL literals, so only alphanumeric + underscore are allowed.
var indexedMetadataKeyRe = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

const (
	// FeatureMovesHistory is used to define if the ledger has to save funds movements history.
	// Value is either ON or OFF
	FeatureMovesHistory = "MOVES_HISTORY"
	// FeatureMovesHistoryPostCommitEffectiveVolumes is used to define if the pvce property of funds movements history
	// has to be updated with back dated transaction.
	// Value is either SYNC or DISABLED.
	// todo: depends on FeatureMovesHistory (dependency should be checked)
	FeatureMovesHistoryPostCommitEffectiveVolumes = "MOVES_HISTORY_POST_COMMIT_EFFECTIVE_VOLUMES"
	// FeatureHashLogs is used to defined it the logs has to be hashed.
	FeatureHashLogs = "HASH_LOGS"
	// FeatureAccountMetadataHistory is used to defined it the account metadata must be historized.
	FeatureAccountMetadataHistory = "ACCOUNT_METADATA_HISTORY"
	// FeatureTransactionMetadataHistory is used to defined it the transaction metadata must be historized.
	FeatureTransactionMetadataHistory = "TRANSACTION_METADATA_HISTORY"
	// FeatureIndexedMetadataKeys is a comma-separated list of metadata keys for which the query builder
	// emits a functional-index-compatible predicate (metadata ->> 'key' = 'value') instead of the default
	// JSONB containment form (metadata @> '{"key":"value"}'). A matching partial functional index must
	// exist on the ledger's transactions table for the rewrite to actually speed up the query.
	// Value: comma-separated key names, e.g. "source_wallet_id,destination_wallet_id". Empty = disabled.
	FeatureIndexedMetadataKeys = "INDEXED_METADATA_KEYS"
)

var (
	DefaultFeatures = FeatureSet{
		FeatureMovesHistory:                           "ON",
		FeatureMovesHistoryPostCommitEffectiveVolumes: "SYNC",
		FeatureHashLogs:                               "SYNC",
		FeatureAccountMetadataHistory:                 "SYNC",
		FeatureTransactionMetadataHistory:             "SYNC",
	}
	MinimalFeatureSet = FeatureSet{
		FeatureMovesHistory:                           "OFF",
		FeatureMovesHistoryPostCommitEffectiveVolumes: "DISABLED",
		FeatureHashLogs:                               "DISABLED",
		FeatureAccountMetadataHistory:                 "DISABLED",
		FeatureTransactionMetadataHistory:             "DISABLED",
	}
	// FeatureConfigurations lists the accepted values of every closed-set feature.
	// Benchmarks enumerate it to build all possible ledger configurations and take the
	// first value of each entry as the default, so every entry must hold at least one
	// value.  Features whose value is free-form belong in OpenEndedFeatures instead.
	//
	// notes: keep the default value as first option for benchmarks
	FeatureConfigurations = map[string][]string{
		FeatureMovesHistory:                           {"ON", "OFF"},
		FeatureMovesHistoryPostCommitEffectiveVolumes: {"SYNC", "DISABLED"},
		FeatureHashLogs:                               {"SYNC", "ASYNC", "DISABLED"},
		FeatureAccountMetadataHistory:                 {"SYNC", "DISABLED"},
		FeatureTransactionMetadataHistory:             {"SYNC", "DISABLED"},
	}
	// OpenEndedFeatures holds features accepting a free-form value that cannot be
	// enumerated. They are validated by feature-specific rules in ValidateFeatureWithValue
	// and deliberately kept out of FeatureConfigurations so benchmark enumeration keeps
	// working.
	OpenEndedFeatures = map[string]func(value string) error{
		FeatureIndexedMetadataKeys: validateIndexedMetadataKeys,
	}
)

func validateIndexedMetadataKeys(value string) error {
	if value == "" {
		return nil
	}
	for _, key := range strings.Split(value, ",") {
		if !indexedMetadataKeyRe.MatchString(key) {
			return fmt.Errorf("INDEXED_METADATA_KEYS: key %q is invalid (only [a-zA-Z0-9_] allowed)", key)
		}
	}

	return nil
}

func ValidateFeatureWithValue(feature, value string) error {
	if validate, ok := OpenEndedFeatures[feature]; ok {
		return validate(value)
	}

	possibleConfigurations, ok := FeatureConfigurations[feature]
	if !ok {
		return fmt.Errorf("feature %q not exists", feature)
	}
	if !slices.Contains(possibleConfigurations, value) {
		return fmt.Errorf("configuration %s it not possible for feature %s", value, feature)
	}

	return nil
}

type FeatureSet map[string]string

func (f FeatureSet) With(feature, value string) FeatureSet {
	ret := FeatureSet{}
	for k, v := range f {
		ret[k] = v
	}
	ret[feature] = value

	return ret
}

func (f FeatureSet) SortedKeys() []string {
	ret := collections.Keys(f)
	slices.Sort(ret)

	return ret
}

func (f FeatureSet) String() string {
	if len(f) == 0 {
		return ""
	}

	ret := ""
	for _, key := range f.SortedKeys() {
		ret = ret + "," + shortenFeature(key) + "=" + f[key]
	}

	return ret[1:]
}

func (f FeatureSet) Match(features FeatureSet) bool {
	for k, v := range features {
		if f[k] != v {
			return false
		}
	}
	return true
}

func shortenFeature(feature string) string {
	return strings.Join(collections.Map(strings.Split(feature, "_"), func(from string) string {
		return from[:1]
	}), "")
}
