package features

import (
	"fmt"
	"slices"
	"strings"

	"github.com/formancehq/go-libs/v5/pkg/types/collections"
)

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
	// notes: keep the default value as first option for benchmarks
	FeatureConfigurations = map[string][]string{
		FeatureMovesHistory:                           {"ON", "OFF"},
		FeatureMovesHistoryPostCommitEffectiveVolumes: {"SYNC", "DISABLED"},
		FeatureHashLogs:                               {"SYNC", "ASYNC", "DISABLED"},
		FeatureAccountMetadataHistory:                 {"SYNC", "DISABLED"},
		FeatureTransactionMetadataHistory:             {"SYNC", "DISABLED"},
		FeatureIndexedMetadataKeys:                    nil, // nil = any comma-separated list of key names is valid
	}
)

func ValidateFeatureWithValue(feature, value string) error {
	possibleConfigurations, ok := FeatureConfigurations[feature]
	if !ok {
		return fmt.Errorf("feature %q not exists", feature)
	}
	// nil/empty set means any value is accepted (open-ended feature, e.g. INDEXED_METADATA_KEYS).
	if len(possibleConfigurations) > 0 && !slices.Contains(possibleConfigurations, value) {
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
