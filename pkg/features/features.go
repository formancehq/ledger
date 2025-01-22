package features

import (
	"fmt"
	"github.com/formancehq/go-libs/v2/collectionutils"
	"slices"
	"strings"
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
	// FeatureIndexAddressSegments is used to defined it we want to index segments of accounts address.
	// Without this feature, the ledger will not allow filtering on partial account address.
	FeatureIndexAddressSegments = "INDEX_ADDRESS_SEGMENTS"
	// FeatureIndexTransactionAccounts is used to define if we want to index accounts used in a transaction.
	FeatureIndexTransactionAccounts = "INDEX_TRANSACTION_ACCOUNTS"
	// FeatureIndexTransactionAccounts is used to define if we want to compute post commit volumes of each transaction
	FeaturePostCommitVolumes = "POST_COMMIT_VOLUMES"
)

var (
	DefaultFeatures = FeatureSet{
		FeatureMovesHistory:                           "ON",
		FeatureMovesHistoryPostCommitEffectiveVolumes: "SYNC",
		FeatureHashLogs:                               "SYNC",
		FeatureAccountMetadataHistory:                 "SYNC",
		FeatureTransactionMetadataHistory:             "SYNC",
		FeatureIndexAddressSegments:                   "ON",
		FeatureIndexTransactionAccounts:               "ON",
		FeaturePostCommitVolumes: "ON",
	}
	MinimalFeatureSet = FeatureSet{
		FeatureMovesHistory:                           "OFF",
		FeatureMovesHistoryPostCommitEffectiveVolumes: "DISABLED",
		FeatureHashLogs:                               "DISABLED",
		FeatureAccountMetadataHistory:                 "DISABLED",
		FeatureTransactionMetadataHistory:             "DISABLED",
		FeatureIndexAddressSegments:                   "OFF",
		FeatureIndexTransactionAccounts:               "OFF",
		FeaturePostCommitVolumes: "OFF",
	}
	FeatureConfigurations = map[string][]string{
		FeatureMovesHistory:                           {"ON", "OFF"},
		FeatureMovesHistoryPostCommitEffectiveVolumes: {"SYNC", "DISABLED"},
		FeatureHashLogs:                               {"SYNC", "DISABLED"},
		FeatureAccountMetadataHistory:                 {"SYNC", "DISABLED"},
		FeatureTransactionMetadataHistory:             {"SYNC", "DISABLED"},
		FeatureIndexAddressSegments:                   {"ON", "OFF"},
		FeatureIndexTransactionAccounts:               {"ON", "OFF"},
		FeaturePostCommitVolumes: {"ON", "OFF"},
	}
)

func ValidateFeatureWithValue(feature, value string) error {
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
	ret := collectionutils.Keys(f)
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
	return strings.Join(collectionutils.Map(strings.Split(feature, "_"), func(from string) string {
		return from[:1]
	}), "")
}
