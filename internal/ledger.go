package ledger

import (
	"fmt"
	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/time"
	"regexp"
	"slices"
)

const (
	FeatureMovesHistory = "MOVES_HISTORY"
	// todo: depends on FeatureMovesHistory (dependency should be checked)
	FeatureMovesHistoryPostCommitEffectiveVolumes = "MOVES_HISTORY_POST_COMMIT_EFFECTIVE_VOLUMES"
	FeatureHashLogs                               = "HASH_LOGS"
	FeatureAccountMetadataHistory                 = "ACCOUNT_METADATA_HISTORY"
	FeatureTransactionMetadataHistory             = "TRANSACTION_METADATA_HISTORY"
	FeatureIndexAddressSegments                   = "INDEX_ADDRESS_SEGMENTS"
	FeatureIndexTransactionAccounts               = "INDEX_TRANSACTION_ACCOUNTS"

	StateInitializing = "initializing"
	StateInUse        = "in-use"

	DefaultBucket = "_default"
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
	}
	MinimalFeatureSet = FeatureSet{
		FeatureMovesHistory:                           "OFF",
		FeatureMovesHistoryPostCommitEffectiveVolumes: "DISABLED",
		FeatureHashLogs:                               "DISABLED",
		FeatureAccountMetadataHistory:                 "DISABLED",
		FeatureTransactionMetadataHistory:             "DISABLED",
		FeatureIndexAddressSegments:                   "OFF",
		FeatureIndexTransactionAccounts:               "OFF",
	}
	FeatureConfigurations = map[string][]string{
		FeatureMovesHistory:                           {"ON", "OFF"},
		FeatureMovesHistoryPostCommitEffectiveVolumes: {"SYNC", "DISABLED"},
		FeatureHashLogs:                               {"SYNC", "DISABLED"},
		FeatureAccountMetadataHistory:                 {"SYNC", "DISABLED"},
		FeatureTransactionMetadataHistory:             {"SYNC", "DISABLED"},
		FeatureIndexAddressSegments:                   {"ON", "OFF"},
		FeatureIndexTransactionAccounts:               {"ON", "OFF"},
	}

	ledgerNameFormat = regexp.MustCompile("^[0-9a-zA-Z_-]{1,63}$")
	bucketNameFormat = regexp.MustCompile("^[0-9a-zA-Z_-]{1,63}$")
)

type FeatureSet map[string]string

func (f FeatureSet) With(feature, value string) FeatureSet {
	ret := FeatureSet{}
	for k, v := range f {
		ret[k] = v
	}
	ret[feature] = value

	return ret
}

type Configuration struct {
	Bucket   string            `json:"bucket"`
	Metadata metadata.Metadata `json:"metadata"`
	Features map[string]string `bun:"features,type:jsonb" json:"features"`
}

func (c *Configuration) SetDefaults() {
	if c.Bucket == "" {
		c.Bucket = DefaultBucket
	}
	if c.Features == nil {
		c.Features = map[string]string{}
	}

	for key, value := range DefaultFeatures {
		if _, ok := c.Features[key]; !ok {
			c.Features[key] = value
		}
	}
}

func NewDefaultConfiguration() Configuration {
	return Configuration{
		Bucket:   DefaultBucket,
		Metadata: metadata.Metadata{},
		Features: DefaultFeatures,
	}
}

type Ledger struct {
	Configuration
	ID      int       `json:"id"`
	Name    string    `json:"name"`
	AddedAt time.Time `json:"addedAt"`
	State   string    `json:"-"`
}

func (l Ledger) HasFeature(feature, value string) bool {
	possibleConfigurations, ok := FeatureConfigurations[feature]
	if !ok {
		panic(fmt.Sprintf("feature %q not exists", feature))
	}
	if !slices.Contains(possibleConfigurations, value) {
		panic(fmt.Sprintf("configuration %s it not possible for feature %s", value, feature))
	}
	return l.Features[feature] == value
}

func New(name string, configuration Configuration) (*Ledger, error) {

	if !ledgerNameFormat.MatchString(name) {
		return nil, newErrInvalidLedgerName(name, fmt.Errorf("name must match format '%s'", ledgerNameFormat.String()))
	}
	if !bucketNameFormat.MatchString(configuration.Bucket) {
		return nil, newErrInvalidBucketName(configuration.Bucket, fmt.Errorf("name must match format '%s'", bucketNameFormat.String()))
	}

	return &Ledger{
		Configuration: configuration,
		Name:          name,
		AddedAt:       time.Now(),
		State:         StateInitializing,
	}, nil
}

func NewWithDefaults(name string) (*Ledger, error) {
	return New(name, NewDefaultConfiguration())
}

// todo: move in shared libs
func Must[V any](v *V, err error) V {
	if err != nil {
		panic(err)
	}
	return *v
}