package ledger

import (
	"fmt"
	"regexp"
	"slices"

	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/time"
)

const (
	// todo: add feature to completely disable logs
	FeatureMovesHistory = "MOVES_HISTORY"
	// todo: depends on FeatureMovesHistory (dependency should be checked)
	FeatureMovesHistoryPostCommitEffectiveVolumes = "MOVES_HISTORY_POST_COMMIT_EFFECTIVE_VOLUMES"
	FeatureHashLogs                               = "HASH_LOGS"
	FeatureAccountMetadataHistory                 = "ACCOUNT_METADATA_HISTORY"
	FeatureTransactionMetadataHistory             = "TRANSACTION_METADATA_HISTORY"
	FeatureIndexAddressSegments                   = "INDEX_ADDRESS_SEGMENTS"
	FeatureIndexTransactionAccounts               = "INDEX_TRANSACTION_ACCOUNTS"

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

func validateFeatureWithValue(feature, value string) error {
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

func (c *Configuration) Validate() error {
	for feature, value := range c.Features {
		if err := validateFeatureWithValue(feature, value); err != nil {
			return err
		}
	}

	return nil
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
}

func (l Ledger) HasFeature(feature, value string) bool {
	if err := validateFeatureWithValue(feature, value); err != nil {
		panic(err)
	}

	return l.Features[feature] == value
}

func New(name string, configuration Configuration) (*Ledger, error) {

	if err := configuration.Validate(); err != nil {
		return nil, err
	}

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
	}, nil
}

func NewWithDefaults(name string) (*Ledger, error) {
	return New(name, NewDefaultConfiguration())
}

func MustNewWithDefault(name string) Ledger {
	ledger, err := New(name, NewDefaultConfiguration())
	if err != nil {
		panic(err)
	}
	return *ledger
}
