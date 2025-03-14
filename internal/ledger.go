package ledger

import (
	"fmt"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/time"
	"github.com/formancehq/ledger/pkg/features"
	"github.com/uptrace/bun"
	"regexp"
	"slices"
)

const (
	StateInitializing = "initializing"
	StateInUse        = "in-use"
)

type Ledger struct {
	bun.BaseModel `bun:"_system.ledgers,alias:ledgers"`

	Configuration
	ID      int       `json:"id" bun:"id,type:int,scanonly"`
	Name    string    `json:"name" bun:"name,type:varchar(255),pk"`
	AddedAt time.Time `json:"addedAt" bun:"added_at,type:timestamp,nullzero"`
	State   string    `json:"-" bun:"state,type:varchar(255),nullzero"`
}

func (l Ledger) HasFeature(feature, value string) bool {
	if err := features.ValidateFeatureWithValue(feature, value); err != nil {
		panic(err)
	}

	return l.Features[feature] == value
}

func (l Ledger) WithMetadata(m metadata.Metadata) Ledger {
	l.Metadata = m
	return l
}

func New(name string, configuration Configuration) (*Ledger, error) {

	if err := configuration.Validate(); err != nil {
		return nil, err
	}

	if !ledgerNameFormat.MatchString(name) {
		return nil, newErrInvalidLedgerName(name, fmt.Errorf("name must match format '%s'", ledgerNameFormat.String()))
	}
	if slices.Contains(reservedLedgerName, name) {
		return nil, newErrInvalidLedgerName(name, fmt.Errorf("name '%s' is reserved", name))
	}
	if !bucketNameFormat.MatchString(configuration.Bucket) {
		return nil, newErrInvalidBucketName(configuration.Bucket, fmt.Errorf("name must match format '%s'", bucketNameFormat.String()))
	}

	return &Ledger{
		Configuration: configuration,
		Name:          name,
		State:         StateInitializing,
	}, nil
}

func NewWithDefaults(name string) (*Ledger, error) {
	return New(name, NewDefaultConfiguration())
}

func MustNewWithDefault(name string) Ledger {
	ledger, err := NewWithDefaults(name)
	if err != nil {
		panic(err)
	}
	return *ledger
}

const (
	DefaultBucket = "_default"
)

var (
	ledgerNameFormat = regexp.MustCompile("^[0-9a-zA-Z_-]{1,63}$")
	bucketNameFormat = regexp.MustCompile("^[0-9a-zA-Z_-]{1,63}$")

	reservedLedgerName = []string{
		// Used for debug in urls...
		"_",
		"_info",
		"_healthcheck",
	}
)

type Configuration struct {
	Bucket   string              `json:"bucket" bun:"bucket,type:varchar(255)"`
	Metadata metadata.Metadata   `json:"metadata" bun:"metadata,type:jsonb,nullzero"`
	Features features.FeatureSet `json:"features" bun:"features,type:jsonb"`
}

func (c *Configuration) SetDefaults() {
	if c.Bucket == "" {
		c.Bucket = DefaultBucket
	}
	if c.Features == nil {
		c.Features = map[string]string{}
	}

	for key, value := range features.DefaultFeatures {
		if _, ok := c.Features[key]; !ok {
			c.Features[key] = value
		}
	}
}

func (c *Configuration) Validate() error {
	for feature, value := range c.Features {
		if err := features.ValidateFeatureWithValue(feature, value); err != nil {
			return err
		}
	}

	return nil
}

func NewDefaultConfiguration() Configuration {
	return Configuration{
		Bucket:   DefaultBucket,
		Metadata: metadata.Metadata{},
		Features: features.DefaultFeatures,
	}
}
