package dummypay

import (
	"encoding/json"
	"fmt"

	"github.com/formancehq/payments/internal/app/connectors/configtemplate"

	"github.com/formancehq/payments/internal/app/connectors"
)

// Config is the configuration for the dummy payment connector.
type Config struct {
	// Directory is the directory where the files are stored.
	Directory string `json:"directory" yaml:"directory" bson:"directory"`

	// FilePollingPeriod is the period between file polling.
	FilePollingPeriod connectors.Duration `json:"filePollingPeriod" yaml:"filePollingPeriod" bson:"filePollingPeriod"`

	// FileGenerationPeriod is the period between file generation
	FileGenerationPeriod connectors.Duration `json:"fileGenerationPeriod" yaml:"fileGenerationPeriod" bson:"fileGenerationPeriod"`
}

// String returns a string representation of the configuration.
func (c Config) String() string {
	return fmt.Sprintf("directory: %s, filePollingPeriod: %s, fileGenerationPeriod: %s",
		c.Directory, c.FilePollingPeriod.String(), c.FileGenerationPeriod.String())
}

func (c Config) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

// Validate validates the configuration.
func (c Config) Validate() error {
	// require directory path to be present
	if c.Directory == "" {
		return ErrMissingDirectory
	}

	// check if file polling period is set properly
	if c.FilePollingPeriod.Duration <= 0 {
		return fmt.Errorf("filePollingPeriod must be greater than 0: %w",
			ErrFilePollingPeriodInvalid)
	}

	// check if file generation period is set properly
	if c.FileGenerationPeriod.Duration <= 0 {
		return fmt.Errorf("fileGenerationPeriod must be greater than 0: %w",
			ErrFileGenerationPeriodInvalid)
	}

	return nil
}

func (c Config) BuildTemplate() (string, configtemplate.Config) {
	cfg := configtemplate.NewConfig()

	cfg.AddParameter("directory", configtemplate.TypeString, true)
	cfg.AddParameter("filePollingPeriod", configtemplate.TypeDurationNs, true)
	cfg.AddParameter("fileGenerationPeriod", configtemplate.TypeDurationNs, false)

	return Name.String(), cfg
}
