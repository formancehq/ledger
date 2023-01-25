package wise

import (
	"encoding/json"

	"github.com/formancehq/payments/internal/app/connectors/configtemplate"
)

type Config struct {
	APIKey string `json:"apiKey" yaml:"apiKey" bson:"apiKey"`
}

func (c Config) Validate() error {
	if c.APIKey == "" {
		return ErrMissingAPIKey
	}

	return nil
}

func (c Config) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

func (c Config) BuildTemplate() (string, configtemplate.Config) {
	cfg := configtemplate.NewConfig()

	cfg.AddParameter("apiKey", configtemplate.TypeString, true)

	return Name.String(), cfg
}
