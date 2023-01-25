package modulr

import (
	"encoding/json"

	"github.com/formancehq/payments/internal/app/connectors/configtemplate"
)

type Config struct {
	APIKey    string `json:"apiKey" bson:"apiKey"`
	APISecret string `json:"apiSecret" bson:"apiSecret"`
	Endpoint  string `json:"endpoint" bson:"endpoint"`
}

func (c Config) Validate() error {
	if c.APIKey == "" {
		return ErrMissingAPIKey
	}

	if c.APISecret == "" {
		return ErrMissingAPISecret
	}

	return nil
}

func (c Config) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

func (c Config) BuildTemplate() (string, configtemplate.Config) {
	cfg := configtemplate.NewConfig()

	cfg.AddParameter("apiKey", configtemplate.TypeString, true)
	cfg.AddParameter("apiSecret", configtemplate.TypeString, true)
	cfg.AddParameter("endpoint", configtemplate.TypeString, false)

	return Name.String(), cfg
}
