package bankingcircle

import (
	"encoding/json"

	"github.com/formancehq/payments/internal/app/connectors/configtemplate"
)

type Config struct {
	Username              string `json:"username" yaml:"username" bson:"username"`
	Password              string `json:"password" yaml:"password" bson:"password"`
	Endpoint              string `json:"endpoint" yaml:"endpoint" bson:"endpoint"`
	AuthorizationEndpoint string `json:"authorizationEndpoint" yaml:"authorizationEndpoint" bson:"authorizationEndpoint"`
}

func (c Config) Validate() error {
	if c.Username == "" {
		return ErrMissingUsername
	}

	if c.Password == "" {
		return ErrMissingPassword
	}

	if c.Endpoint == "" {
		return ErrMissingEndpoint
	}

	if c.AuthorizationEndpoint == "" {
		return ErrMissingAuthorizationEndpoint
	}

	return nil
}

func (c Config) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

func (c Config) BuildTemplate() (string, configtemplate.Config) {
	cfg := configtemplate.NewConfig()

	cfg.AddParameter("username", configtemplate.TypeString, true)
	cfg.AddParameter("password", configtemplate.TypeString, true)
	cfg.AddParameter("endpoint", configtemplate.TypeString, true)
	cfg.AddParameter("authorizationEndpoint", configtemplate.TypeString, true)

	return Name.String(), cfg
}
