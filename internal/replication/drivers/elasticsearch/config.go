package elasticsearch

import (
	"github.com/formancehq/ledger/internal/replication/config"
	"github.com/pkg/errors"
)

const (
	DefaultIndex = "unified-stack-data"
)

type Authentication struct {
	Username   string `json:"username"`
	Password   string `json:"password"`
	AWSEnabled bool   `json:"awsEnabled"`
}

func (a Authentication) Validate() error {
	switch {
	case a.Username == "" && a.Password != "" ||
		a.Username != "" && a.Password == "":
		return errors.New("username and password must be defined together")
	case a.AWSEnabled && a.Username != "":
		return newErrIncorrectIAMConfiguration()
	}
	return nil
}

type Config struct {
	Endpoint       string          `json:"endpoint"`
	Authentication *Authentication `json:"authentication"`
	Index          string          `json:"index"`
}

func (e *Config) SetDefaults() {
	if e.Index == "" {
		e.Index = DefaultIndex
	}
}

func (e *Config) Validate() error {
	if e.Endpoint == "" {
		return errors.New("elasticsearch endpoint is required")
	}

	if e.Authentication != nil {
		if err := e.Authentication.Validate(); err != nil {
			return errors.Wrap(err, "authentication configuration is invalid")
		}
	}

	if e.Index == "" {
		return errors.New("missing index")
	}

	return nil
}

var _ config.Validator = (*Config)(nil)
var _ config.Defaulter = (*Config)(nil)
