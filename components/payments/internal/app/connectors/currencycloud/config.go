package currencycloud

import (
	"encoding/json"
	"time"

	"github.com/formancehq/payments/internal/app/connectors/configtemplate"
)

type Config struct {
	LoginID       string   `json:"loginID" bson:"loginID"`
	APIKey        string   `json:"apiKey" bson:"apiKey"`
	Endpoint      string   `json:"endpoint" bson:"endpoint"`
	PollingPeriod Duration `json:"pollingPeriod" bson:"pollingPeriod"`
}

func (c Config) Validate() error {
	if c.APIKey == "" {
		return ErrMissingAPIKey
	}

	if c.LoginID == "" {
		return ErrMissingLoginID
	}

	if c.PollingPeriod == 0 {
		return ErrMissingPollingPeriod
	}

	return nil
}

func (c Config) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

type Duration time.Duration

func (d *Duration) String() string {
	return time.Duration(*d).String()
}

func (d *Duration) Duration() time.Duration {
	return time.Duration(*d)
}

func (d *Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(*d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var durationValue interface{}

	if err := json.Unmarshal(b, &durationValue); err != nil {
		return err
	}

	switch value := durationValue.(type) {
	case float64:
		*d = Duration(time.Duration(value))

		return nil
	case string:
		tmp, err := time.ParseDuration(value)
		if err != nil {
			return err
		}

		*d = Duration(tmp)

		return nil
	default:
		return ErrDurationInvalid
	}
}

func (c Config) BuildTemplate() (string, configtemplate.Config) {
	cfg := configtemplate.NewConfig()

	cfg.AddParameter("loginID", configtemplate.TypeString, true)
	cfg.AddParameter("apiKey", configtemplate.TypeString, true)
	cfg.AddParameter("endpoint", configtemplate.TypeString, false)
	cfg.AddParameter("pollingPeriod", configtemplate.TypeDurationNs, true)

	return Name.String(), cfg
}
