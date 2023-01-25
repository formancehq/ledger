package stripe

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/formancehq/payments/internal/app/connectors/configtemplate"

	"github.com/formancehq/payments/internal/app/connectors"
)

type Config struct {
	PollingPeriod  connectors.Duration `json:"pollingPeriod" yaml:"pollingPeriod" bson:"pollingPeriod"`
	APIKey         string              `json:"apiKey" yaml:"apiKey" bson:"apiKey"`
	TimelineConfig `bson:",inline"`
}

func (c Config) String() string {
	return fmt.Sprintf("pollingPeriod=%d, pageSize=%d, apiKey=%s", c.PollingPeriod, c.PageSize, c.APIKey)
}

func (c Config) Validate() error {
	if c.APIKey == "" {
		return errors.New("missing api key")
	}

	return nil
}

func (c Config) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

type TimelineConfig struct {
	PageSize uint64 `json:"pageSize" yaml:"pageSize" bson:"pageSize"`
}

func (c Config) BuildTemplate() (string, configtemplate.Config) {
	cfg := configtemplate.NewConfig()

	cfg.AddParameter("apiKey", configtemplate.TypeString, true)
	cfg.AddParameter("pollingPeriod", configtemplate.TypeDurationNs, false)
	cfg.AddParameter("pageSize", configtemplate.TypeDurationUnsignedInteger, false)

	return Name.String(), cfg
}
