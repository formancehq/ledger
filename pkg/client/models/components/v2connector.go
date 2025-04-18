// Code generated by Speakeasy (https://speakeasy.com). DO NOT EDIT.

package components

import (
	"github.com/formancehq/ledger/pkg/client/internal/utils"
	"time"
)

type V2Connector struct {
	Driver    string         `json:"driver"`
	Config    map[string]any `json:"config"`
	ID        string         `json:"id"`
	CreatedAt time.Time      `json:"createdAt"`
}

func (v V2Connector) MarshalJSON() ([]byte, error) {
	return utils.MarshalJSON(v, "", false)
}

func (v *V2Connector) UnmarshalJSON(data []byte) error {
	if err := utils.UnmarshalJSON(data, &v, "", false, false); err != nil {
		return err
	}
	return nil
}

func (o *V2Connector) GetDriver() string {
	if o == nil {
		return ""
	}
	return o.Driver
}

func (o *V2Connector) GetConfig() map[string]any {
	if o == nil {
		return map[string]any{}
	}
	return o.Config
}

func (o *V2Connector) GetID() string {
	if o == nil {
		return ""
	}
	return o.ID
}

func (o *V2Connector) GetCreatedAt() time.Time {
	if o == nil {
		return time.Time{}
	}
	return o.CreatedAt
}
