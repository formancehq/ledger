// Code generated by Speakeasy (https://speakeasy.com). DO NOT EDIT.

package components

import (
	"github.com/formancehq/ledger/pkg/client/internal/utils"
	"time"
)

type V2Pipeline struct {
	Ledger     string    `json:"ledger"`
	ExporterID string    `json:"exporterID"`
	ID         string    `json:"id"`
	CreatedAt  time.Time `json:"createdAt"`
	LastLogID  *int64    `json:"lastLogID,omitempty"`
	Enabled    *bool     `json:"enabled,omitempty"`
}

func (v V2Pipeline) MarshalJSON() ([]byte, error) {
	return utils.MarshalJSON(v, "", false)
}

func (v *V2Pipeline) UnmarshalJSON(data []byte) error {
	if err := utils.UnmarshalJSON(data, &v, "", false, false); err != nil {
		return err
	}
	return nil
}

func (o *V2Pipeline) GetLedger() string {
	if o == nil {
		return ""
	}
	return o.Ledger
}

func (o *V2Pipeline) GetExporterID() string {
	if o == nil {
		return ""
	}
	return o.ExporterID
}

func (o *V2Pipeline) GetID() string {
	if o == nil {
		return ""
	}
	return o.ID
}

func (o *V2Pipeline) GetCreatedAt() time.Time {
	if o == nil {
		return time.Time{}
	}
	return o.CreatedAt
}

func (o *V2Pipeline) GetLastLogID() *int64 {
	if o == nil {
		return nil
	}
	return o.LastLogID
}

func (o *V2Pipeline) GetEnabled() *bool {
	if o == nil {
		return nil
	}
	return o.Enabled
}
