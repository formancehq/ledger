// Code generated by Speakeasy (https://speakeasy.com). DO NOT EDIT.

package components

import (
	"github.com/formancehq/ledger/pkg/client/internal/utils"
	"math/big"
	"time"
)

type V2Transaction struct {
	InsertedAt                 *time.Time                     `json:"insertedAt,omitempty"`
	UpdatedAt                  *time.Time                     `json:"updatedAt,omitempty"`
	Timestamp                  time.Time                      `json:"timestamp"`
	Postings                   []V2Posting                    `json:"postings"`
	Reference                  *string                        `json:"reference,omitempty"`
	Metadata                   map[string]string              `json:"metadata"`
	ID                         *big.Int                       `json:"id"`
	Reverted                   bool                           `json:"reverted"`
	RevertedAt                 *time.Time                     `json:"revertedAt,omitempty"`
	PreCommitVolumes           map[string]map[string]V2Volume `json:"preCommitVolumes,omitempty"`
	PostCommitVolumes          map[string]map[string]V2Volume `json:"postCommitVolumes,omitempty"`
	PreCommitEffectiveVolumes  map[string]map[string]V2Volume `json:"preCommitEffectiveVolumes,omitempty"`
	PostCommitEffectiveVolumes map[string]map[string]V2Volume `json:"postCommitEffectiveVolumes,omitempty"`
}

func (v V2Transaction) MarshalJSON() ([]byte, error) {
	return utils.MarshalJSON(v, "", false)
}

func (v *V2Transaction) UnmarshalJSON(data []byte) error {
	if err := utils.UnmarshalJSON(data, &v, "", false, false); err != nil {
		return err
	}
	return nil
}

func (o *V2Transaction) GetInsertedAt() *time.Time {
	if o == nil {
		return nil
	}
	return o.InsertedAt
}

func (o *V2Transaction) GetUpdatedAt() *time.Time {
	if o == nil {
		return nil
	}
	return o.UpdatedAt
}

func (o *V2Transaction) GetTimestamp() time.Time {
	if o == nil {
		return time.Time{}
	}
	return o.Timestamp
}

func (o *V2Transaction) GetPostings() []V2Posting {
	if o == nil {
		return []V2Posting{}
	}
	return o.Postings
}

func (o *V2Transaction) GetReference() *string {
	if o == nil {
		return nil
	}
	return o.Reference
}

func (o *V2Transaction) GetMetadata() map[string]string {
	if o == nil {
		return map[string]string{}
	}
	return o.Metadata
}

func (o *V2Transaction) GetID() *big.Int {
	if o == nil {
		return big.NewInt(0)
	}
	return o.ID
}

func (o *V2Transaction) GetReverted() bool {
	if o == nil {
		return false
	}
	return o.Reverted
}

func (o *V2Transaction) GetRevertedAt() *time.Time {
	if o == nil {
		return nil
	}
	return o.RevertedAt
}

func (o *V2Transaction) GetPreCommitVolumes() map[string]map[string]V2Volume {
	if o == nil {
		return nil
	}
	return o.PreCommitVolumes
}

func (o *V2Transaction) GetPostCommitVolumes() map[string]map[string]V2Volume {
	if o == nil {
		return nil
	}
	return o.PostCommitVolumes
}

func (o *V2Transaction) GetPreCommitEffectiveVolumes() map[string]map[string]V2Volume {
	if o == nil {
		return nil
	}
	return o.PreCommitEffectiveVolumes
}

func (o *V2Transaction) GetPostCommitEffectiveVolumes() map[string]map[string]V2Volume {
	if o == nil {
		return nil
	}
	return o.PostCommitEffectiveVolumes
}
