package ledger

import (
	"github.com/formancehq/go-libs/v3/time"
	"github.com/uptrace/bun"
)

// Bucket represents a container for ledgers in the system
type Bucket struct {
	bun.BaseModel `bun:"_system.buckets,alias:buckets"`

	ID        int        `json:"id" bun:"id,type:int,scanonly"`
	Name      string     `json:"name" bun:"name,type:varchar(255),pk"`
	AddedAt   time.Time  `json:"addedAt" bun:"added_at,type:timestamp,nullzero"`
	DeletedAt *time.Time `json:"deletedAt,omitempty" bun:"deleted_at,type:timestamp,nullzero"`
}

// IsDeleted returns true if the bucket has been marked as deleted
func (b Bucket) IsDeleted() bool {
	return b.DeletedAt != nil
}

// New creates a new bucket with the given name
func NewBucket(name string) *Bucket {
	return &Bucket{
		Name: name,
	}
}
