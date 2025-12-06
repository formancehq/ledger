package fsm

import (
	"encoding/json"

	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

const (
	// CommandTypeCreateBucket is the command type for creating a new bucket
	CommandTypeCreateBucket service.CommandType = "create_bucket"
	// CommandTypeDeleteBucket is the command type for deleting a bucket
	CommandTypeDeleteBucket service.CommandType = "delete_bucket"
)

// CreateBucketCommand represents the data for a create bucket command
type CreateBucketCommand struct {
	Name   string                 `json:"name"`   // Bucket name/ID (required)
	Driver string                 `json:"driver"` // Driver name (required)
	Config map[string]interface{} `json:"config"` // Driver-specific configuration (required)
}

// NewCreateBucketCommand creates a new CreateBucketCommand
func NewCreateBucketCommand(name, driver string, config map[string]interface{}) (*service.Command, error) {
	data, err := json.Marshal(CreateBucketCommand{
		Name:   name,
		Driver: driver,
		Config: config,
	})
	if err != nil {
		return nil, err
	}
	return &service.Command{
		ID:   service.GenerateRandomID(),
		Type: CommandTypeCreateBucket,
		Data: data,
		Date: time.Now(),
	}, nil
}

// DeleteBucketCommand represents the data for a delete bucket command
type DeleteBucketCommand struct {
	Name string `json:"name"` // Bucket name/ID (required)
}

// NewDeleteBucketCommand creates a new DeleteBucketCommand
func NewDeleteBucketCommand(name string) (*service.Command, error) {
	data, err := json.Marshal(DeleteBucketCommand{
		Name: name,
	})
	if err != nil {
		return nil, err
	}
	return &service.Command{
		ID:   service.GenerateRandomID(),
		Type: CommandTypeDeleteBucket,
		Data: data,
		Date: time.Now(),
	}, nil
}
