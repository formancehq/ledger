package models

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/google/uuid"
)

type Task struct {
	bun.BaseModel `bun:"tasks.task"`

	ID          uuid.UUID `bun:",pk,nullzero"`
	ConnectorID uuid.UUID
	CreatedAt   time.Time `bun:",nullzero"`
	UpdatedAt   time.Time `bun:",nullzero"`
	Name        string
	Descriptor  json.RawMessage
	Status      TaskStatus
	Error       string
	State       json.RawMessage

	Connector *Connector `bun:"rel:belongs-to,join:connector_id=id"`
}

func (t Task) GetDescriptor() TaskDescriptor {
	return TaskDescriptor(t.Descriptor)
}

type TaskDescriptor json.RawMessage

func (td TaskDescriptor) ToMessage() json.RawMessage {
	return json.RawMessage(td)
}

func (td TaskDescriptor) EncodeToString() (string, error) {
	data, err := json.Marshal(td)
	if err != nil {
		return "", fmt.Errorf("failed to encode task descriptor: %w", err)
	}

	return base64.StdEncoding.EncodeToString(data), nil
}

func EncodeTaskDescriptor(descriptor any) (TaskDescriptor, error) {
	res, err := json.Marshal(descriptor)
	if err != nil {
		return nil, fmt.Errorf("failed to encode task descriptor: %w", err)
	}

	return res, nil
}

func DecodeTaskDescriptor[descriptor any](data TaskDescriptor) (descriptor, error) {
	var res descriptor

	err := json.Unmarshal(data, &res)
	if err != nil {
		return res, fmt.Errorf("failed to decode task descriptor: %w", err)
	}

	return res, nil
}

type TaskStatus string

const (
	TaskStatusStopped    TaskStatus = "STOPPED"
	TaskStatusPending    TaskStatus = "PENDING"
	TaskStatusActive     TaskStatus = "ACTIVE"
	TaskStatusTerminated TaskStatus = "TERMINATED"
	TaskStatusFailed     TaskStatus = "FAILED"
)

func (t Task) ParseDescriptor(to interface{}) error {
	err := json.Unmarshal(t.Descriptor, to)
	if err != nil {
		return fmt.Errorf("failed to parse descriptor: %w", err)
	}

	return nil
}
